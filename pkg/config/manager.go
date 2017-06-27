// Copyright 2017 Istio Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package config

import (
	"context"
	"errors"
	"fmt"
	"sync"

	jsonpb "github.com/gogo/protobuf/jsonpb"
	"github.com/golang/glog"
	rpc "github.com/googleapis/googleapis/google/rpc"

	galley "istio.io/api/galley/v1"
	"istio.io/mixer/pkg/adapter"
	"istio.io/mixer/pkg/attribute"
	"istio.io/mixer/pkg/config/descriptor"
	pb "istio.io/mixer/pkg/config/proto"
	"istio.io/mixer/pkg/expr"
)

// Resolver resolves configuration to a list of combined configs.
type Resolver interface {
	// Resolve resolves configuration to a list of combined configs.
	Resolve(bag attribute.Bag, kindSet KindSet, strict bool) ([]*pb.Combined, error)
	// ResolveUnconditional resolves configuration for unconditioned rules.
	// Unconditioned rules are those rules with the empty selector ("").
	ResolveUnconditional(bag attribute.Bag, kindSet KindSet, strict bool) ([]*pb.Combined, error)
}

// ChangeListener listens for config change notifications.
type ChangeListener interface {
	ConfigChange(cfg Resolver, df descriptor.Finder)
}

type validateFunc func(map[string]string) (*Validated, descriptor.Finder, *adapter.ConfigErrors)

// Manager represents the config Manager.
// It is responsible for fetching and receiving configuration changes.
// It applies validated changes to the registered config change listeners.
// api.Handler listens for config changes.
type Manager struct {
	eval                    expr.Evaluator
	validate                validateFunc
	identityAttribute       string
	identityAttributeDomain string

	watcher      galley.WatcherClient
	data         map[string]string
	s            galley.Watcher_WatchClient
	lastRevision int64

	cl []ChangeListener

	sync.RWMutex
	lastError error
}

// NewManager returns a config.Manager.
// Eval validates and evaluates selectors.
// It is also used downstream for attribute mapping.
// AspectFinder finds aspect validator given aspect 'Kind'.
// BuilderFinder finds builder validator given builder 'Impl'.
// LoopDelay determines how often configuration is updated.
// The following fields will be eventually replaced by a
// repository location. At present we use GlobalConfig and ServiceConfig
// as command line input parameters.
// GlobalConfig specifies the location of Global Config.
// ServiceConfig specifies the location of Service config.
func NewManager(eval expr.Evaluator, aspectFinder AspectValidatorFinder, builderFinder BuilderValidatorFinder,
	findAspects AdapterToAspectMapper, watcher galley.WatcherClient, identityAttribute string,
	identityAttributeDomain string) *Manager {
	m := &Manager{
		eval:                    eval,
		watcher:                 watcher,
		data:                    map[string]string{},
		identityAttribute:       identityAttribute,
		identityAttributeDomain: identityAttributeDomain,
		validate: func(cfg map[string]string) (*Validated, descriptor.Finder, *adapter.ConfigErrors) {
			v := newValidator(aspectFinder, builderFinder, findAspects, true, eval)
			rt, ce := v.validate(cfg)
			return rt, v.descriptorFinder, ce
		},
	}

	return m
}

// Register makes the ConfigManager aware of a ConfigChangeListener.
func (c *Manager) Register(cc ChangeListener) {
	c.cl = append(c.cl, cc)
}

// Close stops the config manager go routine.
func (c *Manager) Close() {
	c.s.CloseSend()
}

// LastError returns last error encountered by the manager while processing config.
func (c *Manager) LastError() (err error) {
	c.RLock()
	err = c.lastError
	c.RUnlock()
	return err
}

func (c *Manager) validateAndNotify() {
	fmt.Printf("validating: %+v\n", c.data)
	vd, finder, cerr := c.validate(c.data)
	if cerr != nil {
		c.Lock()
		c.lastError = cerr
		c.Unlock()
		glog.Warningf("Validation failed: %v", cerr)
		return
	}
	rt := newRuntime(vd, c.eval, c.identityAttribute, c.identityAttributeDomain)
	for _, cl := range c.cl {
		cl.ConfigChange(rt, finder)
	}
}

func (c *Manager) toKey(meta *galley.Meta) string {
	if meta.ObjectGroup == "" {
		return fmt.Sprintf("/scopes/%s/%s", meta.Name, meta.ObjectType)
	}
	return fmt.Sprintf("/scopes/%s/subjects/%s/%s", meta.ObjectGroup, meta.Name, meta.ObjectType)
}

func (c *Manager) startWatch(otype string) (int64, []*galley.WatchResponse, error) {
	err := c.s.Send(&galley.WatchRequest{
		RequestUnion: &galley.WatchRequest_CreateRequest{&galley.WatchCreateRequest{
			Subtree: &galley.Meta{
				ApiGroup:          "core",
				ObjectType:        otype,
				ObjectTypeVersion: "v1",
			},
			StartRevision: 0,
		}},
	})
	if err != nil {
		return -1, nil, err
	}
	otherResps := []*galley.WatchResponse{}
	for {
		resp, err := c.s.Recv()
		if err != nil {
			return -1, nil, err
		}
		created := resp.GetCreated()
		if created == nil {
			otherResps = append(otherResps, resp)
			continue
		}
		if resp.Status.Code != int32(rpc.OK) {
			return -1, nil, errors.New(resp.Status.Message)
		}
		for _, o := range created.InitialState {
			key := c.toKey(o.Meta)
			v, err := (&jsonpb.Marshaler{}).MarshalToString(o.SourceData)
			if err != nil {
				glog.Errorf("failed to serialize: %v", err)
				continue
			}
			c.data[key] = v
		}
		if created.CurrentRevision > c.lastRevision {
			c.lastRevision = created.CurrentRevision
		}
		return resp.WatchId, otherResps, nil
	}
}

func (c *Manager) applyChanges(changes []*galley.Event) {
	for _, change := range changes {
		key := c.toKey(change.Kv.Meta)
		if change.Type == galley.DELETE {
			delete(c.data, key)
		} else {
			v, err := (&jsonpb.Marshaler{}).MarshalToString(change.Kv.SourceData)
			if err != nil {
				glog.Errorf("failed to marshal: %v", err)
				continue
			}
			c.data[key] = v
		}
	}
}

func (c *Manager) initialFetchAndVerify() error {
	s, err := c.watcher.Watch(context.Background())
	if err != nil {
		return err
	}
	c.s = s
	_, resps, err := c.startWatch("adapters")
	if err != nil {
		return err
	}
	_, resps2, err := c.startWatch("descriptors")
	if err != nil {
		return err
	}
	resps = append(resps, resps2...)
	_, resps2, err = c.startWatch("rules")
	if err != nil {
		return err
	}
	resps = append(resps, resps2...)
	for _, resp := range resps {
		if evs := resp.GetEvents(); evs != nil {
			c.applyChanges(evs.Events)
		} else if p := resp.GetProgress(); p != nil && p.CurrentRevision > c.lastRevision {
			c.lastRevision = p.CurrentRevision
		}
	}
	c.validateAndNotify()
	return nil
}

func (c *Manager) loop() {
	for {
		resp, err := c.s.Recv()
		if err != nil {
			c.Lock()
			c.lastError = err
			c.Unlock()
			break
		}
		if resp.Status.Code != int32(rpc.OK) {
			glog.Errorf("watch failure: %d %s", resp.Status.Code, resp.Status.Message)
			continue
		}
		if evs := resp.GetEvents(); evs != nil {
			c.applyChanges(evs.Events)
			c.validateAndNotify()
		} else if p := resp.GetProgress(); p != nil && p.CurrentRevision > c.lastRevision {
			c.lastRevision = p.CurrentRevision
		}
		// todo: handle watchcanceled result.
	}
}

// Start watching for configuration changes and handle updates.
func (c *Manager) Start() {
	if err := c.initialFetchAndVerify(); err != nil {
		c.Lock()
		c.lastError = err
		c.Unlock()
		glog.Errorf("can't initiate the watcher: %v", err)
		return
	}
	go c.loop()
}
