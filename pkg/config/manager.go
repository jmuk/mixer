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
	"crypto/sha1"
	"errors"
	"reflect"
	"sync"
	"time"

	"github.com/golang/glog"

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

// Manager represents the config Manager.
// It is responsible for fetching and receiving configuration changes.
// It applies validated changes to the registered config change listeners.
// api.Handler listens for config changes.
type Manager struct {
	eval          expr.Evaluator
	aspectFinder  AspectValidatorFinder
	builderFinder BuilderValidatorFinder
	findAspects   AdapterToAspectMapper
	loopDelay     time.Duration
	store         KeyValueStore
	validate      validateFunc

	// attribute around which scopes and subjects are organized.
	identityAttribute       string
	identityAttributeDomain string

	ticker         *time.Ticker
	lastFetchIndex int
	notifiedIndex  int

	cl []ChangeListener

	lastValidated *Validated
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
	findAspects AdapterToAspectMapper, store KeyValueStore, loopDelay time.Duration, identityAttribute string,
	identityAttributeDomain string) *Manager {
	m := &Manager{
		eval:                    eval,
		aspectFinder:            aspectFinder,
		builderFinder:           builderFinder,
		findAspects:             findAspects,
		loopDelay:               loopDelay,
		store:                   store,
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

// NotifyStoreChanged is called by "store" when new changes are available
func (c *Manager) NotifyStoreChanged(index int) {
	if c.notifiedIndex == indexNotSupported {
		c.notifiedIndex = index
	}
}

// Register makes the ConfigManager aware of a ConfigChangeListener.
func (c *Manager) Register(cc ChangeListener) {
	c.cl = append(c.cl, cc)
}

func readdb(store KeyValueStore, prefix string) (map[string]string, map[string][sha1.Size]byte, int, error) {
	keys, index, err := store.List(prefix, true)
	if err != nil {
		return nil, nil, index, err
	}

	// read
	shas := map[string][sha1.Size]byte{}
	data := map[string]string{}

	var found bool
	var val string

	for _, k := range keys {
		val, index, found = store.Get(k)
		if !found {
			continue
		}
		data[k] = val
		shas[k] = sha1.Sum([]byte(val))
	}

	return data, shas, index, nil
}

//  /scopes/global/subjects/global/rules
//  /scopes/global/adapters
//  /scopes/global/descriptors

// fetch config and return runtime if a new one is available.
func (c *Manager) fetch() (*runtime, descriptor.Finder, error) {
	// TODO: use ChangeLogReader with c.notifiedIndex
	data, shas, index, err := readdb(c.store, "/")
	if glog.V(9) {
		glog.Info(data)
	}
	if err != nil {
		return nil, nil, errors.New("Unable to read database: " + err.Error())
	}
	// check if sha has changed.
	if c.lastValidated != nil && reflect.DeepEqual(shas, c.lastValidated.shas) {
		// nothing actually changed.
		return nil, nil, nil
	}

	var vd *Validated
	var finder descriptor.Finder
	var cerr *adapter.ConfigErrors

	vd, finder, cerr = c.validate(data)
	if cerr != nil {
		glog.Warningf("Validation failed: %v", cerr)
		return nil, nil, cerr
	}
	c.lastFetchIndex = index
	vd.shas = shas
	c.lastValidated = vd
	return newRuntime(vd, c.eval, c.identityAttribute, c.identityAttributeDomain), finder, nil
}

// fetchAndNotify fetches a new config and notifies listeners if something has changed
func (c *Manager) fetchAndNotify() {
	rt, df, err := c.fetch()
	if err != nil {
		c.Lock()
		c.lastError = err
		c.Unlock()
		glog.Warningf("Error loading new config %v", err)
	}
	if rt == nil {
		return
	}

	glog.Infof("Loaded new config %s", c.store)
	for _, cl := range c.cl {
		cl.ConfigChange(rt, df)
	}
}

// LastError returns last error encountered by the manager while processing config.
func (c *Manager) LastError() (err error) {
	c.RLock()
	err = c.lastError
	c.RUnlock()
	return err
}

// Close stops the config manager go routine.
func (c *Manager) Close() {
	if c.ticker != nil {
		c.ticker.Stop()
	}
}

func (c *Manager) loop() {
	for range c.ticker.C {
		if c.notifiedIndex == indexNotSupported || c.notifiedIndex > c.lastFetchIndex {
			c.fetchAndNotify()
		}
	}
}

// Start watching for configuration changes and handle updates.
func (c *Manager) Start() {
	c.fetchAndNotify()
	c.notifiedIndex = c.lastFetchIndex
	if cn, ok := c.store.(ChangeNotifier); ok {
		cn.RegisterStoreChangeListener(c)
	}

	// if store does not support notification use the loop
	// If it is not successful, we will continue to watch for changes.
	c.ticker = time.NewTicker(c.loopDelay)
	go c.loop()
}
