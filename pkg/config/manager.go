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
	"fmt"
	"sync"
	"time"

	"github.com/ghodss/yaml"
	"github.com/golang/glog"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"

	"istio.io/mixer/pkg/adapter"
	"istio.io/mixer/pkg/attribute"
	"istio.io/mixer/pkg/config/descriptor"
	pb "istio.io/mixer/pkg/config/proto"
	"istio.io/mixer/pkg/expr"
	"istio.io/mixer/pkg/template"
)

// TODO: should be customized
const ns = "default"

type validateFunc func(cfg map[string]string) (rt *Validated, desc descriptor.Finder, ce *adapter.ConfigErrors)

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
	ns            string
	discovery     *discovery.DiscoveryClient
	dynclient     *dynamic.Client
	validate      validateFunc

	cached   map[string]map[string]map[string]interface{}
	watchers map[string]watch.Interface
	chupdate chan interface{}

	// attribute around which scopes and subjects are organized.
	identityAttribute       string
	identityAttributeDomain string

	ticker         *time.Ticker
	lastFetchIndex int

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
	getBuilderInfoFns []adapter.GetBuilderInfoFn, findAspects AdapterToAspectMapper, repository template.Repository,
	config *rest.Config, loopDelay time.Duration, identityAttribute string,
	identityAttributeDomain string) *Manager {
	dclient := discovery.NewDiscoveryClientForConfigOrDie(config)
	config.APIPath = "/apis"
	config.GroupVersion = &schema.GroupVersion{Group: "config.istio.io", Version: "v1alpha2"}
	dynclient, err := dynamic.NewClient(config)
	if err != nil {
		glog.Infof("failed to create a dynamic client: %v", err)
		return nil
	}
	m := &Manager{
		eval:                    eval,
		aspectFinder:            aspectFinder,
		builderFinder:           builderFinder,
		findAspects:             findAspects,
		loopDelay:               loopDelay,
		discovery:               dclient,
		dynclient:               dynclient,
		ns:                      ns,
		identityAttribute:       identityAttribute,
		identityAttributeDomain: identityAttributeDomain,
		validate: func(cfg map[string]string) (*Validated, descriptor.Finder, *adapter.ConfigErrors) {
			r := newRegistry2(getBuilderInfoFns, repository.SupportsTemplate)
			v := newValidator(aspectFinder, builderFinder, r.FindBuilderInfo, SetupHandlers, repository, findAspects, true, eval)
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
	for _, w := range c.watchers {
		w.Stop()
	}
	// TODO: there's race condition of chupdate here. Fix it.
	close(c.chupdate)
}

func (c *Manager) watch(w watch.Interface, data map[string]map[string]interface{}) {
	defer w.Stop()
	for ev := range w.ResultChan() {
		if ev.Type == watch.Error {
			glog.Errorf("watch error: %+v", ev.Object)
			continue
		}
		uns := ev.Object.(*unstructured.Unstructured)
		name := uns.Object["metadata"].(map[string]interface{})["name"].(string)
		spec := uns.Object["spec"].(map[string]interface{})
		if ev.Type == watch.Deleted {
			delete(data, name)
		} else {
			data[name] = spec
		}
		c.chupdate <- true
	}
}

func (c *Manager) waitAndConsume(d time.Duration) {
	after := time.After(d)
	for {
		select {
		case <-c.chupdate:
			// pass, consume chupdate.
		case <-after:
			return
		}
	}
}

func (c *Manager) buildListConfig(m map[string]map[string]interface{}, key, targetKey string, target map[string]string) error {
	dataList := make([]interface{}, 0, len(m))
	for _, cfg := range m {
		dataList = append(dataList, cfg)
	}
	listBytes, err := yaml.Marshal(map[string]interface{}{key: dataList})
	if err != nil {
		return err
	}
	target[targetKey] = string(listBytes)
	return nil
}

func (c *Manager) buildMapConfig(m map[string]map[string]interface{}, typeName string, target map[string]string) error {
	for name, cfg := range m {
		targetKey := fmt.Sprintf("/scopes/global/subjects/%s/%s", name, typeName)
		cfgBytes, err := yaml.Marshal(cfg)
		if err != nil {
			return err
		}
		target[targetKey] = string(cfgBytes)
	}
	return nil
}

func (c *Manager) validateAndBuildRuntime() {
	// Currently this is very inefficient. We need to refactor the validator.
	data := map[string]string{}

	if err := c.buildListConfig(c.cached["Adapter"], "adapters", keyAdapters, data); err != nil {
		glog.Errorf("Failed to build config data: %v", err)
		return
	}
	if err := c.buildListConfig(c.cached["Handler"], "handlers", keyHandlers, data); err != nil {
		glog.Errorf("Failed to build config data: %v", err)
		return
	}
	if globalDescriptors, ok := c.cached["Descriptor"]["global"]; ok {
		descBytes, err := yaml.Marshal(globalDescriptors)
		if err != nil {
			glog.Errorf("Failed to build config data: %v", err)
			return
		}
		data[keyDescriptors] = string(descBytes)
	}
	for key, name := range map[string]string{
		"Constructor": "constructors",
		"ActionRule":  "action_rules",
		"Rule":        "rules",
	} {
		if err := c.buildMapConfig(c.cached[key], name, data); err != nil {
			glog.Errorf("Failed to build config data: %v", err)
			return
		}
	}

	vd, finder, cerr := c.validate(data)
	if cerr != nil {
		glog.Warningf("Validation failed: %v", cerr)
		return
	}
	rt := newRuntime(vd, c.eval, c.identityAttribute, c.identityAttributeDomain)
	glog.Infof("%+v", rt)
	for _, cl := range c.cl {
		cl.ConfigChange(rt, finder)
	}
}

// Start watching for configuration changes and handle updates.
func (c *Manager) Start() {
	c.chupdate = make(chan interface{})
	c.cached = map[string]map[string]map[string]interface{}{
		"Adapter":     {},
		"Descriptor":  {},
		"Handler":     {},
		"Constructor": {},
		"ActionRule":  {},
		"Rule":        {},
	}
	c.watchers = map[string]watch.Interface{}
	list, err := c.discovery.ServerResourcesForGroupVersion("config.istio.io/v1alpha2")
	if err != nil {
		glog.Errorf("can't fetch the resources: %+v", err)
		return
	}
	for _, res := range list.APIResources {
		if data, ok := c.cached[res.Kind]; ok {
			w, err := c.dynclient.Resource(&res, c.ns).Watch(metav1.ListOptions{})
			if err == nil {
				go c.watch(w, data)
				c.watchers[res.Kind] = w
			} else {
				glog.Errorf("failed to start watching %s: %v", res.Kind, err)
			}
		}
	}
	// watch emits the initial data as 'ADDED' events. Wait a bit to get the initial status.
	c.waitAndConsume(time.Second)
	c.validateAndBuildRuntime()
	go func() {
		for range c.chupdate {
			c.validateAndBuildRuntime()
		}
	}()
}
