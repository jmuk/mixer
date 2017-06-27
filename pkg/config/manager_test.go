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
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ghodss/yaml"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/types"
	rpc "github.com/googleapis/googleapis/google/rpc"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	galley "istio.io/api/galley/v1"
	"istio.io/mixer/pkg/adapter"
	"istio.io/mixer/pkg/config/descriptor"
)

const (
	keyTargetService = "target.service"
	keyServiceDomain = "svc.cluster.local"
)

type mtest struct {
	gcContent string
	gc        string
	scContent string
	sc        string
	ada       map[string]adapter.ConfigValidator
	asp       map[Kind]AspectValidator
	errStr    string
}

type fakelistener struct {
	called int
	rt     Resolver
	df     descriptor.Finder
	sync.Mutex
}

func (f *fakelistener) ConfigChange(cfg Resolver, df descriptor.Finder) {
	f.Lock()
	f.rt = cfg
	f.df = df
	f.called++
	f.Unlock()
}
func (f *fakelistener) Called() int {
	f.Lock()
	called := f.called
	f.Unlock()
	return called
}

func TestConfigManager(t *testing.T) {
	evaluator := newFakeExpr()
	mlist := []mtest{
		{"", "", "", "", nil, nil, ""},
		{ConstGlobalConfig, "globalconfig", "", "", nil, nil, "failed validation"},
		{ConstGlobalConfig, "globalconfig", sSvcConfig, "serviceconfig", nil, nil, "failed validation"},
		{ConstGlobalConfigValid, "globalconfig", sSvcConfig2, "serviceconfig", map[string]adapter.ConfigValidator{
			"denyChecker": &lc{},
			"metrics":     &lc{},
			"listchecker": &lc{},
		}, map[Kind]AspectValidator{
			DenialsKind: &ac{},
			MetricsKind: &ac{},
			ListsKind:   &ac{},
		}, ""},
	}
	for idx, mt := range mlist {
		t.Run(strconv.Itoa(idx), func(t *testing.T) {
			loopDelay := time.Millisecond * 50
			vf := newVfinder(mt.ada, mt.asp)
			watcher := newFakeWatcher(mt.gcContent, mt.scContent)
			ma := NewManager(evaluator, vf.FindAspectValidator, vf.FindAdapterValidator, vf.AdapterToAspectMapperFunc,
				watcher, keyTargetService, keyServiceDomain)
			testConfigManager(t, ma, mt, loopDelay)
		})
	}
}

func testConfigManager(t *testing.T, mgr *Manager, mt mtest, loopDelay time.Duration) {
	fl := &fakelistener{}
	mgr.Register(fl)

	mgr.Start()
	defer mgr.Close()

	le := mgr.LastError()

	if mt.errStr != "" && le == nil {
		t.Fatalf("Expected an error %s Got nothing", mt.errStr)
	}

	if mt.errStr == "" && le != nil {
		t.Fatalf("Unexpected error %s", le)
	}

	if mt.errStr == "" && fl.rt == nil {
		t.Error("Config listener was not notified")
	}

	if mt.errStr == "" && le == nil {
		called := fl.Called()
		if le == nil && called != 1 {
			t.Errorf("called Got: %d, want: 1", called)
		}
		// give mgr time to go thru the start Loop() go routine
		// fetchAndNotify should be indirectly called multiple times.
		time.Sleep(loopDelay * 2)
		// check again. should not change, no new data is available
		called = fl.Called()
		if le == nil && called != 1 {
			t.Errorf("called Got: %d, want: 1", called)
		}
		return
	}

	if !strings.Contains(le.Error(), mt.errStr) {
		t.Fatalf("Unexpected error. Expected %s\nGot: %s\n", mt.errStr, le)
	}
}

// fakeWatcher

type fakeWatcher struct {
	data map[string][]*galley.ConfigObject
}

func (w *fakeWatcher) Watch(ctx context.Context, opts ...grpc.CallOption) (galley.Watcher_WatchClient, error) {
	return &fakeWatchStream{ctx: ctx, data: w.data}, nil
}

func newFakeWatcher(gc string, sc string) *fakeWatcher {
	gcjson, _ := yaml.YAMLToJSON([]byte(gc))
	gcpb := &types.Struct{}
	jsonpb.UnmarshalString(string(gcjson), gcpb)

	scjson, _ := yaml.YAMLToJSON([]byte(sc))
	scpb := &types.Struct{}
	jsonpb.UnmarshalString(string(scjson), scpb)
	return &fakeWatcher{
		map[string][]*galley.ConfigObject{
			"adapters": {
				{
					Meta:       &galley.Meta{ApiGroup: "core", ObjectType: "adapters", ObjectTypeVersion: "v1", Name: "global"},
					SourceData: gcpb,
				},
			},
			"descriptors": {
				{
					Meta:       &galley.Meta{ApiGroup: "core", ObjectType: "descriptors", ObjectTypeVersion: "v1", Name: "global"},
					SourceData: &types.Struct{},
				},
			},
			"rules": {
				{
					Meta:       &galley.Meta{ApiGroup: "core", ObjectType: "rules", ObjectTypeVersion: "v1", ObjectGroup: "global", Name: "global"},
					SourceData: scpb,
				},
			},
		},
	}
}

type fakeWatchStream struct {
	resps []*galley.WatchResponse
	data  map[string][]*galley.ConfigObject
	ctx   context.Context
}

func (s *fakeWatchStream) Send(req *galley.WatchRequest) error {
	if create := req.GetCreateRequest(); create != nil {
		s.resps = append(s.resps, &galley.WatchResponse{
			Status: &rpc.Status{Code: int32(rpc.OK)},
			ResponseUnion: &galley.WatchResponse_Created{
				&galley.WatchCreated{
					InitialState:    s.data[create.Subtree.ObjectType],
					CurrentRevision: 0,
				},
			},
		})
		return nil
	}
	// watch cancel is not supported yet.
	return fmt.Errorf("should not reach: %+v", req)
}

func (s *fakeWatchStream) Recv() (*galley.WatchResponse, error) {
	if len(s.resps) == 0 {
		return nil, fmt.Errorf("ended")
	}
	resp := s.resps[0]
	s.resps = s.resps[1:]
	return resp, nil
}

func (s *fakeWatchStream) Header() (metadata.MD, error) {
	return nil, fmt.Errorf("not supported")
}

func (s *fakeWatchStream) Trailer() metadata.MD {
	return nil
}

func (s *fakeWatchStream) CloseSend() error {
	return nil
}

func (s *fakeWatchStream) Context() context.Context {
	return s.ctx
}

func (s *fakeWatchStream) SendMsg(m interface{}) error {
	return fmt.Errorf("not supported")
}

func (s *fakeWatchStream) RecvMsg(m interface{}) error {
	return fmt.Errorf("not supported")
}
