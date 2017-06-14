// Copyright 2016 Istio Authors
//
// Licensed under the Apache License, Revision 2.0 (the "License");
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
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gogo/protobuf/jsonpb"
	pb "github.com/gogo/protobuf/types"
	"github.com/golang/glog"
	rpc "github.com/googleapis/googleapis/google/rpc"
	"golang.org/x/net/context"

	configpb "istio.io/api/config/v1"
	"istio.io/mixer/pkg/config/store"
)

type watcher struct {
	stream               configpb.Service_WatchServer
	keyPrefix            string
	sendPut              bool
	sendDelete           bool
	lastNotifiedRevision int64
}

type configAPIServer struct {
	// TODO: allow multiple kvs.
	kvs store.KeyValueStore
	cr  store.ChangeLogReader

	lastNotifiedIndex int
	lastFetchedIndex  int

	nextWatcherID int64
	watchers      map[int64]*watcher
	mu            sync.Mutex
}

var _ configpb.ServiceServer = &configAPIServer{}

// NewConfigAPIServer creates a new configpb.ServiceServer instance whose
// storage backend is specified as the url.
func NewConfigAPIServer(url string, interval time.Duration) (configpb.ServiceServer, error) {
	kvs, err := store.NewRegistry(StoreInventory()...).NewStore(url)
	if err != nil {
		return nil, err
	}
	s := &configAPIServer{kvs: kvs, watchers: map[int64]*watcher{}}
	if cn, ok := kvs.(store.ChangeNotifier); ok {
		cn.RegisterListener(s)
	} else {
		kvs.Close()
		return nil, fmt.Errorf("config store %s is not a change notifier", url)
	}
	if cr, ok := kvs.(store.ChangeLogReader); ok {
		s.cr = cr
	} else {
		kvs.Close()
		return nil, fmt.Errorf("config store %s is not changelog readable", url)
	}
	go func() {
		ticker := time.NewTicker(interval)
		for range ticker.C {
			s.check()
		}
	}()
	return s, nil
}

func (s *configAPIServer) buildPath(meta *configpb.Meta) string {
	var paths = []string{meta.ApiGroup, meta.ApiGroupVersion, meta.ObjectType, meta.ObjectGroup}
	if len(meta.Name) != 0 {
		paths = append(paths, meta.Name)
	}
	return "/" + strings.Join(paths, "/")
}

func (s *configAPIServer) pathToMeta(path string) (*configpb.Meta, error) {
	if path[0] != '/' {
		return nil, fmt.Errorf("illformed path %s", path)
	}
	paths := strings.Split(path[1:], "/")
	if len(paths) < 5 {
		return nil, fmt.Errorf("insufficient path components: %s", path)
	}
	return &configpb.Meta{
		ApiGroup:        paths[0],
		ApiGroupVersion: paths[1],
		ObjectType:      paths[2],
		ObjectGroup:     paths[3],
		Name:            strings.Join(paths[4:], "/"),
	}, nil
}

func (s *configAPIServer) buildObject(data string, meta *configpb.Meta, selector *configpb.ObjectFieldInclude) (obj *configpb.Object, err error) {
	src := &pb.Struct{Fields: map[string]*pb.Value{}}
	err = jsonpb.UnmarshalString(data, src)
	if err != nil {
		return nil, err
	}
	obj = &configpb.Object{Meta: meta}
	if selector != nil && selector.SourceData {
		obj.SourceData = src
	}
	if selector != nil && selector.Data {
		glog.Infof("data is requested, but not supported yet")
	}
	return obj, nil
}

func (s *configAPIServer) GetObject(ctx context.Context, req *configpb.GetObjectRequest) (resp *configpb.Object, err error) {
	value, index, found := s.kvs.Get(s.buildPath(req.Meta))
	if !found {
		return nil, fmt.Errorf("object not found")
	}
	resp, err = s.buildObject(value, req.Meta, req.Incl)
	if err != nil {
		return nil, err
	}
	resp.Meta.Revision = int64(index)
	return resp, nil
}

func (s *configAPIServer) ListObjects(ctx context.Context, req *configpb.ListObjectsRequest) (resp *configpb.ObjectList, err error) {
	keys, index, err := s.kvs.List(s.buildPath(req.Meta), true)
	if err != nil {
		return nil, err
	}
	resp = &configpb.ObjectList{Meta: req.Meta}
	resp.Meta.Revision = int64(index)
	for _, k := range keys {
		m, err := s.pathToMeta(k)
		if err != nil {
			glog.Warningf("error on key: %v", err)
			continue
		}
		var obj *configpb.Object
		if req.Incl != nil && (req.Incl.SourceData || req.Incl.Data) {
			value, gindex, found := s.kvs.Get(s.buildPath(m))
			if !found {
				glog.Warningf("not found: %+v", m)
				continue
			}
			obj, err = s.buildObject(value, m, req.Incl)
			if err != nil {
				glog.Warningf("error on fetching the content for %s: %v", k, err)
				continue
			}
			obj.Meta.Revision = int64(gindex)
		} else {
			obj = &configpb.Object{Meta: m}
			obj.Meta.Revision = int64(index)
		}
		resp.Objects = append(resp.Objects, obj)
	}
	return resp, nil
}

func (s *configAPIServer) ListObjectTypes(ctx context.Context, req *configpb.ListObjectTypesRequest) (resp *configpb.ObjectTypeList, err error) {
	var prefix string
	if req.Meta == nil || (req.Meta.ApiGroup == "" && req.Meta.ApiGroupVersion == "") {
		prefix = "/"
	} else {
		prefix = fmt.Sprintf("/%s/%s/", req.Meta.ApiGroup, req.Meta.ApiGroupVersion)
	}
	keys, _, err := s.kvs.List(prefix, true)
	resp = &configpb.ObjectTypeList{
		Meta:        req.Meta,
		ObjectTypes: make([]*configpb.Meta, 0, len(keys)),
	}
	known := map[string]bool{}
	for _, k := range keys {
		m, err := s.pathToMeta(k)
		if err != nil {
			glog.Infof("can't parse key %s: %v", k, err)
			continue
		}
		m.Name = ""
		mKey := s.buildPath(m)
		if _, ok := known[mKey]; ok {
			continue
		}
		known[mKey] = true
		resp.ObjectTypes = append(resp.ObjectTypes, m)
	}
	return resp, nil
}

func (s *configAPIServer) CreateObject(ctx context.Context, req *configpb.CreateObjectRequest) (resp *configpb.Object, err error) {
	value, err := (&jsonpb.Marshaler{}).MarshalToString(req.SourceData)
	if err != nil {
		return nil, err
	}
	index, err := s.kvs.Set(s.buildPath(req.Meta), string(value))
	if err != nil {
		return nil, err
	}
	resp = &configpb.Object{Meta: req.Meta, SourceData: req.SourceData}
	resp.Meta.Revision = int64(index)
	return resp, nil
}

func (s *configAPIServer) UpdateObject(ctx context.Context, req *configpb.UpdateObjectRequest) (resp *configpb.Object, err error) {
	return s.CreateObject(ctx, &configpb.CreateObjectRequest{Meta: req.Meta, SourceData: req.SourceData})
}

func (s *configAPIServer) DeleteObject(ctx context.Context, req *configpb.DeleteObjectRequest) (resp *pb.Empty, err error) {
	err = s.kvs.Delete(s.buildPath(req.Meta))
	return &pb.Empty{}, err
}

func (s *configAPIServer) check() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lastFetchedIndex < s.lastNotifiedIndex {
		changes, err := s.cr.Read(s.lastFetchedIndex)
		if err != nil {
			glog.Warningf("failed to read changes: %v", err)
			return
		}
		for _, c := range changes {
			if c.Index < s.lastFetchedIndex {
				s.lastFetchedIndex = c.Index
			}
		}
		toSend := s.filterEvents(s.watchers, changes)
		for id, evs := range toSend {
			s.watchers[id].stream.Send(&configpb.WatchResponse{
				WatchId:      id,
				ResponseType: configpb.DATA,
				Status: &rpc.Status{
					Code: int32(rpc.OK),
				},
				Events: evs,
			})
		}
	}
}

func (s *configAPIServer) NotifyStoreChanged(index int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastNotifiedIndex = index
}

func (s *configAPIServer) filterEvents(watchers map[int64]*watcher, changes []store.Change) map[int64][]*configpb.Event {
	toSend := map[int64][]*configpb.Event{}

	sort.Slice(changes, func(i, j int) bool {
		return changes[i].Index > changes[j].Index
	})
	visited := map[string]bool{}
	filtered := make([]store.Change, 0, len(changes))
	for _, c := range changes {
		if visited[c.Key] {
			continue
		}
		visited[c.Key] = true
		filtered = append(filtered, c)
	}

	for _, c := range filtered {
		meta, err := s.pathToMeta(c.Key)
		if err != nil {
			glog.Warningf("%v", err)
			continue
		}
		ev := &configpb.Event{
			Kv: &configpb.Object{Meta: meta},
		}
		dataFetched := false
		if c.Type == store.Update {
			ev.Type = configpb.UPDATE
		} else {
			ev.Type = configpb.DELETE
		}
		for id, w := range watchers {
			if strings.HasPrefix(c.Key, w.keyPrefix) && w.lastNotifiedRevision < int64(c.Index) {
				if c.Type == store.Update && !dataFetched {
					dataFetched = true
					data, _, found := s.kvs.Get(c.Key)
					if found {
						glog.Warningf("failed to fetch data for key %s", c.Key)
					}
					ev.Kv, err = s.buildObject(data, ev.Kv.Meta, &configpb.ObjectFieldInclude{Data: true, SourceData: true})
					if err != nil {
						glog.Warningf("failed to builds an object for key %s: %v", c.Key, err)
					}
				}
				toSend[id] = append(toSend[id], ev)
				w.lastNotifiedRevision = int64(c.Index)
			}
		}
	}
	return toSend
}

func (s *configAPIServer) startWatch(req *configpb.WatchCreateRequest, stream configpb.Service_WatchServer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := s.nextWatcherID
	s.nextWatcherID++
	w := &watcher{
		keyPrefix:            s.buildPath(req.Key),
		stream:               stream,
		lastNotifiedRevision: req.StartRevision - 1,
	}
	resp := &configpb.WatchResponse{
		WatchId:      id,
		ResponseType: configpb.WATCH_CREATED,
	}
	changes, err := s.cr.Read(int(req.StartRevision))
	if err == nil {
		resp.Status = &rpc.Status{Code: int32(rpc.OK)}
		events := s.filterEvents(map[int64]*watcher{id: w}, changes)
		resp.Events = events[id]
		s.watchers[id] = w
	} else {
		resp.Status = &rpc.Status{
			Code:    int32(rpc.INTERNAL),
			Message: err.Error(),
		}
	}
	stream.Send(resp)
}

func (s *configAPIServer) cancelWatch(req *configpb.WatchCancelRequest, stream configpb.Service_WatchServer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	resp := &configpb.WatchResponse{
		WatchId:      req.WatchId,
		ResponseType: configpb.WATCH_CANCELED,
	}
	_, found := s.watchers[req.WatchId]
	if found {
		delete(s.watchers, req.WatchId)
		resp.Status = &rpc.Status{
			Code: int32(rpc.OK),
		}
	} else {
		resp.Status = &rpc.Status{
			Code:    int32(rpc.NOT_FOUND),
			Message: fmt.Sprintf("watcher id %d not found", req.WatchId),
		}
	}
	stream.Send(resp)
}

func (s *configAPIServer) Watch(stream configpb.Service_WatchServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			break
		}
		if createReq := req.GetCreateRequest(); createReq != nil {
			s.startWatch(createReq, stream)
		} else if cancelReq := req.GetCancelRequest(); cancelReq != nil {
			s.cancelWatch(cancelReq, stream)
		}
	}
	return nil
}
