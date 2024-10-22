// Copyright 2015 The etcd Authors
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

package v3compactor

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/golang/protobuf/proto"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
)

type fakeCompactable struct {
	t   *testing.T
	got *pb.CompactionRequest
}

func newFakeCompactible(t *testing.T) *fakeCompactable {
	fc := &fakeCompactable{t: t}
	t.Cleanup(func() {
		if fc.got != nil {
			t.Errorf("test ended with unexamined compaction request")
		}
	})
	return fc
}

func (fc *fakeCompactable) Compact(ctx context.Context, r *pb.CompactionRequest) (*pb.CompactionResponse, error) {
	if fc.got != nil {
		fc.t.Errorf("compaction happening with prior unexamined compaction request")
	}
	fc.got = r
	return &pb.CompactionResponse{}, nil
}

func (fc *fakeCompactable) Want(want *pb.CompactionRequest) {
	fc.t.Helper()
	synctest.Wait()
	if want == nil {
		if fc.got != nil {
			fc.t.Fatalf("got compaction request, want none")
		}
	} else {
		if fc.got == nil {
			fc.t.Fatalf("got no compaction request, want one")
		}
		if !proto.Equal(fc.got, want) {
			fc.t.Fatalf("compact request = %v, want %v", fc.got, want)
		}
		fc.got = nil
	}
}

type fakeRevGetter struct {
	mu  sync.Mutex
	rev int64
}

func (fr *fakeRevGetter) Rev() int64 {
	fr.mu.Lock()
	defer fr.mu.Unlock()
	return fr.rev
}

func (fr *fakeRevGetter) IncRev() {
	synctest.Wait()
	fr.mu.Lock()
	defer fr.mu.Unlock()
	fr.rev++
}

func (fr *fakeRevGetter) SetRev(rev int64) {
	synctest.Wait()
	fr.mu.Lock()
	defer fr.mu.Unlock()
	fr.rev = rev
}
