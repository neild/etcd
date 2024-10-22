// Copyright 2017 The etcd Authors
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
	"testing"
	"testing/synctest"
	"time"

	"go.uber.org/zap/zaptest"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
)

func TestRevision(t *testing.T) {
	synctest.Run(func() {
		testRevision(t)
	})
}
func testRevision(t *testing.T) {
	rg := &fakeRevGetter{rev: 1}
	compactable := newFakeCompactible(t)
	tb := newRevision(zaptest.NewLogger(t), 10, rg, compactable)

	tb.Run()
	defer tb.Stop()

	time.Sleep(revInterval)
	// nothing happens
	compactable.Want(nil)

	rg.SetRev(100)
	expectedRevision := int64(90)
	time.Sleep(revInterval)
	compactable.Want(&pb.CompactionRequest{Revision: expectedRevision})

	// skip the same revision
	time.Sleep(revInterval)
	// nothing happens
	compactable.Want(nil)

	rg.SetRev(200)
	expectedRevision = int64(190)
	time.Sleep(revInterval)
	compactable.Want(&pb.CompactionRequest{Revision: expectedRevision})
}

func TestRevisionPause(t *testing.T) {
	synctest.Run(func() {
		testRevisionPause(t)
	})
}
func testRevisionPause(t *testing.T) {
	rg := &fakeRevGetter{rev: 100}
	compactable := newFakeCompactible(t)
	tb := newRevision(zaptest.NewLogger(t), 10, rg, compactable)

	tb.Run()
	defer tb.Stop()
	tb.Pause()

	// tb will collect 3 hours of revisions but not compact since paused
	time.Sleep(3 * time.Hour)
	compactable.Want(nil) // no compaction

	// tb resumes to being blocked on the clock
	tb.Resume()

	// unblock clock, will kick off a compaction at hour 3:05
	time.Sleep(revInterval)
	compactable.Want(&pb.CompactionRequest{Revision: 90})
}
