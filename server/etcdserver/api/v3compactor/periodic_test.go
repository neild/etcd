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
	"testing"
	"testing/synctest"
	"time"

	"go.uber.org/zap/zaptest"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
)

func TestPeriodicHourly(t *testing.T) {
	synctest.Run(func() {
		testPeriodicHourly(t)
	})
}
func testPeriodicHourly(t *testing.T) {
	retentionHours := 2
	retentionDuration := time.Duration(retentionHours) * time.Hour

	// TODO: Do not depand or real time (Recorder.Wait) in unit tests.
	rg := &fakeRevGetter{rev: 1}
	compactable := newFakeCompactible(t)
	tb := newPeriodic(zaptest.NewLogger(t), retentionDuration, rg, compactable)

	tb.Run()
	defer tb.Stop()

	initialIntervals, intervalsPerPeriod := tb.getRetentions(), 10

	// compaction doesn't happen til 2 hours elapse
	for i := 0; i < initialIntervals-1; i++ {
		compactable.Want(nil) // no compaction
		rg.IncRev()
		time.Sleep(tb.getRetryInterval())
	}

	// very first compaction
	expectedRevision := int64(1)
	compactable.Want(&pb.CompactionRequest{Revision: expectedRevision})

	// simulate 3 hours
	// now compactor kicks in, every hour
	for i := 0; i < 3; i++ {
		// advance one hour, one revision for each interval
		for j := 0; j < intervalsPerPeriod; j++ {
			rg.IncRev()
			time.Sleep(tb.getRetryInterval())
		}

		expectedRevision = int64((i + 1) * 10)
		compactable.Want(&pb.CompactionRequest{Revision: expectedRevision})
	}
}

func TestPeriodicMinutes(t *testing.T) {
	synctest.Run(func() {
		testPeriodicMinutes(t)
	})
}
func testPeriodicMinutes(t *testing.T) {
	retentionMinutes := 5
	retentionDuration := time.Duration(retentionMinutes) * time.Minute

	rg := &fakeRevGetter{rev: 1}
	compactable := newFakeCompactible(t)
	tb := newPeriodic(zaptest.NewLogger(t), retentionDuration, rg, compactable)

	tb.Run()
	defer tb.Stop()

	initialIntervals, intervalsPerPeriod := tb.getRetentions(), 10

	// compaction doesn't happen til 5 minutes elapse
	for i := 0; i < initialIntervals-1; i++ {
		compactable.Want(nil) // no compaction
		rg.IncRev()
		time.Sleep(tb.getRetryInterval())
	}

	// very first compaction
	expectedRevision := int64(1)
	compactable.Want(&pb.CompactionRequest{Revision: expectedRevision})

	// compaction happens at every interval
	for i := 0; i < 5; i++ {
		// advance 5-minute, one revision for each interval
		for j := 0; j < intervalsPerPeriod; j++ {
			compactable.Want(nil) // no compaction
			rg.IncRev()
			time.Sleep(tb.getRetryInterval())
		}

		expectedRevision = int64((i + 1) * 10)
		compactable.Want(&pb.CompactionRequest{Revision: expectedRevision})
	}
}

func TestPeriodicPause(t *testing.T) {
	synctest.Run(func() {
		testPeriodicPause(t)
	})
}
func testPeriodicPause(t *testing.T) {
	retentionDuration := time.Hour
	rg := &fakeRevGetter{rev: 1}
	compactable := newFakeCompactible(t)
	tb := newPeriodic(zaptest.NewLogger(t), retentionDuration, rg, compactable)
	defer tb.Stop()

	tb.Run()
	tb.Pause()

	n := tb.getRetentions()

	// tb will collect 3 hours of revisions but not compact since paused
	for i := 0; i < n*3; i++ {
		rg.IncRev()
		time.Sleep(tb.getRetryInterval())
	}

	compactable.Want(nil) // no compaction

	// tb resumes to being blocked on the clock
	// will kick off a compaction at T=3h6m by retry
	tb.Resume()
	time.Sleep(tb.getRetryInterval())
	compactable.Want(&pb.CompactionRequest{Revision: int64(1 + 2*n + 1)})
}

func TestPeriodicSkipRevNotChange(t *testing.T) {
	synctest.Run(func() {
		testPeriodicSkipRevNotChange(t)
	})
}
func testPeriodicSkipRevNotChange(t *testing.T) {
	retentionMinutes := 5
	retentionDuration := time.Duration(retentionMinutes) * time.Minute

	rg := &fakeRevGetter{rev: 101}
	compactable := newFakeCompactible(t)
	tb := newPeriodic(zaptest.NewLogger(t), retentionDuration, rg, compactable)

	tb.Run()
	defer tb.Stop()

	initialIntervals, intervalsPerPeriod := tb.getRetentions(), 10

	// first compaction happens til 5 minutes elapsed
	for i := 0; i < initialIntervals-1; i++ {
		compactable.Want(nil) // no compaction
		time.Sleep(tb.getRetryInterval())
	}

	// first compaction the compact revision will be 100+1
	expectedRevision := int64(100 + 1)
	compactable.Want(&pb.CompactionRequest{Revision: expectedRevision})

	// compaction doesn't happens at every interval since revision not change
	for i := 0; i < 5; i++ {
		for j := 0; j < intervalsPerPeriod; j++ {
			compactable.Want(nil) // no compaction
			time.Sleep(tb.getRetryInterval())
		}
	}
	synctest.Wait()

	// Note: The original version of the loop below iterates initialIntervals times.
	// The rewritten version of the test uses initialIntervals+1.
	//
	// The change is because in the original version of the test,
	// advancing the fake clock follows the following sequence of events:
	//
	//   1. advance the clock, waking the Periodic's goroutine
	//   2. the Periodic considers whether to perform a compaction
	//   3. the Periodic blocks reading from the fakeRevGetter
	//   4. the test calls waitOneAction, unblocking the Periodic
	//   5. the Periodic sleeps for retryInterval
	//
	// In the original version of the test, we exit the nested loops above
	// with the Periodic at step 3--blocked reading from the fakeRevGetter.
	// When the loop below calls waitOneAction for the fist time,
	// the fakeRevGetter increments its revision and returns 102.
	//
	// In the current version of the test, the entire sequence of events
	// occurs when we call time.Sleep: The clock advances, and
	// the Periodic wakes, reads a revision, and sleeps again.
	// When we exit the nested loops above, the Periodic has already read
	// revision 101.
	//
	// The design of Periodic means that its decision to perform compactions
	// lags the change in revision by one polling cycle: When it wakes,
	// it decides to perform a compaction based on the previously read
	// revisions. This means that we need to wait initialIntervals plus
	// one additional cycles for the compaction to occur.
	//
	// In the original version of the test, that additional cycle is the last
	// iteration of the nested loops above. In the rewritten version,
	// it is explicit in the loop below.

	// when revision changed, compaction is normally
	for i := 0; i < initialIntervals+1; i++ {
		compactable.Want(nil) // no compaction
		rg.IncRev()
		time.Sleep(tb.getRetryInterval())
	}

	expectedRevision = int64(100 + 2)
	compactable.Want(&pb.CompactionRequest{Revision: expectedRevision})
}
