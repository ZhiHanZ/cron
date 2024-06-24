package cron

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Many tests schedule a job for every second, and then wait at most a second
// for it to run.  This amount is just slightly larger than 1 second to
// compensate for a few milliseconds of runtime.
const ONE_SECOND = 1*time.Second + 10*time.Millisecond

// Start and stop cron with no entries.
func TestNoEntries(t *testing.T) {
	cron := New()
	cron.Start()

	select {
	case <-time.After(ONE_SECOND):
		t.FailNow()
	case <-stop(cron):
	}
}

// Start, stop, then add an entry. Verify entry doesn't run.
func TestStopCausesJobsToNotRun(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	cron := New()
	cron.Start()
	cron.Stop()
	cron.AddFunc("* * * * * ?", func() { wg.Done() }, "test1", nil)

	select {
	case <-time.After(ONE_SECOND):
		// No job ran!
	case <-wait(wg):
		t.FailNow()
	}
}

// Add a job, start cron, expect it runs.
func TestAddBeforeRunning(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	cron := New()
	cron.AddFunc("* * * * * ?", func() { wg.Done() }, "test2", nil)
	cron.Start()
	defer cron.Stop()

	// Give cron 2 seconds to run our job (which is always activated).
	select {
	case <-time.After(ONE_SECOND):
		t.FailNow()
	case <-wait(wg):
	}
}

// Start cron, add a job, expect it runs.
func TestAddWhileRunning(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	cron := New()
	cron.Start()
	defer cron.Stop()
	cron.AddFunc("* * * * * ?", func() { wg.Done() }, "test3", nil)

	select {
	case <-time.After(ONE_SECOND):
		t.FailNow()
	case <-wait(wg):
	}
}

// Test timing with Entries.
func TestSnapshotEntries(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	cron := New()
	cron.AddFunc("@every 2s", func() { wg.Done() }, "test4", nil)
	cron.Start()
	defer cron.Stop()

	// Cron should fire in 2 seconds. After 1 second, call Entries.
	time.Sleep(ONE_SECOND)

	// Even though Entries was called, the cron should fire at the 2 second mark.
	select {
	case <-time.After(ONE_SECOND):
		t.FailNow()
	case <-wait(wg):
	}

}

// Test that the entries are correctly sorted.
// Add a bunch of long-in-the-future entries, and an immediate entry, and ensure
// that the immediate entry runs immediately.
// Also: Test that multiple jobs run in the same instant.
func TestMultipleEntries(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(2)

	cron := New()
	cron.AddFunc("0 0 0 1 1 ?", func() {}, "test5", nil)
	cron.AddFunc("* * * * * ?", func() { wg.Done() }, "test6", nil)
	cron.AddFunc("0 0 0 31 12 ?", func() {}, "test7", nil)
	cron.AddFunc("* * * * * ?", func() { wg.Done() }, "test8", nil)

	cron.Start()
	defer cron.Stop()

	select {
	case <-time.After(ONE_SECOND):
		t.FailNow()
	case <-wait(wg):
	}
}

// Test running the same job twice.
func TestRunningJobTwice(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(2)

	cron := New()
	cron.AddFunc("0 0 0 1 1 ?", func() {}, "test9", nil)
	cron.AddFunc("0 0 0 31 12 ?", func() {}, "test10", nil)
	cron.AddFunc("* * * * * ?", func() { wg.Done() }, "test11", nil)

	cron.Start()
	defer cron.Stop()

	select {
	case <-time.After(2 * ONE_SECOND):
		t.FailNow()
	case <-wait(wg):
	}
}

func TestRunningMultipleSchedules(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(2)

	cron := New()
	cron.AddFunc("0 0 0 1 1 ?", func() {}, "test12", nil)
	cron.AddFunc("0 0 0 31 12 ?", func() {}, "test13", nil)
	cron.AddFunc("* * * * * ?", func() { wg.Done() }, "test14", nil)
	cron.Schedule(Every(time.Minute), FuncJob(func() {}), "test15")
	cron.Schedule(Every(time.Second), FuncJob(func() { wg.Done() }), "test16")
	cron.Schedule(Every(time.Hour), FuncJob(func() {}), "test17")

	cron.Start()
	defer cron.Stop()

	select {
	case <-time.After(2 * ONE_SECOND):
		t.FailNow()
	case <-wait(wg):
	}
}

func TestRunEverMsWithOneMs(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(3)
	var occupied atomic.Bool

	cron := New()
	cron.tight = true
	s := time.Now()
	cron.Schedule(Every(500*time.Millisecond), FuncJob(func() {
		start := time.Now()
		if occupied.Load() {
			return
		}
		occupied.Store(true)
		defer func() {
			occupied.Store(false)
		}()
		time.Sleep(100 * time.Millisecond)
		t.Logf("exec %v", time.Since(start))
		wg.Done()
	}), "test20")
	cron.Start()
	defer cron.Stop()

	select {
	case <-time.After(2 * ONE_SECOND):
		t.FailNow()
	case <-wait(wg):
		t.Log(time.Since(s))
		if time.Since(s) < 1500*time.Millisecond {
			t.Errorf("time %v", time.Since(s))
		}
	}
}

func TestRunEveryMs(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(3)
	var occupied atomic.Bool

	cron := New()
	cron.tight = true
	// expected schedule
	// 0s: schedule 1 task
	// 500ms: skip no task
	// 600ms: start 2 task
	// 1000ms: skip no task
	// 1100ms: 3 task
	// 1500ms: skip no task
	s := time.Now()
	cron.Schedule(Every(500*time.Millisecond), FuncJob(func() {
		start := time.Now()
		if occupied.Load() {
			return
		}
		occupied.Store(true)
		defer func() {
			occupied.Store(false)
		}()
		time.Sleep(600 * time.Millisecond)
		t.Logf("exec %v", time.Since(start))
		wg.Done()
	}), "test19")
	cron.Start()
	defer cron.Stop()

	select {
	case <-time.After(3 * ONE_SECOND):
		t.FailNow()
	case <-wait(wg):
		t.Log(time.Since(s))
		if time.Since(s) < 1800*time.Millisecond {
			t.Errorf("time %v", time.Since(s))
		}
	}
}

func TestRunTight(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(4)
	var occupied atomic.Bool
	cron := New()
	cron.tight = true
	// expected schedule
	// 0s: schedule 1 task
	// 1s: skip no task
	// 1.1s: start 2 task
	// 2s: skip no task
	// 2.2s: 3 task
	// 3s: skip no task
	// 3.3s: 4 task
	// 4s: skip no task
	// 4.4s: finished all
	s := time.Now()
	cron.Schedule(Every(1*time.Second), FuncJob(func() {
		start := time.Now()
		if occupied.Load() {
			return
		}
		occupied.Store(true)
		defer func() {
			occupied.Store(false)
		}()
		time.Sleep(1100 * time.Millisecond)
		t.Logf("exec %v", time.Since(start))
		wg.Done()
	}), "test18")
	cron.Start()
	defer cron.Stop()
	select {
	case <-time.After(6 * ONE_SECOND):
		t.FailNow()
	case <-wait(wg):
		t.Log(time.Since(s))
		if time.Since(s) < 4400*time.Millisecond {
			t.Errorf("time %v", time.Since(s))
		}
	}
}

func TestRunNoTight(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(4)
	var occupied atomic.Bool
	cron := New()
	cron.tight = false
	// expected schedule
	// 0s: schedule 1 task
	// 1s: skip no task
	// 2s: start 2 task
	// 3s: skip no task
	// 4s: 1 task finished, 3 task
	// 3s: skip no task
	// 6s: 1 task finished, 4 task
	// 7s: skip no task
	// 7.1s: finished all
	skipTimes := 0
	s := time.Now()
	cron.Schedule(Every(1*time.Second), FuncJob(func() {
		start := time.Now()
		if occupied.Load() {
			skipTimes++
			return
		}
		occupied.Store(true)
		defer func() {
			occupied.Store(false)
		}()
		time.Sleep(1100 * time.Millisecond)
		t.Logf("exec %v", time.Since(start))
		wg.Done()
	}), "test18")
	cron.Start()
	defer cron.Stop()
	select {
	case <-time.After(9 * ONE_SECOND):
		t.FailNow()
	case <-wait(wg):
		if time.Since(s) < 7*time.Second {
			t.Errorf("time %v", time.Since(s))
		}
		if skipTimes != 4 {
			t.Errorf("skipTimes %v", skipTimes)
		}
	}
}

// Test that the cron is run in the local time zone (as opposed to UTC).
func TestLocalTimezone(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	now := time.Now().Local()
	spec := fmt.Sprintf("%d %d %d %d %d ?",
		now.Second()+1, now.Minute(), now.Hour(), now.Day(), now.Month())

	cron := New()
	cron.AddFunc(spec, func() { wg.Done() }, "test18", nil)
	cron.Start()
	defer cron.Stop()

	select {
	case <-time.After(ONE_SECOND):
		t.FailNow()
	case <-wait(wg):
	}
}

type testJob struct {
	wg   *sync.WaitGroup
	name string
}

func (t testJob) Run() {
	t.wg.Done()
}

// Simple test using Runnables.
func TestJob(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	cron := New()
	cron.AddJob("0 0 0 30 Feb ?", testJob{wg, "job0"}, "test19", nil)
	cron.AddJob("0 0 0 1 1 ?", testJob{wg, "job1"}, "test20", nil)
	cron.AddJob("* * * * * ?", testJob{wg, "job2"}, "test21", nil)
	cron.AddJob("1 0 0 1 1 ?", testJob{wg, "job3"}, "test22", nil)
	cron.Schedule(Every(5*time.Second+5*time.Nanosecond), testJob{wg, "job4"}, "test23")
	cron.Schedule(Every(5*time.Minute), testJob{wg, "job5"}, "test24")

	cron.Start()
	defer cron.Stop()

	select {
	case <-time.After(ONE_SECOND):
		t.FailNow()
	case <-wait(wg):
	}

	// Ensure the entries are in the right order.
	expecteds := []string{"job2", "job4", "job5", "job1", "job3", "job0"}

	var actuals []string
	for _, entry := range cron.Entries() {
		actuals = append(actuals, entry.Job.(testJob).name)
	}

	for i, expected := range expecteds {
		if actuals[i] != expected {
			t.Errorf("Jobs not in the right order.  (expected) %s != %s (actual)", expecteds, actuals)
			t.FailNow()
		}
	}
}

// Add a job, start cron, remove the job, expect it to have not run
func TestAddBeforeRunningThenRemoveWhileRunning(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	cron := New()
	cron.AddFunc("* * * * * ?", func() { wg.Done() }, "test25", nil)
	cron.Start()
	defer cron.Stop()
	cron.RemoveJob("test25")

	// Give cron 2 seconds to run our job (which is always activated).
	select {
	case <-time.After(ONE_SECOND):
	case <-wait(wg):
		t.FailNow()
	}
}

// Add a job, remove the job, start cron, expect it to have not run
func TestAddBeforeRunningThenRemoveBeforeRunning(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	cron := New()
	cron.AddFunc("* * * * * ?", func() { wg.Done() }, "test26", nil)
	cron.RemoveJob("test26")
	cron.Start()
	defer cron.Stop()

	// Give cron 2 seconds to run our job (which is always activated).
	select {
	case <-time.After(ONE_SECOND):
	case <-wait(wg):
		t.FailNow()
	}
}

func wait(wg *sync.WaitGroup) chan bool {
	ch := make(chan bool)
	go func() {
		wg.Wait()
		ch <- true
	}()
	return ch
}

func stop(cron *Cron) chan bool {
	ch := make(chan bool)
	go func() {
		cron.Stop()
		ch <- true
	}()
	return ch
}
