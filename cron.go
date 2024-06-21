// This library implements a cron spec parser and runner.  See the README for
// more details.
package cron

import (
	"github.com/sirupsen/logrus"
	"sort"
	"time"
)

type entries []*Entry

// Cron keeps track of any number of entries, invoking the associated func as
// specified by the schedule. It may be started, stopped, and the entries may
// be inspected while running.
type Cron struct {
	entries  entries
	stop     chan struct{}
	add      chan *Entry
	remove   chan string
	snapshot chan entries
	running  bool

	// Enable tight execution of jobs.
	// If this is true and the previous job execution time is greater than the schedule time,
	// then the next job will start immediately.
	tight    bool
	runEntry chan RunEntry
}

// Job is an interface for submitted cron jobs.
type Job interface {
	Run()
}

// The Schedule describes a job's duty cycle.
type Schedule interface {
	// Return the next activation time, later than the given time.
	// Next is invoked initially, and then each time the job is run.
	Next(time.Time) time.Time
}

type RunEntry struct {
	CurrentEntry *Entry
	EndTime      time.Time
}

func (e RunEntry) RunEarly() bool {
	return e.EndTime.After(e.CurrentEntry.Next)
}

// Entry consists of a schedule and the func to execute on that schedule.
type Entry struct {
	// The schedule on which this job should be run.
	Schedule Schedule

	// The next time the job will run. This is the zero time if Cron has not been
	// started or this entry's schedule is unsatisfiable
	Next time.Time

	// The last time this job was run. This is the zero time if the job has never
	// been run.
	Prev time.Time

	// The Job to run.
	Job Job

	// Unique name to identify the Entry so as to be able to remove it later.
	Name string
}

// byTime is a wrapper for sorting the entry array by time
// (with zero time at the end).
type byTime []*Entry

func (s byTime) Len() int      { return len(s) }
func (s byTime) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s byTime) Less(i, j int) bool {
	// Two zero times should return false.
	// Otherwise, zero is "greater" than any other time.
	// (To sort it at the end of the list.)
	if s[i].Next.IsZero() {
		return false
	}
	if s[j].Next.IsZero() {
		return true
	}
	return s[i].Next.Before(s[j].Next)
}

// New returns a new Cron job runner.
func New() *Cron {
	return &Cron{
		entries:  nil,
		add:      make(chan *Entry),
		remove:   make(chan string),
		stop:     make(chan struct{}),
		snapshot: make(chan entries),
		runEntry: make(chan RunEntry),
		running:  false,
		tight:    true,
	}
}

func (c *Cron) SetTight(tight bool) {
	c.tight = tight
}

func (c *Cron) IsTight() bool {
	return c.tight
}

// A wrapper that turns a func() into a cron.Job
type FuncJob func()

func (f FuncJob) Run() { f() }

// AddFunc adds a func to the Cron to be run on the given schedule.
func (c *Cron) AddFunc(spec string, cmd func(), name string, tz *time.Location) {
	c.AddJob(spec, FuncJob(cmd), name, tz)
}

// AddFunc adds a Job to the Cron to be run on the given schedule.
func (c *Cron) AddJob(spec string, cmd Job, name string, tz *time.Location) {
	c.Schedule(Parse(spec, tz), cmd, name)
}

// RemoveJob removes a Job from the Cron based on name.
func (c *Cron) RemoveJob(name string) {
	if !c.running {
		i := c.entries.pos(name)

		if i == -1 {
			return
		}

		c.entries = c.entries[:i+copy(c.entries[i:], c.entries[i+1:])]
		return
	}

	c.remove <- name
}

func (entrySlice entries) pos(name string) int {
	for p, e := range entrySlice {
		if e.Name == name {
			return p
		}
	}
	return -1
}

// Schedule adds a Job to the Cron to be run on the given schedule.
func (c *Cron) Schedule(schedule Schedule, cmd Job, name string) {
	entry := &Entry{
		Schedule: schedule,
		Job:      cmd,
		Name:     name,
	}

	if !c.running {
		i := c.entries.pos(entry.Name)
		if i != -1 {
			return
		}
		c.entries = append(c.entries, entry)
		return
	}

	c.add <- entry
}

// Entries returns a snapshot of the cron entries.
func (c *Cron) Entries() []*Entry {
	if c.running {
		c.snapshot <- nil
		x := <-c.snapshot
		return x
	}
	return c.entrySnapshot()
}

// Start the cron scheduler in its own go-routine.
func (c *Cron) Start() {
	c.running = true
	go c.run()
}

// Run every entry whose next time was this effective time.
func (c *Cron) execute(e *Entry) {
	start := time.Now().Local()
	e.Job.Run()
	if c.tight {

		end := time.Now().Local()
		elapsed := end.Sub(start)
		runEntry := RunEntry{
			CurrentEntry: e,
			EndTime:      e.Prev.Add(elapsed),
		}
		if runEntry.RunEarly() {
			c.runEntry <- runEntry
		}
	}
}

// Run the scheduler.. this is private just due to the need to synchronize
// access to the 'running' state variable.
func (c *Cron) run() {
	// Figure out the next activation times for each entry.
	now := time.Now().Local()
	for _, entry := range c.entries {
		entry.Next = entry.Schedule.Next(now)
	}

	for {
		// Determine the next entry to run.
		sort.Sort(byTime(c.entries))

		var effective time.Time
		if len(c.entries) == 0 || c.entries[0].Next.IsZero() {
			// If there are no entries yet, just sleep - it still handles new entries
			// and stop requests.
			effective = now.AddDate(10, 0, 0)
		} else {
			effective = c.entries[0].Next
		}

		select {
		case runEarlyEntry := <-c.runEntry:
			if c.tight && runEarlyEntry.RunEarly() {
				e := runEarlyEntry.CurrentEntry
				e.Prev = runEarlyEntry.EndTime
				e.Next = e.Schedule.Next(runEarlyEntry.EndTime)
				logrus.WithField("module", "cron").WithField("name", runEarlyEntry.CurrentEntry.Name).WithField("prev", runEarlyEntry.CurrentEntry.Prev).WithField("next", runEarlyEntry.CurrentEntry.Next).Infof("Start Run early entry")
				go c.execute(e)
				logrus.WithField("module", "cron").WithField("name", runEarlyEntry.CurrentEntry.Name).WithField("prev", runEarlyEntry.CurrentEntry.Prev).WithField("next", runEarlyEntry.CurrentEntry.Next).Infof("End Run early entry")

			}
			continue
		case now = <-time.After(effective.Sub(now)):
			// Run every entry whose next time was this effective time.
			for _, e := range c.entries {
				if e.Next != effective {
					break
				}
				e.Prev = e.Next
				e.Next = e.Schedule.Next(effective)
				logrus.WithField("module", "cron").WithField("name", e.Name).WithField("prev", e.Prev).WithField("next", e.Next).Infof("Start Run entry")
				go c.execute(e)
				logrus.WithField("module", "cron").WithField("name", e.Name).WithField("prev", e.Prev).WithField("next", e.Next).Infof("End Run entry")
			}
			continue

		case newEntry := <-c.add:
			i := c.entries.pos(newEntry.Name)
			if i != -1 {
				break
			}
			c.entries = append(c.entries, newEntry)
			newEntry.Next = newEntry.Schedule.Next(time.Now().Local())

		case name := <-c.remove:
			i := c.entries.pos(name)

			if i == -1 {
				break
			}

			c.entries = c.entries[:i+copy(c.entries[i:], c.entries[i+1:])]

		case <-c.snapshot:
			c.snapshot <- c.entrySnapshot()

		case <-c.stop:
			return
		default:
			// avoid bloc
			logrus.WithField("module", "cron").WithField("now", now).Infof("Default case")
		}

		// 'now' should be updated after newEntry and snapshot cases.
		now = time.Now().Local()
	}
}

// Stop the cron scheduler.
func (c *Cron) Stop() {
	c.stop <- struct{}{}
	c.running = false
}

// entrySnapshot returns a deep copy of the current cron entry list.
func (c *Cron) entrySnapshot() []*Entry {
	entries := []*Entry{}
	for _, e := range c.entries {
		entries = append(entries, &Entry{
			Schedule: e.Schedule,
			Next:     e.Next,
			Prev:     e.Prev,
			Job:      e.Job,
			Name:     e.Name,
		})
	}
	return entries
}
