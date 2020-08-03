package namesrv

import "time"

type Event func()

type Scheduler struct {
	timers map[int]timer
}

type timer struct {
	timer *time.Ticker
	stopChan chan bool
	event Event
}

func NewScheduler() *Scheduler {
	return &Scheduler{
		make(map[int]timer),
	}
}

func (t *Scheduler) Add(id int, period time.Duration, event Event) {
	_, ok := t.timers[id]
	if ok {
		return
	}
	timer := timer{
		time.NewTicker(period),
		make(chan bool),
		event,
	}
	t.timers[id] = timer
	timer.start()
}

func (t *Scheduler) Del(id int) {
	timer, ok := t.timers[id]
	if !ok {
		return
	}
	timer.stop()
	delete(t.timers, id)
}

func (t *Scheduler) Start() {
	for _, timer := range t.timers {
		timer.start()
	}
}

func (t *Scheduler) Stop() {
	for _, timer := range t.timers {
		timer.stop()
	}
}

func (t *timer) start() {
	go func() {
		for {
			select {
			case <- t.timer.C:
				t.event()
			case <- t.stopChan:
				t.timer.Stop()
				return
			}
		}
	}()
}

func (t *timer) stop() {
	t.stopChan <- true
	close(t.stopChan)
}