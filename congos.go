package congos

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

type ConGos struct {
	stop         chan struct{}
	quit         chan struct{}
	pendingTasks chan interface{}
	tokenBucket  chan struct{}
	concurrency  int32
	running      int32
	callback     func(interface{})
}

func NewConGos(maxPending, maxConcurrency int, fn func(interface{})) *ConGos {
	c := &ConGos{
		callback:     fn,
		concurrency:  0,
		running:      0,
		quit:         make(chan struct{}),
		stop:         make(chan struct{}),
		pendingTasks: make(chan interface{}, maxPending),
		tokenBucket:  make(chan struct{}, maxConcurrency),
	}

	for i := 0; i < maxConcurrency; i++ {
		c.tokenBucket <- struct{}{}
	}

	return c
}

func (c *ConGos) Start(ctx context.Context) error {
	if !atomic.CompareAndSwapInt32(&c.running, 0, 1) {
		return errors.New("ConGo is already running")
	}
	var wg sync.WaitGroup
	for {
		select {
		case <-ctx.Done():
			goto stop
		case <-c.stop:
			goto stop
		case task := <-c.pendingTasks:
			wg.Add(1)
			<-c.tokenBucket
			go func() {
				atomic.AddInt32(&c.concurrency, 1)
				defer func() {
					wg.Done()
					atomic.AddInt32(&c.concurrency, -1)
					c.tokenBucket <- struct{}{}
				}()
				c.callback(task)
			}()
		}
	}
stop:
	wg.Done()
	//todo process pending tasks
	c.quit <- struct{}{}
	return nil
}

func (c *ConGos) Stop() error {
	if !atomic.CompareAndSwapInt32(&c.running, 1, 0) {
		return errors.New("ConGo is not running")
	}
	c.stop <- struct{}{}
	<-c.quit
	return nil
}

func (c *ConGos) Append(task interface{}) error {
	c.pendingTasks <- task
	return nil
}
