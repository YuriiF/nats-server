// Copyright 2026 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"runtime"
	"sync/atomic"
)

// Used to limit number of disk IO calls in flight since they could all be blocking an OS thread.
// https://github.com/nats-io/nats-server/issues/2742
type diskIOSemaphore struct {
	ch      chan struct{}
	waiters atomic.Int64
}

func newDiskIOSemaphore(n int) *diskIOSemaphore {
	d := &diskIOSemaphore{ch: make(chan struct{}, n)}
	for range n {
		d.ch <- struct{}{}
	}
	return d
}

func defaultDiskIOSemaphore() *diskIOSemaphore {
	// Limit ourselves to a sensible number of blocking I/O calls.
	// Range between 4-16 concurrent disk I/Os based on CPU cores,
	// or 50% of cores if greater than 32 cores.
	mp := runtime.GOMAXPROCS(-1)
	nIO := min(16, max(4, mp))
	if mp > 32 {
		nIO = max(16, mp/2)
	}
	return newDiskIOSemaphore(nIO)
}

func raftDiskIOSemaphore() *diskIOSemaphore {
	// During election storms, Raft groups will issue a large
	// number of concurrent fsyncs.
	// A limit based on the number CPU cores makes poor use of
	// devices that can handle requests in parallel, and can
	// slow down elections, or cause them to time out, which
	// would require even more I/O.
	return newDiskIOSemaphore(512)
}

func (d *diskIOSemaphore) acquire() {
	select {
	case <-d.ch:
		return
	default:
		// No slot available, count this
		// waiter before blocking.
		d.waiters.Add(1)
		<-d.ch
		d.waiters.Add(-1)
	}
}

func (d *diskIOSemaphore) release() {
	d.ch <- struct{}{}
}

func (d *diskIOSemaphore) cap() int {
	return cap(d.ch)
}
