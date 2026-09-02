// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package proxy

import (
	"runtime"
	"sync"

	"github.com/CodisLabs/codis/pkg/models"
	"github.com/CodisLabs/codis/pkg/utils/sync2/atomic2"
)

type Slot struct {
	id   int
	lock struct {
		hold bool
		sync.RWMutex
	}
	refs sync.WaitGroup
	// mgetPlanState is a lock-free admission gate for merged-MGET route
	// planning. The high bit blocks new planners; low bits count planners that
	// are currently reading this slot's route and adding refs.
	mgetPlanState atomic2.Int64

	switched bool

	backend, migrate struct {
		id int
		bc *sharedBackendConn
	}
	replicaGroups [][]*sharedBackendConn

	method forwardMethod
}

func (s *Slot) snapshot() *models.Slot {
	var m = &models.Slot{
		Id:     s.id,
		Locked: s.lock.hold,

		BackendAddr:        s.backend.bc.Addr(),
		BackendAddrGroupId: s.backend.id,
		MigrateFrom:        s.migrate.bc.Addr(),
		MigrateFromGroupId: s.migrate.id,
		ForwardMethod:      s.method.GetId(),
	}
	for i := range s.replicaGroups {
		var group []string
		for _, bc := range s.replicaGroups[i] {
			group = append(group, bc.Addr())
		}
		m.ReplicaGroups = append(m.ReplicaGroups, group)
	}
	return m
}

func (s *Slot) blockAndWait() {
	if !s.lock.hold {
		s.lock.hold = true
		s.blockMGetRoutePlanning()
		s.lock.Lock()
	}
	s.refs.Wait()
}

func (s *Slot) unblock() {
	if !s.lock.hold {
		return
	}
	s.lock.hold = false
	s.lock.Unlock()
	s.unblockMGetRoutePlanning()
}

const mgetPlanBlocked int64 = 1 << 62

func (s *Slot) tryAcquireMGetRoutePlan() bool {
	// Atomic Add gives readers a single-instruction fast path on amd64. If a
	// writer closed the gate before this Add, roll back immediately; otherwise
	// the writer observes this reader and waits for its matching release.
	if state := s.mgetPlanState.Add(1); state&mgetPlanBlocked != 0 {
		s.mgetPlanState.Sub(1)
		return false
	}
	return true
}

func (s *Slot) releaseMGetRoutePlan() {
	s.mgetPlanState.Sub(1)
}

func (s *Slot) blockMGetRoutePlanning() {
	// A second block would overflow the reserved high-bit range and become
	// smaller than mgetPlanBlocked. Router serializes writers, so this is only
	// an invariant check.
	if state := s.mgetPlanState.Add(mgetPlanBlocked); state < mgetPlanBlocked {
		panic("mget route planning is already blocked")
	}
	// Route planning contains no network IO and releases its token immediately
	// after refs.Add. Topology updates are rare, so yielding here keeps the MGET
	// hot path free from a mutex/condition-variable wakeup.
	for s.mgetPlanState.Int64() != mgetPlanBlocked {
		runtime.Gosched()
	}
}

func (s *Slot) unblockMGetRoutePlanning() {
	if state := s.mgetPlanState.Sub(mgetPlanBlocked); state != 0 {
		panic("mget route planning unblocked with active planners")
	}
}

func (s *Slot) forward(r *Request, hkey []byte) error {
	return s.method.Forward(s, r, hkey)
}
