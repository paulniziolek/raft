package raftstate

import "sync/atomic"

// The state for a single raft node
type RaftState struct {
	state State

	currTerm uint64
}

func (r *RaftState) GetState() State {
	stateAddr := (*uint32)(&r.state)
	return State(atomic.LoadUint32(stateAddr))
}

func (r *RaftState) SetState(s State) {
	stateAddr := (*uint32)(&r.state)
	atomic.StoreUint32(stateAddr, uint32(s))
}

func (r *RaftState) GetTerm() int {
	termAddr := (*uint64)(&r.currTerm)
	return int(atomic.LoadUint64(termAddr))
}

func (r *RaftState) SetTerm(newTerm uint64) {
	termAddr := (*uint64)(&r.currTerm)
	atomic.StoreUint64(termAddr, uint64(newTerm))
}
