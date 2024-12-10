package raftstate

import "sync/atomic"

// The state for a single raft node
type RaftState struct {
	state State

	currTerm uint64

	votedFor int32
}

func NewRaftState() *RaftState {
	raftState := &RaftState{
		state:    Follower,
		currTerm: 0,
		votedFor: -1,
	}
	return raftState
}

func (r *RaftState) GetState() State {
	stateAddr := (*uint32)(&r.state)
	return State(atomic.LoadUint32(stateAddr))
}

func (r *RaftState) SetState(s State) {
	stateAddr := (*uint32)(&r.state)
	atomic.StoreUint32(stateAddr, uint32(s))
}

func (r *RaftState) GetTerm() uint64 {
	termAddr := (*uint64)(&r.currTerm)
	return atomic.LoadUint64(termAddr)
}

// Set Term has an effect of clearing votedFor
func (r *RaftState) SetTerm(newTerm uint64) {
	termAddr := (*uint64)(&r.currTerm)
	atomic.StoreUint64(termAddr, uint64(newTerm))

	r.SetVotedFor(-1)
}

func (r *RaftState) GetVotedFor() int32 {
	votedForAddr := (*int32)(&r.votedFor)
	return atomic.LoadInt32(votedForAddr)
}

func (r *RaftState) SetVotedFor(vote int32) {
	votedForAddr := (*int32)(&r.votedFor)
	atomic.StoreInt32(votedForAddr, vote)
}
