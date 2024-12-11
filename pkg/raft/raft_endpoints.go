package raft

import "github.com/paulniziolek/raft/pkg/raft/raftstate"

type RequestVoteArgs struct {
	Term         uint64
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        uint64
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.setLastContact()
	// we should have differing logic depending on if the current state is follower, leader, or candidate
	// for followers, we should always grant the vote if the term is higher AND their log is at least as up-to-date as the followers
	// AND the current term our votedFor is nil (or -1)

	// for leaders, we should only step down as leader if the term is higher
	// for candidates, we should deny the vote and respond with the current term

	currTerm := rf.raftState.GetTerm()
	reply.Term = currTerm

	if currTerm > args.Term && args.CandidateId != rf.me {
		// deny the vote, our term is higher (or equal to)
		rf.logger.Info().Int("Requesting node ID", args.CandidateId).
			Uint64("Requesting node's term", args.Term).
			Uint64("Receiver term", currTerm).
			Msg("Rejecting RequestVote, our term is higher or equal to candidate")
		return
	}

	switch rf.raftState.GetState() {
	case raftstate.Follower:
		// TODO: add voting condition of only when the log is up to date once logs are implemented
		if rf.raftState.GetVotedFor() != -1 && currTerm == args.Term {
			// deny the vote, we have already votedFor another candidate
			rf.logger.Info().Int("Requesting node ID", args.CandidateId).
				Uint64("Requesting node's term", args.Term).
				Uint64("Receiver term", currTerm).
				Int32("Receiver votedFor", rf.raftState.GetVotedFor()).
				Msg("Rejecting RequestVote, we have already voted in the current term")
			return
		}

		rf.logger.Info().Int("Requesting node ID", args.CandidateId).
			Uint64("Requesting node's term", args.Term).
			Msg("Accepting RequestVote and voting for candidate")

	case raftstate.Candidate:
		// we step down as candidate since our term is lower
		// need to figure out how to exit from runCandidate loop, perhaps using some refresh channel lol
		if args.CandidateId == rf.me {
			rf.logger.Info().Int("Requesting node ID", args.CandidateId).
				Uint64("Requesting node's term", args.Term).
				Uint64("Receiver term", currTerm).
				Msg("Accepting RequestVote for self")
			break
		}

		rf.logger.Warn().Int("Requesting node ID", args.CandidateId).
			Uint64("Requesting node's term", args.Term).
			Uint64("Receiver term", currTerm).
			Msg("Accepting RequestVote and stepping down as Candidate, our term is lower")
		rf.raftState.SetState(raftstate.Follower)
		rf.refreshCh <- struct{}{}

	case raftstate.Leader:
		// we step down as leader since our term is lower
		// Need to figure out leader cleanup actions and exit from runLeader loop
		rf.logger.Warn().Int("Requesting node ID", args.CandidateId).
			Uint64("Requesting node's term", args.Term).
			Uint64("Receiver term", currTerm).
			Msg("Accepting RequestVote and stepping down as Leader, our term is lower")
		rf.raftState.SetState(raftstate.Follower)
		rf.refreshCh <- struct{}{}
	}

	rf.raftState.SetTerm(args.Term)
	reply.Term = args.Term
	reply.VoteGranted = true
	rf.raftState.SetVotedFor(int32(args.CandidateId))
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) SendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntryArgs struct {
	Term     uint64
	LeaderID int

	PrevLogIndex       int
	PrevLogTerm        int
	Entries            []*interface{} // TODO: Design entry struct
	LeadersCommitIndex int
}

type AppendEntryReply struct {
	Term    uint64
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.setLastContact()

	// Logic with append entries:
	// 1. reply false if term < currTerm
	// 2. reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
	// 3. if an existing entry conflicts with new one (same idx but diff terms), delete existing entry and all that follow it
	// 4. append any new entries not in the log
	// 5. if leaderCommitIndex > commitIndex, set commitIndex = min(leaderCommitIndex, index of last new entry)
	// TODO: finish 2-5
	currTerm := rf.raftState.GetTerm()
	reply.Term = currTerm
	reply.Success = true

	if args.Term < currTerm {
		rf.logger.Warn().Uint64("receiver term", currTerm).
			Uint64("requester term", args.Term).
			Int("requester id", args.LeaderID).
			Msg("rejecting AppendEntry, our term is higher")

		reply.Success = false
		// TODO: actually process this rejected AppendEntry()
	}

	if len(args.Entries) == 0 {
		return
	}
}

func (rf *Raft) SendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
}
