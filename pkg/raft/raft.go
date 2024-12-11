package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/paulniziolek/raft/pkg/raft/raftconfig"
	"github.com/paulniziolek/raft/pkg/raft/raftstate"
	"github.com/paulniziolek/raft/pkg/rpc"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex       // Lock to protect shared access to this peer's state
	peers     []*rpc.ClientEnd // RPC end points of all peers
	persister *Persister       // Object to hold this peer's persisted state
	me        int              // this peer's index into peers[]
	dead      int32            // set by Kill()

	// channels
	refreshCh  chan struct{}
	shutdownCh chan struct{}

	config      *raftconfig.RaftConfig
	raftState   *raftstate.RaftState
	lastContact time.Time
	logger      zerolog.Logger
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	state := rf.raftState.GetState()
	isLeader := state == raftstate.Leader

	term := rf.raftState.GetTerm()

	rf.logger.Debug().
		Int("term", int(term)).
		Bool("isLeader", isLeader).
		Msg("GetState called")

	return int(term), isLeader
}

// Returns the number of votes needed for a candidate to win majority over the cluster
func (rf *Raft) getQuorumSize() int {
	// We assume all peers are running servers and Voters.
	return len(rf.peers)/2 + 1
}

func (rf *Raft) LastContact() (last time.Time) {
	rf.mu.Lock()
	last = rf.lastContact
	rf.mu.Unlock()
	return last
}

func (rf *Raft) setLastContact() {
	rf.mu.Lock()
	rf.lastContact = time.Now()
	rf.mu.Unlock()
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	index = -1
	term = -1
	isLeader = true

	// Your code here (2B).

	rf.logger.Info().
		Interface("command", command).
		Msg("Start called")

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	rf.shutdownCh <- struct{}{}
	rf.logger.Info().Msg("Kill called")
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*rpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.config = raftconfig.NewRaftConfig()
	rf.raftState = raftstate.NewRaftState()
	rf.refreshCh = make(chan struct{}, 1)
	rf.shutdownCh = make(chan struct{}, 1)

	fileName := fmt.Sprintf("node%d.log", me)
	file, _ := os.OpenFile(fileName, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
	rf.logger = log.Output(zerolog.ConsoleWriter{
		Out:        file,
		TimeFormat: time.RFC3339,
		NoColor:    true,
	}).With().Int("nodeID", me).Logger()

	go rf.run()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func (rf *Raft) run() {
	for {
		select {
		case <-rf.shutdownCh:
			// TODO: perform shutdown
			return
		default:
		}

		switch rf.raftState.GetState() {
		case raftstate.Follower:
			rf.runFollower()
		case raftstate.Candidate:
			rf.runCandidate()
		case raftstate.Leader:
			rf.runLeader()
		}
	}
}

func (rf *Raft) runFollower() {
	// TODO: Impl follower logic
	electionTimer := rf.config.RandomElectionTimeout()

	rf.logger.Info().Msg("Running as Follower")

	for rf.raftState.GetState() == raftstate.Follower {
		select {
		case <-electionTimer:
			lastContact := rf.LastContact()
			electionTimer = rf.config.RandomElectionTimeout()

			if time.Since(lastContact) < time.Duration(rf.config.ElectionTimeout) {
				continue
			}

			rf.logger.Warn().Msg("Follower HB timeout reached, starting election")
			rf.raftState.SetState(raftstate.Candidate)
			return

		case <-rf.shutdownCh:
			rf.shutdownCh <- struct{}{}
			return
		}
	}
}

func (rf *Raft) runCandidate() {
	// TODO: Impl candidate logic
	rf.logger.Info().Msg("Running as Candidate")
	electionTimer := rf.config.RandomElectionTimeout()
	var voteCh <-chan *RequestVoteReply

	lastTerm := rf.raftState.GetTerm()
	rf.raftState.SetTerm(lastTerm + 1)

	voteCh = rf.voteSelf()

	// TODO: need to transition to Candidate State and send out VoteRequest RPCs to all peers
	votesNeeded := rf.getQuorumSize()
	votes := 0

	for rf.raftState.GetState() == raftstate.Candidate {
		select {
		case voteReply := <-voteCh:
			// if a peer is on a higher term, we should step down from candidate
			if voteReply.Term > rf.raftState.GetTerm() {
				rf.raftState.SetState(raftstate.Follower)
				return
			}

			if voteReply.VoteGranted {
				votes++
			}

			if votes == votesNeeded {
				// convert to leader
				rf.raftState.SetState(raftstate.Leader)
				return
			}

		case <-electionTimer:
			rf.logger.Warn().Msg("Candidate election timeout reached, restarting election")
			return

		case <-rf.shutdownCh:
			rf.shutdownCh <- struct{}{}
			rf.raftState.SetState(raftstate.Follower)
			return

		case <-rf.refreshCh:
		}
	}
	rf.logger.Info().Msg("Stepping down as candidate")
}

// Votes for self and requests votes from all peers
func (rf *Raft) voteSelf() <-chan *RequestVoteReply {
	voteCh := make(chan *RequestVoteReply, rf.getQuorumSize())

	requestVoteArgs := &RequestVoteArgs{
		Term:        rf.raftState.GetTerm(),
		CandidateId: rf.me,
		// TODO: Fix the log index/term once log entry struct is implemented
		LastLogIndex: -1,
		LastLogTerm:  -1,
	}

	for i := range rf.peers {
		// Allowing for Candidate to vote for self
		go func() {
			// TODO: check if RPC was sent/received
			reply := &RequestVoteReply{}
			_ = rf.SendRequestVote(i, requestVoteArgs, reply)
			voteCh <- reply
		}()
	}

	return voteCh
}

func (rf *Raft) runLeader() {
	// TODO: Impl leader logic
	rf.logger.Info().Msg("Running as Leader")
	hbTimer := rf.config.GetHeartbeatTimer()

	for rf.raftState.GetState() == raftstate.Leader {
		select {
		case <-hbTimer:
			// send HB to all servers
			hbTimer = rf.config.GetHeartbeatTimer()
			rf.sendHeartbeat()

		case <-rf.shutdownCh:
			rf.shutdownCh <- struct{}{}
			rf.raftState.SetState(raftstate.Follower)
			return

		case <-rf.refreshCh:
		}
	}
	rf.logger.Info().Msg("Stepping down as leader")
}

func (rf *Raft) sendHeartbeat() {
	appendEntryArgs := &AppendEntryArgs{
		Term:     rf.raftState.GetTerm(),
		LeaderID: rf.me,
		// TODO: add log index and other relevant append entry stuff here once entry is implemented
	}

	for i := range rf.peers {
		if i == rf.me {
			// don't need to send HB to self
			continue
		}
		go func() {
			reply := &AppendEntryReply{}
			_ = rf.SendAppendEntry(i, appendEntryArgs, reply)
			// TODO: process append entry reply
		}()
	}
}
