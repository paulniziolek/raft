package raftstate

type State uint32

const (
	Follower State = iota
	Leader
	Candidate
)

func (s State) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Leader:
		return "Leader"
	case Candidate:
		return "Candidate"
	default:
		return "Unknown"
	}
}
