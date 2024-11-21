package raftstate

type State string

const (
	Follower  State = "FOLLOWER"
	Leader    State = "LEADER"
	Candidate State = "CANDIDATE"
	Unknown   State = ""
)
