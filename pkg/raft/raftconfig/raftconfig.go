package raftconfig

import (
	"math/rand"
	"time"
)

const (
	defaultElectionTimeout       = 1000
	defaultElectionTimeoutOffset = 100
	defaultHeartbeat             = 200
)

type RaftConfig struct {
	// paper recommends 150-300ms, but default for this is 1000ms
	ElectionTimeout int64
	// max random offset for the election timeout, to prevent servers from becoming
	// election candidates all at the same time, default is 100ms
	ElectionTimeoutRandOffset int64
	// paper recommends considerably faster than 150-300ms, but default for this is
	// 200ms, since tester limits up to 10 heartbeats a second
	Heartbeat int64
}

func NewRaftConfig(options ...Option) *RaftConfig {
	config := &RaftConfig{
		ElectionTimeout:           defaultElectionTimeout,
		ElectionTimeoutRandOffset: defaultElectionTimeoutOffset,
		Heartbeat:                 defaultHeartbeat,
	}
	for _, option := range options {
		option(config)
	}
	config.ElectionTimeout += rand.Int63n(config.ElectionTimeoutRandOffset)
	return config
}

// Returns a random [election] timeout, calculated by the config ElectionTimeout + rand(0, ElectionTimeoutRandOffset)
func (rc *RaftConfig) RandomElectionTimeout() <-chan time.Time {
	baseTimeout := time.Duration(rc.ElectionTimeout) * time.Millisecond
	extra := time.Duration(rand.Int63n(rc.ElectionTimeoutRandOffset)) * time.Millisecond
	return time.After(baseTimeout + extra)
}
