package raftconfig

import (
	"math/rand"
	"time"
)

const (
	defaultElectionTimeout       = 1000
	defaultElectionTimeoutOffset = 500
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

func (rc *RaftConfig) RandomElectionTimeout() <-chan time.Time {
	baseTimeout := time.Duration(rc.ElectionTimeout) * time.Millisecond
	extra := time.Duration(rand.Int63n(rc.ElectionTimeoutRandOffset)) * time.Millisecond
	return time.After(baseTimeout + extra)
}

func (rc *RaftConfig) GetHeartbeatTimer() <-chan time.Time {
	timeout := time.Duration(rc.Heartbeat) * time.Millisecond
	return time.After(timeout)
}
