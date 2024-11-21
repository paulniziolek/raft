package raftconfig

type Option func(*RaftConfig)

func WithElectionTimeout(timeout int64) Option {
	return func(config *RaftConfig) {
		config.ElectionTimeout = timeout
	}
}

func WithElectionTimeoutOffset(offset int64) Option {
	return func(config *RaftConfig) {
		config.ElectionTimeoutRandOffset = offset
	}
}

func WithHeartbeat(heartbeat int64) Option {
	return func(config *RaftConfig) {
		config.Heartbeat = heartbeat
	}
}
