package main

import (
	"time"

	"github.com/paulniziolek/raft/pkg/raft"
)

func main() {
	// TODO: invoke raft for CLI use
	raftConfig := raft.NewConfig(nil, 3, false)
	raftConfig.Begin("Starting RAFT cluster from main entry point")

	time.Sleep(10 * time.Second)

	raftConfig.End()
}
