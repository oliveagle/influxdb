package cluster

import (
	"fmt"
	"time"

	log "code.google.com/p/log4go"
	p "github.com/influxdb/influxdb/protocol"
)

/**
master -> update ShardData -> Add ExpendingServerIds    // 保证也往那台机器上写
master -> startExpendingShardData   // 保存任务信息
*/

type AddShardServer struct {
	RunningOnServerId uint32
	StartTime         time.Time
	FinishTime        time.Time
	ServerIds         []uint32
	IsFinished        bool
}

func NewAddShardServer(runningOn uint32, serverIds []uint32) *AddShardServer {
	return &AddShardServer{
		runningOnServerId: runningOn,
		ServerIds:         serverIds,
		StartTime:         nil,
		FinishTime:        nil,
		IsFinished:        false,
	}
}
