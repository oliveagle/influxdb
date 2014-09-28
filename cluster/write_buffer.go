package cluster

import (
	"reflect"
	"time"

	log "code.google.com/p/log4go"
	"github.com/influxdb/influxdb/protocol"
)

// Acts as a buffer for writes
type WriteBuffer struct {
	writer                     Writer
	wal                        WAL
	serverId                   uint32
	writes                     chan *protocol.Request
	stoppedWrites              chan uint32
	bufferSize                 int
	shardIds                   map[uint32]bool
	shardLastRequestNumber     map[uint32]uint32
	shardCommitedRequestNumber map[uint32]uint32
	writerInfo                 string
}

type Writer interface {
	Write(request *protocol.Request) error
}

func NewWriteBuffer(writerInfo string, writer Writer, wal WAL, serverId uint32, bufferSize int) *WriteBuffer {
	log.Info("%s: Initializing write buffer with buffer size of %d", writerInfo, bufferSize)
	buff := &WriteBuffer{
		writer:                     writer,
		wal:                        wal,
		serverId:                   serverId,
		writes:                     make(chan *protocol.Request, bufferSize),
		stoppedWrites:              make(chan uint32, 1),
		bufferSize:                 bufferSize,
		shardIds:                   make(map[uint32]bool),
		shardLastRequestNumber:     map[uint32]uint32{},
		shardCommitedRequestNumber: map[uint32]uint32{},
		writerInfo:                 writerInfo,
	}
	go buff.handleWrites()
	return buff
}

func (self *WriteBuffer) ShardsRequestNumber() map[uint32]uint32 {
	return self.shardLastRequestNumber
}

func (self *WriteBuffer) HasUncommitedWrites() bool {
	return !reflect.DeepEqual(self.shardCommitedRequestNumber, self.shardLastRequestNumber)
}

// This method never blocks. It'll buffer writes until they fill the buffer then drop the on the
// floor and let the background goroutine replay from the WAL
func (self *WriteBuffer) Write(request *protocol.Request) {
	// self.shardLastRequestNumber[request.GetShardId()] = request.GetRequestNumber()
	// self.writes <- request
	// 否则:
	// 		self.stoppedWrites <- *request.RequestNumber:
}

func (self *WriteBuffer) handleWrites() {
	for {
		select {
		case requestDropped := <-self.stoppedWrites:
			self.replayAndRecover(requestDropped)
		case request := <-self.writes:
			self.write(request)
		}
	}
}

func (self *WriteBuffer) write(request *protocol.Request) {
	// self.write.Write(request) -> self.wal.Commit(...)
	// 如果失败, 永远retry下去. 每次间隔100ms
}

func (self *WriteBuffer) replayAndRecover(missedRequest uint32) {
	// 先清空self.writes (写buffer)
	for {
		self.wal.RecoverServerFromRequestNumber(*req.RequestNumber, shardIds, func(request *protocol.Request, shardId uint32) error {
			self.write(request)
		})
	RequestLoop:
		for {
			newReq := <-self.writes
			//if error or default: break RequestLoop
		}
		// now make sure that no new writes were dropped. If so, do the replay again from this place.
		<-self.stoppedWrites
	}
}
