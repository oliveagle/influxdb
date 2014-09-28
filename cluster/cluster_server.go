package cluster

import (
	"errors"
	"fmt"
	"net"
	"time"

	log "code.google.com/p/log4go"
	c "github.com/influxdb/influxdb/configuration"
	"github.com/influxdb/influxdb/protocol"
)

const (
	HEARTBEAT_TIMEOUT = 100 * time.Millisecond
)

type ClusterServer struct {
	Id                       uint32
	RaftName                 string
	State                    ServerState
	RaftConnectionString     string
	ProtobufConnectionString string
	connection               ServerConnection
	HeartbeatInterval        time.Duration
	Backoff                  time.Duration
	MinBackoff               time.Duration
	MaxBackoff               time.Duration
	isUp                     bool
	writeBuffer              *WriteBuffer
	heartbeatStarted         bool
}

type ServerConnection interface {
	Connect()
	Close()
	ClearRequests()
	MakeRequest(*protocol.Request, ResponseChannel) error
	CancelRequest(*protocol.Request)
}

type ServerState int

const (
	LoadingRingData ServerState = iota
	SendingRingData
	DeletingOldData
	Running
	Potential
)

func (self *ClusterServer) GetStateName() (stateName string) {
	switch {
	case self.State == LoadingRingData:
		return "LoadingRingData"
	case self.State == SendingRingData:
		return "SendingRingData"
	case self.State == DeletingOldData:
		return "DeletingOldData"
	case self.State == Running:
		return "Running"
	case self.State == Potential:
		return "Potential"
	}
	return "UNKNOWN"
}

func NewClusterServer(raftName, raftConnectionString, protobufConnectionString string, connection ServerConnection, config *c.Configuration) *ClusterServer {

	s := &ClusterServer{
		RaftName:                 raftName,
		RaftConnectionString:     raftConnectionString,
		ProtobufConnectionString: protobufConnectionString,
		connection:               connection,
		HeartbeatInterval:        config.ProtobufHeartbeatInterval.Duration,
		Backoff:                  config.ProtobufMinBackoff.Duration,
		MinBackoff:               config.ProtobufMinBackoff.Duration,
		MaxBackoff:               config.ProtobufMaxBackoff.Duration,
		heartbeatStarted:         false,
	}

	return s
}

func (self *ClusterServer) StartHeartbeat() {
	if self.heartbeatStarted {
		return
	}

	self.heartbeatStarted = true
	self.isUp = true
	go self.heartbeat()
}

func (self *ClusterServer) SetWriteBuffer(writeBuffer *WriteBuffer) {
	self.writeBuffer = writeBuffer
}

func (self *ClusterServer) GetId() uint32 {
	return self.Id
}

func (self *ClusterServer) Connect() {
	self.connection.Connect()
}

func (self *ClusterServer) MakeRequest(request *protocol.Request, responseStream chan<- *protocol.Response) {
	err := self.connection.MakeRequest(request, NewResponseChannelWrapper(responseStream))
	if err != nil {
		self.connection.CancelRequest(request)
		self.markServerAsDown()
	}
}

func (self *ClusterServer) Write(request *protocol.Request) error {
	// self.connection.MakeRequest(request, response_channel)
}

func (self *ClusterServer) BufferWrite(request *protocol.Request) {
	self.writeBuffer.Write(request)
}

func (self *ClusterServer) IsUp() bool {
	return self.isUp
}

// private methods

var HEARTBEAT_TYPE = protocol.Request_HEARTBEAT

func (self *ClusterServer) heartbeat() {
	// 每阁self.HeartbeatInterval时间, 就发起一个heartbeatRequest.
	// 如果成功响应, 就标记self.isUp = True, 否则 self.markServerAsDown
}

func (self *ClusterServer) getHeartbeatResponse(responseChan <-chan *protocol.Response) error {
	// 每隔self.HeartbeatInterval就接收并处理一条heartbeat response
}

func (self *ClusterServer) markServerAsDown() {
	self.isUp = false
	self.connection.ClearRequests()
}

func (self *ClusterServer) handleHeartbeatError(err error) {
	self.markServerAsDown()
	time.Sleep(self.Backoff)
}

// in the coordinator test we don't want to create protobuf servers,
// so we just ignore creating a protobuf client when the connection
// string has a 0 port
func shouldConnect(addr string) bool {
}
