package cluster

import (
	"fmt"
	"sort"
	"strings"
	"time"

	log "code.google.com/p/log4go"
	"github.com/influxdb/influxdb/common"
	"github.com/influxdb/influxdb/engine"
	"github.com/influxdb/influxdb/metastore"
	"github.com/influxdb/influxdb/parser"
	p "github.com/influxdb/influxdb/protocol"
	"github.com/influxdb/influxdb/wal"
)

// A shard implements an interface for writing and querying data.
// It can be copied to multiple servers or the local datastore.
// Shards contains data from [startTime, endTime]
// Ids are unique across the cluster
type Shard interface {
	Id() uint32
	StartTime() time.Time
	EndTime() time.Time
	Write(*p.Request) error
	SyncWrite(req *p.Request, assignSeqNum bool) error
	Query(querySpec *parser.QuerySpec, response chan<- *p.Response)
	ReplicationFactor() int
	IsMicrosecondInRange(t int64) bool
}

type NewShardData struct {
	Id                uint32 `json:",omitempty"`
	SpaceName         string
	Database          string
	StartTime         time.Time
	EndTime           time.Time
	ServerIds         []uint32
	ReReplicServerIds []uint32
	ReReplicStartTime time.Time
	ReReplicEndTime   time.Time
	ReReplicState     int
}

const (
	RE_REPLIC_STATE_STARTED = iota
	RE_REPLIC_STATE_FINISHED
	RE_REPLIC_STATE_ERROR
)

type ShardType int

const (
	LONG_TERM ShardType = iota
	SHORT_TERM
)

type ShardData struct {
	id               uint32
	startTime        time.Time
	startMicro       int64
	endMicro         int64
	endTime          time.Time
	wal              WAL
	servers          []wal.Server
	clusterServers   []*ClusterServer
	store            LocalShardStore
	serverIds        []uint32
	shardDuration    time.Duration
	shardNanoseconds uint64
	localServerId    uint32
	IsLocal          bool
	SpaceName        string
	Database         string
}

func NewShard(id uint32, startTime, endTime time.Time, database, spaceName string, wal WAL) *ShardData {
	//return &ShardData{...}
}

const (
	PER_SERVER_BUFFER_SIZE  = 10
	LOCAL_WRITE_BUFFER_SIZE = 10
)

type LocalShardDb interface {
	Write(database string, series []*p.Series) error
	Query(*parser.QuerySpec, engine.Processor) error
	DropFields(fields []*metastore.Field) error
	IsClosed() bool
}

type LocalShardStore interface {
	Write(request *p.Request) error
	SetWriteBuffer(writeBuffer *WriteBuffer)
	BufferWrite(request *p.Request)
	GetOrCreateShard(id uint32) (LocalShardDb, error)
	ReturnShard(id uint32)
	DeleteShard(shardId uint32) error
}

func (self *ShardData) IsMicrosecondInRange(t int64) bool {
	// false > self.startMicro >= true > self.endMicro > False
}

func (self *ShardData) SetServers(servers []*ClusterServer) {
	//设置 self.servers 并且  self.sortServerIds()
}

func (self *ShardData) SetLocalStore(store LocalShardStore, localServerId uint32) error {
	// 把localServerId 添加到 self.serverIds中, 并排序
	// 设置 self.store, 并且检查是否可以打开shard文件(夹).
	// self.IsLocal = true
}

func (self *ShardData) DropFields(fields []*metastore.Field) error {
	if self.IsLocal {
		shard, _ := self.store.GetOrCreateShard(self.id)
		return shard.DropFields(fields)
	}
}

func (self *ShardData) SyncWrite(request *p.Request, assignSeqNum bool) error {
	if assignSeqNum {
		self.wal.AssignSequenceNumbers(request)
	}
	// 遍历所有的clusterServers 并调用 clusterServer.Write(request)
	// 最后执行 self.store.Write(request)
}

func (self *ShardData) Write(request *p.Request) error {
	// wal先分配requestNumber 并 记录requset
	// self.store.BufferWrite(request)
	// 然后遍历所有的  self.clusterServers:
	// 		server.BufferWrite(requestWithoutId)
}

func (self *ShardData) WriteLocalOnly(request *p.Request) error {
	self.store.Write(request)
}

func (self *ShardData) getProcessor(querySpec *parser.QuerySpec, processor engine.Processor) (engine.Processor, error) {
	// 根据query 创建并返回一个合适的 engine.Processor
	// return engine.NewPassthroughEngine(processor, 1), nil
	// processor, err = engine.NewQueryEngine(processor, query)
	// processor = engi ne.NewPassthroughEngine(processor, 1000)
	// processor = engine.NewPassthroughEngineWithLimit(processor, 1000, query.Limit)
	// processor = engine.NewFilteringEngine(query, processor)
}

func (self *ShardData) Query(querySpec *parser.QuerySpec, response chan<- *p.Response) {
	// if querySpec.RunAgainstAllServersInShard:
	// 		if querySpec.IsDeleteFromSeriesQuery():
	// 			self.logAndHandleDeleteQuery(querySpec, response)
	// 		else if querySpec.IsDropSeriesQuery():
	// 			self.logAndHandleDropSeriesQuery(querySpec, response)

	// 对于Query 数据这类型的:
	// 		如果 可以本地返回结果 就先直接本地query并返回结果
	// 		从shard的serverid中随机挑选一个isUp的server, 发起请求并返回结果
	// 		如果没有找到可以发起请求的server, 返回错误message:  "No servers up to query shard id"
}

// Returns a random healthy server or nil if none currently exist
func (self *ShardData) randomHealthyServer() *ClusterServer {
	// 返回随机的 isUp == true 的 clusterServer
}

func (self *ShardData) String() string {
	// 相当于 python  __str__ 返回 instance 的 字符串描述
	// "[ID: %d, START: %d, END: %d, LOCAL: %s, SERVERS: [%s]]"
}

func (self *ShardData) ShouldAggregateLocally(querySpec *parser.QuerySpec) bool {
	// Returns true if we can aggregate the data locally per shard,
	// i.e. the group by interval lines up with the shard duration and
	// there are no joins or merges
}

type Shards []*ShardData

func (shards Shards) ShouldAggregateLocally(querySpec *parser.QuerySpec) bool {
	// Return true iff we can aggregate locally on all the given shards,
	// false otherwise
}

func (self *ShardData) QueryResponseBufferSize(querySpec *parser.QuerySpec, batchPointSize int) int {
	// return 1000 or tickCount * 100
}

func (self *ShardData) logAndHandleDeleteQuery(querySpec *parser.QuerySpec, response chan<- *p.Response) {
	queryString := querySpec.GetQueryStringWithTimeCondition()
	request := self.createRequest(querySpec)
	request.Query = &queryString
	self.LogAndHandleDestructiveQuery(querySpec, request, response, false)
}

func (self *ShardData) logAndHandleDropSeriesQuery(querySpec *parser.QuerySpec, response chan<- *p.Response) {
	self.LogAndHandleDestructiveQuery(querySpec, self.createRequest(querySpec), response, false)
}

func (self *ShardData) LogAndHandleDestructiveQuery(querySpec *parser.QuerySpec, request *p.Request, response chan<- *p.Response, runLocalOnly bool) {
	self.HandleDestructiveQuery(querySpec, request, response, runLocalOnly)
}

func (self *ShardData) deleteDataLocally(querySpec *parser.QuerySpec) error {
	shard, err := self.store.GetOrCreateShard(self.id)
	return shard.Query(querySpec, NilProcessor{})
}

func (self *ShardData) forwardRequest(request *p.Request) ([]<-chan *p.Response, []uint32, error) {
	// 遍历 self.clusterServers:
	//		server.MakeRequest
	// 返回 responseChannels, serverIds, nil
}

func (self *ShardData) HandleDestructiveQuery(querySpec *parser.QuerySpec, request *p.Request, response chan<- *p.Response, runLocalOnly bool) {
	// if self.IsLocal -> self.deleteDataLocally(querySpec)
	// if !runLocalOnly -> self.forwardRequest(request) 并等待处理结果
}

func (self *ShardData) createRequest(querySpec *parser.QuerySpec) *p.Request {
	// return &p.Request{...}
}

// used to serialize shards when sending around in raft or when snapshotting in the log
func (self *ShardData) ToNewShardData() *NewShardData {
	//return &NewShardData{...}
}

// server ids should always be returned in sorted order
func (self *ShardData) sortServerIds() {
	sort.Ints(serverIdInts) //increasing order.
}.

func SortShardsByTimeAscending(shards []*ShardData) {
	sort.Sort(ByShardTimeAsc{shards})
}

func SortShardsByTimeDescending(shards []*ShardData) {
	sort.Sort(ByShardTimeDesc{shards})
}

type ShardCollection []*ShardData

func (s ShardCollection) Len() int      { return len(s) }
func (s ShardCollection) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

type ByShardTimeDesc struct{ ShardCollection }
type ByShardTimeAsc struct{ ShardCollection }

func (s ByShardTimeAsc) Less(i, j int) bool {
	if iStartTime == jStartTime {
		return s.ShardCollection[i].Id() < s.ShardCollection[j].Id()
	}
	return iStartTime < jStartTime
}
func (s ByShardTimeDesc) Less(i, j int) bool {
	if iStartTime == jStartTime {
		return s.ShardCollection[i].Id() < s.ShardCollection[j].Id()
	}
	return iStartTime > jStartTime
}
