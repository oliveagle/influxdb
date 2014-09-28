package wal

import (
	"fmt"
	"math"
	"os"
	"path"
	"sort"
	"strings"

	"code.google.com/p/goprotobuf/proto"
	logger "code.google.com/p/log4go"
	"github.com/influxdb/influxdb/configuration"
	"github.com/influxdb/influxdb/protocol"
)

type WAL struct {
	state             *GlobalState
	config            *configuration.Configuration
	logFiles          []*log
	logIndex          []*index
	serverId          uint32
	nextLogFileSuffix uint32
	entries           chan interface{}

	// counters to force index creation, bookmark and flushing
	requestsSinceLastFlush    int
	requestsSinceLastBookmark int
	requestsSinceLastIndex    int
	requestsSinceRotation     int
}

const HOST_ID_OFFSET = uint64(10000)

func NewWAL(config *configuration.Configuration) (*WAL, error) {
	// WAL dir 下
	// 1. newGlobalState	文件名:	bookmark
	// 2. 遍历WAL dir下所有的log.xxx文件
	//		加入到  wal.logFiles 以及 wal.logIndex
	// 3. go wal.processEntries()
}

func (self *WAL) SetServerId(id uint32) {
	self.serverId = id
	if err := self.recover(); err != nil {
		panic(err)
	}
}

func (self *WAL) Commit(requestNumber uint32, serverId uint32) error {
	// 添加 commitEntry 并等待 处理结果
}

func (self *WAL) RecoverServerFromLastCommit(serverId uint32, shardIds []uint32, yield func(request *protocol.Request, shardId uint32) error) error {
	return self.RecoverServerFromRequestNumber(requestNumber, shardIds, yield)
}

func (self *WAL) isInRange(requestNumber uint32) bool {
	// 判断requestNubmer是否在 GlobalState 中记录的范围内
}

func (self *WAL) RecoverServerFromRequestNumber(requestNumber uint32, shardIds []uint32, yield func(request *protocol.Request, shardId uint32) error) error {
	// 遍历 并 replay 从 RequestNumber开始的logfile.
	// yield用来最终对replay的request进行处理
	for idx := firstIndex; idx < len(logFiles); idx++ {
		replayChannel, stopChan := logFile.dupAndReplayFromOffset(shardIds, firstOffset, requestNumber)
		for {
			x := <-replayChannel
			if err := yield(x.request, x.shardId); err != nil {
				stopChan <- struct{}{}
				return err
			}
		}
	}
}

func (self *WAL) Close() error {
	return self.closeCommon(true)
}

func (self *WAL) closeWithoutBookmarking() error {
	return self.closeCommon(false)
}

func (self *WAL) closeCommon(shouldBookmark bool) error {
	// 添加closeEntry 并等待处理结果
}

func (self *WAL) processClose(shouldBookmark bool) error {
	// 关闭所有的logfile
	// bookmark if needed
}

func (self *WAL) processEntries() {
	// loop 处理对应的 Entry, 如果传入没有定义的entry就panic
	for {
		e := <-self.entries
		switch x := e.(type) {
		case *commitEntry:
			self.processCommitEntry(x)
		case *appendEntry:
			self.processAppendEntry(x)
		case *bookmarkEntry:
			err := self.bookmark()
		case *closeEntry:
			x.confirmation <- &confirmation{0, self.processClose(x.shouldBookmark)}
			return
		default:
			panic(fmt.Errorf("unknown entry type %T", e))
		}
	}
}

func (self *WAL) assignSequenceNumbers(shardId uint32, request *protocol.Request) {
	// 给series中的每个datapoint 分配一个单调递增的数字(SequenceNumber)
	// 并在GlobalState中保存最后一个值
}

func (self *WAL) processAppendEntry(e *appendEntry) {
	// 得到一个单调递增的 RequestNumber
	// 为每个DataPoint 指定一个 单调递增的 SequenceNumber
	// append 到 最后一个logfile 并更新几个标签值
	// rotateTheLogFile
	// self.conditionalBookmarkAndIndex
	//           到某个值开始 index
	//           到某个值开始 bookmark
	//           到某个值开始 flush
}

func (self *WAL) processCommitEntry(e *commitEntry) {
	// 更改global state中的commit的信息, 删除没有用的log
	// self.state.commitRequestNumber(e.serverId, e.requestNumber)
}

// creates a new log file using the next suffix and initializes its
// state with the state of the last log file
func (self *WAL) createNewLog(firstRequestNumber uint32) (*log, error) {
	// 创建一个 单调增加的(文件名) log对象
}

func (self *WAL) openLog(logFileName string) (*log, *index, error) {
	// 打开log文件, 用这个文件创建Log对象, 再用log对象创建index对象, 最后添加到self.logIndex中
}

// Will assign sequence numbers if null. Returns a unique id that
// should be marked as committed for each server as it gets confirmed.
func (self *WAL) AssignSequenceNumbersAndLog(request *protocol.Request, shard Shard) (uint32, error) {
	// 往self.entries 中添加一条 assignSeqOnly=false 的 appendEntry, 并返回 confirmation.requestNumber, confirmation.err
}

// Assigns sequence numbers if null.
func (self *WAL) AssignSequenceNumbers(request *protocol.Request) error {
	// 往self.entries 中添加一条 assignSeqOnly=true 的 appendEntry
}

// returns the first log file that contains the given request number
func (self *WAL) firstLogFile() int {
	// 返回第一个包含 self.state.ServerLastRequestNumber 的 log文件, 并返回 其在slice中的序号
}

func (self *WAL) shouldRotateTheLogFile() bool {
	return self.requestsSinceRotation >= self.config.WalRequestsPerLogFile
}

func (self *WAL) recover() error {
	for idx, logFile := range self.logFiles {
		// 遍历所有的log文件,
		// replay log文件
		replay, _ := logFile.dupAndReplayFromOffset(nil, lastOffset, 0)
		for {
			replayRequest := <-replay

			//更新 global state 中几个需要的值:
			// 	LargetRequestNumber, shardId, sequenceNumber

			// 添加 index 记录
			// self.logIndex[idx].addEntry(...)
		}
	}
}

func (self *WAL) rotateTheLogFile(nextRequestNumber uint32) (bool, error) {
	// rotate log
}

func (self *WAL) conditionalBookmarkAndIndex() {
	// 按照配置文件中规定的几种threshold来决定何时做 index, bookmark 和 flush操作
}

func (self *WAL) flush() error {
	//Fsyncing the log file to disk"
}

func (self *WAL) CreateCheckpoint() error {
	// 往 self.entries 中添加一个bookmarkEntry
}

func (self *WAL) bookmark() error {
	// 把 Global State 保存到本地文件
}

func (self *WAL) index() error {
	// 添加当前的global state到index中.
	// index 中记录的是:

	//     Version
	// FirstRequestNumber, FirstOffset, LastRequestNumber, LastOffset\n
	// FirstRequestNumber, FirstOffset, LastRequestNumber, LastOffset\n
}
