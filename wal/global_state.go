package wal

import (
	"bufio"
	"encoding/gob"
	"fmt"
	"io"
	"math"
	"os"
)

type GlobalState struct {
	// used for creating index entries
	CurrentFileSuffix uint32
	CurrentFileOffset int64

	// keep track of the next request number
	LargestRequestNumber uint32

	// used for rollover
	FirstSuffix uint32

	// last seq number used
	ShardLastSequenceNumber map[uint32]uint64

	// committed request number per server
	ServerLastRequestNumber map[uint32]uint32

	// path to the state file
	path string
}

// from version 0.7 to 0.8 the Suffix variables changed from
// ints to uint32s. We need this struct to convert them.
type oldGlobalState struct {
	GlobalState
	CurrentFileSuffix int
	FirstSuffix       int
}

func newGlobalState(path string) (*GlobalState, error) {
	//创建新的GlobalState对象, 校验
}

func (self *GlobalState) writeToFile() error {
	// 先写到 .new的文件
	// 删除 原先的文件
	// 再重命名 .new的文件成原先的文件名
}

func (self *GlobalState) write(w io.Writer) error {
	// 写 版本号, encode自身并保存
}

func (self *GlobalState) read(r *os.File) error {
	// 从文件中读取各种保存的数据
}

func (self *GlobalState) recover(shardId uint32, sequenceNumber uint64) {
	lastSequenceNumber := self.ShardLastSequenceNumber[shardId]

	if sequenceNumber > lastSequenceNumber {
		self.ShardLastSequenceNumber[shardId] = sequenceNumber
	}
}

func (self *GlobalState) getNextRequestNumber() uint32 {
	self.LargestRequestNumber++
	return self.LargestRequestNumber
}

func (self *GlobalState) getCurrentSequenceNumber(shardId uint32) uint64 {
	return self.ShardLastSequenceNumber[shardId]
}

func (self *GlobalState) setCurrentSequenceNumber(shardId uint32, sequenceNumber uint64) {
	self.ShardLastSequenceNumber[shardId] = sequenceNumber
}

func (self *GlobalState) commitRequestNumber(serverId, requestNumber uint32) {
	self.ServerLastRequestNumber[serverId] = requestNumber
}

func (self *GlobalState) LowestCommitedRequestNumber() uint32 {
	// return  min(self.ServerLastRequestNumber)
}
