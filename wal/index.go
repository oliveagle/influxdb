package wal

import (
	logger "code.google.com/p/log4go"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
)

type indexEntry struct {
	FirstRequestNumber uint32 // first request number in the block
	LastRequestNumber  uint32 // number of requests in the block
	FirstOffset        int64  // the offset of the first request
	LastOffset         int64  // the offset of the last request
}

func (self *indexEntry) NumberOfRequests() int {
	return int(self.LastRequestNumber-self.FirstRequestNumber) + 1
}

type index struct {
	f       *os.File
	Entries []*indexEntry
}

func newIndex(path string) (*index, error) {
	/*
	   Version\n
	   FirstRequestNumber, FirstOffset, LastRequestNumber, LastOffset\n
	   FirstRequestNumber, FirstOffset, LastRequestNumber, LastOffset\n

	   读取这样的一个文件,解析并返回一个index对象
	*/
}

func (self *index) addEntry(firstRequestNumber, lastRequestNumber uint32, firstOffset, lastOffset int64) {
	// 写入 FirstRequestNumber, FirstOffset, LastRequestNumber, LastOffset\n  到文件
	// 同时 添加这个entry到 self.Entries
}

func (self *index) syncFile() error {
	return self.f.Sync()
}

func (self *index) close() error {
	return self.f.Close()
}

func (self *index) delete() error {
	return os.Remove(self.f.Name())
}

func (self *index) getLastRequestInfo() (uint32, int64) {
	//  return lastEntry.LastRequestNumber, lastEntry.LastOffset
}

func (self *index) getLastOffset() int64 {
	// return lastEntry.LastOffset
}

func (self *index) getLength() int {
	// return int(lastRequestNumber - firstRequestNumber)
}

func (self *index) getFirstRequestInfo() (uint32, int64) {
	// return firstEntry.FirstRequestNumber, firstEntry.FirstOffset
}

func (self *index) blockSearch(requestNumber uint32) func(int) bool {
	//	return requestNumber <= self.Entries[i].LastRequestNumber
}

func (self *index) requestOffset(requestNumber uint32) int64 {
	// 异常情况返回  -1
	// 否则: 返回 最小的一个index的FirstOffset
}

func (self *index) requestOrLastOffset(requestNumber uint32) int64 {
	// 前一个index的LastOffset, 或者本index的FirstOffset, 或者 0
}
