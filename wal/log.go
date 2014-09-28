package wal

import (
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
	"strings"

	"code.google.com/p/goprotobuf/proto"
	logger "code.google.com/p/log4go"
	"github.com/influxdb/influxdb/configuration"
	"github.com/influxdb/influxdb/protocol"
)

type log struct {
	closed                 bool
	fileSize               uint64
	file                   *os.File
	requestsSinceLastFlush int
	config                 *configuration.Configuration
	cachedSuffix           uint32
}

func newLog(file *os.File, config *configuration.Configuration) (*log, error) {
	// 新的log文件并检查
}

func (self *log) check() error {
	// 检查逻辑, 就是全部entry遍历一边. 如果遍历过程中有错误就truncate文件
}

func (self *log) offset() int64 {
	// 得到文件当前的offset
}

func (self *log) suffix() uint32 {
	return self.cachedSuffix
}

// this is for testing only
func (self *log) syncFile() error {
	return self.file.Sync()
}

func (self *log) close() error {
	return self.file.Close()
}

func (self *log) delete() error {
	return os.Remove(self.file.Name())
}

func (self *log) appendRequest(request *protocol.Request, shardId uint32) error {
	// 1. 写入 headerEntry
	// 2. 写入 request 串化后的Entry
}

func (self *log) dupLogFile() (*os.File, error) {
	return os.OpenFile(self.file.Name(), os.O_RDWR, 0)
}

func (self *log) dupAndReplayFromOffset(shardIds []uint32, offset int64, rn uint32) (chan *replayRequest, chan struct{}) {
	// 开启异步线程 replay
	stopChan := make(chan struct{}, 1)
	replayChan := make(chan *replayRequest, 10)

	go func() {
		file, err := self.dupLogFile()
		self.replayFromFileLocation(file, shardIdsSet, replayChan, stopChan)
	}()
	return replayChan, stopChan
}

func (self *log) getNextHeader(file *os.File) (int, *entryHeader, error) {
	hdr := &entryHeader{}
	numberOfBytes, err := hdr.Read(file)
	return numberOfBytes, hdr, err
}

func (self *log) skip(file *os.File, offset int64, rn uint32) error {
	// skip掉一部分已经处理的
	// skipRequest
}

func (self *log) skipRequest(file *os.File, hdr *entryHeader) (err error) {
	_, err = file.Seek(int64(hdr.length), os.SEEK_CUR)
	return
}

func (self *log) skipToRequest(file *os.File, requestNumber uint32) error {
	// skip到某个requestNumber
}

func (self *log) replayFromFileLocation(file *os.File,
	for {
		// 读取request数据
		// 创建 protocol.Request对象
		req := &protocol.Request{}
		replayRequest := &replayRequest{hdr.requestNumber, req, hdr.shardId, offset, offset + int64(numberOfBytes) + int64(hdr.length), nil}
		// 发送request 或 这 停止
		if sendOrStop(replayRequest, replayChan, stopChan) {
			return
		}
		// 增加 offset
	}
}

func sendOrStop(req *replayRequest, replayChan chan *replayRequest, stopChan chan struct{}) bool {
	// 发送 request 到 replayChan
	// 或者 stop
}
