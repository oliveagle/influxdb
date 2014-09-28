package wal

import (
	"encoding/binary"
	"io"
)

type entryHeader struct {
	requestNumber uint32
	shardId       uint32
	length        uint32
}

func (self *entryHeader) Write(w io.Writer) (int, error) {
	// 写entryHeader中的数据到 io.Writer中.
	// 用binary.Write(w, binary.BigEndian, (requestNumber, shardId, length))
	// 返回 4+4+4, nil
}

func (self *entryHeader) Read(r io.Reader) (int, error) {
	// binary.Read(r, binary.BigEndian,(&self.requestNumber, &self.shardId, &self.length))
	// 返回 4+4+4, nil
}
