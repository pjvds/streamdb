package storage

import (
	"github.com/cespare/xxhash"
	"fmt"
)

type StreamID string
type Payload []byte

func (this Payload) Len() int {
	return len(this)
}

func (this Payload) Len32() int32 {
	return int32(this.Len())
}

func (this Payload) Len64() int64 {
	return int64(this.Len())
}

func (this Payload) SizeOnDisk() int {
	return HEADER_SIZE + this.Len()
}

func (this Payload) SizeOnDisk64() int64 {
	return HEADER_SIZE + this.Len64()
}

func (this Payload) Hash() uint64 {
	return xxhash.Sum64(this)
}

type LogOffset struct{
	Offset int32
	Page int32
	Location int64
}

func (this LogOffset) After(that LogOffset) bool {
	return this.Location > that.Location
}

func (this LogOffset) Before(that LogOffset) bool {
	return this.Location < that.Location
}

func (this LogOffset) String() string {
	return fmt.Sprintf("%v:%v/%v", this.Offset, this.Page,this.Location)
}

type PagePosition int64
