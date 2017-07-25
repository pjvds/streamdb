package storage

import "github.com/cespare/xxhash"

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

type PagePosition int64
