package storage

import (
	"encoding/binary"
)

const HEADER_SIZE = (
	8 + // MAGIC
	64 + // OFFSET
	32 + // PAGE
	64 + // LOCATION
	32 + // LENGHT
	64) / 8 // HASH

const MAGIC_POSITION = 0
const OFFSET_POSITION = 1
const PAGE_POSITION = OFFSET_POSITION + 8
const LOCATION_POSITION = PAGE_POSITION + 4
const LENGTH_POSITION = LOCATION_POSITION + 8
const HASH_POSITION = LENGTH_POSITION + 8

const MAGIC_START = 42
const MAGIC_EOF = 43

type Header struct {
	Magic byte
	Offset LogOffset
	Length int32
	Hash   uint64
}

func newHeader(offset LogOffset, payload SinglePayload) Header {
	return Header{
		Magic: MAGIC_START,
		Offset: offset,
		Length: payload.Len32(),
		Hash:   payload.Hash(),
	}
}

func ReadHeader(buffer []byte) Header {
	if len(buffer) < HEADER_SIZE {
		panic("buffer too small")
	}

	return Header{
		Magic: buffer[MAGIC_POSITION],
		Offset: LogOffset{
			Offset: int64(binary.BigEndian.Uint64(buffer[OFFSET_POSITION:])),
			Page: int32(binary.BigEndian.Uint32(buffer[PAGE_POSITION:])),
			Location: int64(binary.BigEndian.Uint64(buffer[LOCATION_POSITION:])),
		},
		Length: int32(binary.BigEndian.Uint32(buffer[LENGTH_POSITION:])),
		Hash: binary.BigEndian.Uint64(buffer[HASH_POSITION:]),
	}
}

func (this *Header) ToBytes() []byte {
	b := make([]byte, HEADER_SIZE)
	b[MAGIC_POSITION] = this.Magic
	binary.BigEndian.PutUint64(b[OFFSET_POSITION:], uint64(this.Offset.Offset))
	binary.BigEndian.PutUint32(b[PAGE_POSITION:], uint32(this.Offset.Page))
	binary.BigEndian.PutUint64(b[LOCATION_POSITION:], uint64(this.Offset.Location))
	binary.BigEndian.PutUint32(b[LENGTH_POSITION:], uint32(this.Length))
	binary.BigEndian.PutUint64(b[HASH_POSITION:], this.Hash)

	return b
}
