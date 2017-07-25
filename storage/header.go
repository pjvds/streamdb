package storage

import (
	"encoding/binary"
)

const HEADER_SIZE = (
	8 + // MAGIC
	32 + // LENGHT
	64) / 8 // HASH
const MAGIC_START = 42
const MAGIC_EOF = 43

type Header struct {
	Magic byte
	Length int32
	Hash   uint64
}

func newHeader(payload Payload) Header {
	return Header{
		Magic: MAGIC_START,
		Length: payload.Len32(),
		Hash:   payload.Hash(),
	}
}

func ReadHeader(buffer []byte) Header {
	if len(buffer) < HEADER_SIZE {
		panic("buffer too small")
	}

	return Header{
		Magic: buffer[0],
		Length: int32(binary.BigEndian.Uint32(buffer[1:])),
		Hash: binary.BigEndian.Uint64(buffer[5:]),
	}
}

func (this *Header) ToBytes() []byte {
	b := make([]byte, 13)
	b[0] = this.Magic
	binary.BigEndian.PutUint32(b[1:], uint32(this.Length))
	binary.BigEndian.PutUint64(b[5:], this.Hash)

	return b
}
