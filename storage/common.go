package storage

type StreamID string
type Payload []byte

type StreamOffset int64
type PagePosition int64

func (this StreamOffset) Next() StreamOffset {
	return this + 1
}
