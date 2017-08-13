package storage

import (
	"github.com/cespare/xxhash"
	"fmt"
	"bytes"
	"regexp"
	"github.com/pkg/errors"
	"strconv"
)

type StreamID string
type SinglePayload []byte

type HeaderAndOffset struct {
	Header
	Offset int64
}

type PayloadSet struct{
	headers []HeaderAndOffset
	buffer bytes.Buffer
}

func NewPayloadSet() *PayloadSet{
	return &PayloadSet{
		headers: make([]HeaderAndOffset, 0),
	}
}

func (this *PayloadSet) Append(payload SinglePayload) int {
	header := HeaderAndOffset{
		Header: newHeader(EmptyLogOffset, payload),
		Offset: int64(this.buffer.Len()),
	}

	this.buffer.Grow(payload.SizeOnDisk())
	this.buffer.Write(header.ToBytes())
	this.buffer.Write(payload)

	this.headers = append(this.headers, header)

	return len(this.headers)-1
}

func (this PayloadSet) Reset() {
	this.headers = this.headers[0:0]
	this.buffer.Reset()
}

func (this PayloadSet) SizeOnDisk() int{
	return this.buffer.Len()
}

func (this PayloadSet) SizeOnDisk64() int64{
	return int64(this.SizeOnDisk())
}

func (this SinglePayload) Len() int {
	return len(this)
}

func (this SinglePayload) Len32() int32 {
	return int32(this.Len())
}

func (this PayloadSet) 	ToBytes() []byte {
	return this.buffer.Bytes()
}


func (this SinglePayload) Len64() int64 {
	return int64(this.Len())
}

func (this SinglePayload) ToBytes(offset LogOffset) []byte {
	buffer := make([]byte, this.SizeOnDisk())

	header := newHeader(offset, this)
	copy(buffer, header.ToBytes())
	copy(buffer[HEADER_SIZE:], this)

	return buffer
}


func (this SinglePayload) SizeOnDisk() int {
	return HEADER_SIZE + this.Len()
}

func (this SinglePayload) SizeOnDisk64() int64 {
	return HEADER_SIZE + this.Len64()
}

type Payload interface {
	EntryCount() int
	ToBytes() []byte
	SizeOnDisk() int
	SizeOnDisk64() int64
}

func (this PayloadSet) EntryCount() int {
	return len(this.headers)
}

func (this SinglePayload) EntryCount() int {
	return 1
}

func (this SinglePayload) Hash() uint64 {
	return xxhash.Sum64(this)
}

var EmptyLogOffset = LogOffset{}

type LogOffset struct{
	Offset int64
	Page int32
	Location int64
}

func ParseLogOffset(value string) (LogOffset, error) {
	r := regexp.MustCompile("(\\d+):(\\d+)\\/(\\d+)")
	matches := r.FindAllString(value, 3)

	if len(matches) != 3 {
		return LogOffset{}, errors.Errorf("invalid log offset string: %v", value)
	}

	offset, err := strconv.ParseInt(matches[0], 32, 10)
	if err != nil {
		return LogOffset{}, errors.Errorf("invalid offset, %v: %v", err, value)
	}

	page, err  := strconv.ParseInt(matches[1], 32, 10)
	if err != nil {
		return LogOffset{}, errors.Errorf("invalid page, %v: %v", err, value)
	}

	location, err := strconv.ParseInt(matches[2], 64, 10)
	if err != nil {
		return LogOffset{}, errors.Errorf("invalid location, %v: %v", err, value)
	}

	return LogOffset{
		Offset: offset,
		Page: int32(page),
		Location: location,
	}, nil
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
