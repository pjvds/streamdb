package storage

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type StorageManager interface {
	Append(id StreamID, payload Payload) (StreamOffset, error)
}

func OpenStorageDirectory(directory string) (StorageManager, error) {
	stat, err := os.Stat(directory)
	if err != nil {
		return nil, err
	}

	if !stat.IsDir() {
		return nil, fmt.Errorf("%v is not a directory", directory)
	}

	return &FileStorageManager{
		directory: directory,
		streams:   make(map[StreamID]*Stream),
	}, nil
}

type PayloadAppender interface {
	Append(payload Payload) (StreamOffset, error)
}

type StreamAppender interface {
	Append(id StreamID, payload Payload) (StreamOffset, error)
}

type FileStorageManager struct {
	directory string

	streamsLock sync.RWMutex
	streams     map[StreamID]*Stream
}

func (this *FileStorageManager) Append(id StreamID, payload Payload) (StreamOffset, error) {
	stream, err := this.getOrCreateStream(id)
	if err != nil {
		return StreamOffset(0), err
	}

	return stream.Append(payload)
}

func (this *FileStorageManager) getOrCreateStream(id StreamID) (*Stream, error) {
	this.streamsLock.RLock()
	stream, ok := this.streams[id]
	this.streamsLock.RUnlock()

	if ok {
		return stream, nil
	}

	return this.createStreamAndAddToMap(id)
}

func (this *FileStorageManager) createStreamAndAddToMap(id StreamID) (*Stream, error) {
	this.streamsLock.Lock()
	defer this.streamsLock.Unlock()

	stream, ok := this.streams[id]
	if ok {
		return stream, nil
	}

	file, err := os.Create(filepath.Join(this.directory, string(id)))
	if err != nil {
		return nil, err
	}

	stream = &Stream{
		id: id,
		page: &StreamPage{
			file: file,
		},
	}

	this.streams[id] = stream
	return stream, nil
}

type Stream struct {
	id     StreamID
	offset StreamOffset
	page   *StreamPage
	file   *os.File
}

func (this *Stream) Append(payload Payload) (StreamOffset, error) {
	_, err := this.page.Append(payload)
	if err != nil {
		return StreamOffset(0), err
	}

	writtenAt := this.offset
	this.offset = this.offset.Next()
	return writtenAt, nil
}

type StreamPage struct {
	file      *os.File
	byteorder binary.ByteOrder
	position  int64
}

func (this *StreamPage) Append(payload Payload) (PagePosition, error) {
	written, err := this.file.WriteAt(payload, this.position)
	if err != nil {
		return PagePosition(0), err
	}

	writtenAt := this.position
	this.position += int64(written)

	return PagePosition(writtenAt), nil
}
