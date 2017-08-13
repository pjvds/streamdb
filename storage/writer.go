package storage

import (
	"errors"
	"io"
	"os"
	"path"
	"strconv"
	"sync/atomic"
	"sync"
	"go.uber.org/zap"
)

var (
	ErrNotADir     = errors.New("not a directory")
	ErrDirNotEmpty = errors.New("directory not empty")
	ErrClosed = errors.New("closed")
	ErrBufferOverflow = errors.New("buffer overflow")
	ErrBufferTooSmall = errors.New("buffer too small")
	ErrReadTooShort = errors.New("read too short")
)

type Store interface {
	Append(id StreamID, payload SinglePayload) (LogOffset, error)
}

func OpenLogStream(log *zap.Logger, directory string) (*LogStream, error) {
	stat, err := os.Stat(directory)
	if err != nil {
		return nil, err
	}

	if !stat.IsDir() {
		return nil, ErrNotADir
	}

	handle, err := os.Open(directory)
	if err != nil {
		return nil, err
	}
	defer handle.Close()

	_, err = handle.Readdirnames(1)
	if err == nil {
		return nil, ErrDirNotEmpty
	}
	if err != io.EOF {
		return nil, err
	}

	logStream := &LogStream{
		log: log,
		offset: 1,
		directory: directory,
		pageSize:  5e8, // half GB
	}

	if err := logStream.rotate(); err != nil {
		return nil, err
	}

	return logStream, nil
}

type LogStream struct {
	log *zap.Logger

	offset   int64
	pages    []*LogPage
	tailPage *LogPage

	directory string
	pageSize  int64

	closed bool
	lock sync.RWMutex
}

func (this *LogStream) Close() error{
	this.lock.Lock()
	defer this.lock.Unlock()

	if (this.closed) {
		return ErrClosed
	}

	for _, page := range this.pages {
		page.Close()
	}

	this.closed = true
	return nil
}

func (this *LogStream) Append(payload Payload) (LogOffset, error) {
	this.lock.Lock()
	defer this.lock.Unlock()

	if this.closed {
		this.log.Debug("append failed", zap.Error(ErrClosed))
		return LogOffset{}, ErrClosed
	}

	if !this.tailPage.SpaceLeftFor(payload) {
		this.log.Debug("tail page full",
			zap.Int("payload size", payload.SizeOnDisk()),
			zap.Int64("page size", this.tailPage.size))

		if err := this.rotate(); err != nil {
			return LogOffset{}, err
		}
	}

	location, err := this.tailPage.Append(payload)
	if err != nil {
		return LogOffset{}, err
	}

	offset := this.offset
	this.offset = offset + int64(payload.EntryCount())

	page := int32(len(this.pages))

	result := LogOffset{Offset: offset, Page: page, Location: location }

	this.log.Debug("append success", zap.Stringer("result", result))
	return result, nil
}

func (this *LogStream) Read(offset LogOffset, buffer []byte) (int, error) {
	this.lock.RLock()
	defer this.lock.RUnlock()

	page := this.pages[offset.Page-1]
	return page.Read(offset.Location, buffer)
}

func (this *LogStream) Sync() (LogOffset, error) {
	this.lock.RLock()
	defer this.lock.RUnlock()

	offset := this.offset
	page := int32(len(this.pages))

	position, err := this.tailPage.Sync()

	if err != nil {
		return LogOffset{}, err
	}

	return LogOffset{
		Offset:	offset,
		Page: page,
		Location: position,
	}, nil
}


type LogPage struct {
	file     *os.File
	position int64
	size     int64
	closed 	 bool
}


func (this *LogPage) Read(location int64, buffer []byte) (int, error) {
	return this.file.ReadAt(buffer, location)
}

func (this *LogPage) getPosition() int64 {
	return atomic.LoadInt64(&this.position)
}

func (this *LogPage) incrementPosition(delta int64) {
	atomic.AddInt64(&this.position, delta)
}

func (this *LogPage) Close() error {
	if this.closed {
		return ErrClosed
	}

	if err := this.file.Close(); err != nil {
		return err
	}

	this.closed = true
	return nil
}

func (this *LogPage) finalize() error {
	// TODO: close file and open readonly
	return nil
}

var ErrPageFull = errors.New("no space left in page")

func (this *LogStream) rotate() error {
	if this.tailPage != nil {
		if err := this.tailPage.finalize(); err != nil {
			return err
		}
	}

	number := len(this.pages) + 1
	filename := path.Join(this.directory, strconv.Itoa(number)+".page")
	file, err := os.Create(filename)
	if err != nil {
		return err
	}

	if err := file.Truncate(this.pageSize); err != nil {
		file.Close()
		return err
	}

	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}

	page := &LogPage{
		file: file,
		size: this.pageSize,
	}
	this.pages = append(this.pages, page)
	this.tailPage = page

	return nil
}

// Sync commits the current contents of the log page file to stable storage.
// It returns the position of the last known write before this commit.
func (this *LogPage) Sync() (int64, error) {
	position := this.getPosition()

	if err := this.file.Sync(); err != nil {
		return 0, err
	}

	return position, nil
}

func (this *LogPage) SpaceLeftFor(payload Payload) bool {
	return this.getPosition() + payload.SizeOnDisk64() < this.size
}

func (this *LogPage) Append(payload Payload) (int64, error) {
	if this.closed {
		return 0, ErrClosed
	}

	// check if we have enough space left in this page
	if !this.SpaceLeftFor(payload)  {
		return 0, ErrPageFull
	}

	position := this.getPosition()

	// write header
	if n, err := this.file.WriteAt(payload.ToBytes(), position); err != nil {
		return 0, err
	} else {
		this.incrementPosition(int64(n))
	}

	return position, nil
}
