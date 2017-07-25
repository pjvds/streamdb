package controller

import (
	"github.com/pjvds/streamdb/storage"
	"time"
	"go.uber.org/zap"
	"errors"
	"context"
)

var ErrTimeout = errors.New("timeout")
var ErrClosed = errors.New("closed")

func NewStreamController(log *zap.SugaredLogger, syncMonitor *SyncMonitor, stream *storage.LogStream) *StreamController{
	return &StreamController{
		syncMonitor: syncMonitor,
		stream: stream,
	}
}

type StreamController struct{
	syncMonitor *SyncMonitor
	stream *storage.LogStream
}

type SyncMonitor struct{
	log *zap.Logger
	requests chan syncMonitorEntry

	stream *storage.LogStream

	closed chan struct{}
	closing chan struct{}
}

func newSyncMonitor(log *zap.Logger, stream *storage.LogStream) *SyncMonitor{
	monitor := &SyncMonitor{
		log: log,
		requests: make(chan syncMonitorEntry),
		stream: stream,
		closing: make(chan struct{}),
		closed: make(chan struct{}),
	}
	go monitor.do()

	return monitor
}

type syncMonitorEntry struct{
	offset int32
	synced chan struct{}
}

func (this *SyncMonitor) do() {
	timer := time.NewTimer(50 * time.Millisecond)

	defer func() {
		timer.Stop()
		close(this.closed)

		this.log.Debug("sync monitor stopped")
	}()


	offset, err := this.stream.Sync()
	if err != nil {
		this.log.Error("sync failed", zap.Error(err))
	}

	outstanding := make([]syncMonitorEntry, 0)

	for {
		select {
		case <-timer.C:
				offset, err = this.stream.Sync()
				if err != nil {
					this.log.Error("sync failed", zap.Error(err))
					continue
				}

			this.log.Debug("sync success", zap.Stringer("offset", offset))

			if len(outstanding) == 0 {
				continue
			}

			i := 0
			for _, request := range outstanding {
				if request.offset <= offset.Offset {
					request.synced <- struct{}{}
				}

				outstanding[i] = request
				i++
			}
			outstanding = outstanding[0:i+1]
			this.log.Debug("signalled sync", zap.Int("signalled", i+1), zap.Int("outstanding", len(outstanding)))

		case request := <- this.requests:
				if request.offset <= offset.Offset {
					if request.offset <= offset.Offset {
						request.synced <- struct{}{}
					}
				}

				outstanding = append(outstanding, request)

		case <-this.closing:
			return
		}
	}
}

func (this *SyncMonitor) Close() {
	select{
		case this.closing <- struct{}{}:
			<-this.closed
		case <-this.closed:
	}
}

func (this *SyncMonitor) waitForSync(ctx context.Context, offset int32) error {
	request := syncMonitorEntry{
		offset: offset,
		synced: make(chan struct{}, 1),
	}

	select{
		case this.requests <- request:
		case <-ctx.Done():
			return ctx.Err()
	}

	select{
	case <-request.synced:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (this *StreamController) append(ctx context.Context, request *AppendRequest) (*AppendReply, error) {
	offset, err := this.stream.Append(storage.Payload(request.Payload))
	if err != nil {
		return nil, err
	}

	if request.Sync {
		if err := this.syncMonitor.waitForSync(ctx, offset.Offset); err != nil {
			return nil, err
		}
	}

	return &AppendReply{
		Offset: int64(offset.Offset),
	}, nil
}
