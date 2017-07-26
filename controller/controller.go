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

	accumulator chan AppendRequestEnvelope
	appender chan []AppendRequestEnvelope
}

type SyncMonitor struct{
	log *zap.Logger
	requests chan syncMonitorEntry

	stream *storage.LogStream

	closed chan struct{}
	closing chan struct{}
}

type AppendRequestEnvelope struct {
	Request *AppendRequest
	Reply chan AppendReplyEnvelope
}

type AppendReplyEnvelope struct{
	Reply *AppendReply
	Err error
}

type AppendPayloadRequestEnvelope struct{
	Requests []AppendRequestEnvelope
	Payload storage.Payload
	Reply chan AppendPayloadReplyEnvelope
}

func (this AppendPayloadRequestEnvelope) Fail(err error) {
	reply := AppendReplyEnvelope{
		Err: err,
		Reply: nil,
	}

	for _, request := range this.Requests{
		request.Reply <- reply
	}
}

func (this AppendPayloadRequestEnvelope) Success(offset storage.LogOffset) {
	individualOffset := int64(offset.Offset)

	for _, request := range this.Requests{
		request.Reply <- AppendReplyEnvelope{
			Reply: &AppendReply{
				Offset: individualOffset,
			},
		}

		individualOffset += storage.SinglePayload(request.Request.Payload).SizeOnDisk64()
	}
}

type AppendPayloadReplyEnvelope struct{
	Offset storage.LogOffset
	Err error
}

type AppendRequestAccumulator struct {
	accumulate chan AppendRequestEnvelope
	dispatch chan AppendPayloadRequestEnvelope
	stream storage.LogStream
}

func (this AppendRequestAccumulator) doAccumulation() {
	for {
		select {
		case request := <- this.accumulate:
			set := &storage.PayloadSet{}
			set.Append(request.Request.Payload)

			storageRequest := AppendPayloadRequestEnvelope{
				Requests: append([]AppendRequestEnvelope{}, request),
				Payload: set,
			}

			for {
				select {
				case incoming := <- this.accumulate:
					set.Append(incoming.Request.Payload)
					storageRequest.Requests = append(storageRequest.Requests, incoming)
				case this.dispatch <- storageRequest:
					break
				}
			}
		}
	}
}

func (this AppendRequestAccumulator) doAppend() {
	for request := range this.dispatch {
		offset, err := this.stream.Append(request.Payload)
		if err != nil {
			request.Fail(err)
		} else {
			request.Success(offset)
		}
	}
}

func (this AppendRequestAccumulator) Append(request *AppendRequest) (*AppendReply, error) {
	reply := make(chan AppendReplyEnvelope, 1)

	this.accumulate <- AppendRequestEnvelope{
		Request: request,
		Reply: reply,
	}

	appendReply := <-reply
	return appendReply.Reply, appendReply.Err
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
	ticker := time.NewTicker(50 * time.Millisecond)
	defer func() {
		ticker.Stop()
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
		case <-ticker.C:
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
					continue
				}

				outstanding[i] = request
				i++
			}
			outstanding = outstanding[0:i]
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
	offset, err := this.stream.Append(storage.SinglePayload(request.Payload))
	if err != nil {
		return nil, err
	}

	if request.Sync {
		if err := this.syncMonitor.waitForSync(ctx, offset.Offset); err != nil {
			panic(err)
			return nil, err
		}
	}

	return &AppendReply{
		Offset: int64(offset.Offset),
	}, nil
}
