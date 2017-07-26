package controller

import (
	"testing"
	"io/ioutil"
	"os"
	"github.com/pjvds/streamdb/storage"
	"go.uber.org/zap"
	"context"
	"github.com/stretchr/testify/assert"
	"sync"
	"sync/atomic"
)

func TestStreamController_Append(t *testing.T) {
	log, _ := zap.NewDevelopment()

	dir, err := ioutil.TempDir("", "streamdb")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	stream, err := storage.OpenLogStream(log, dir)
	if err != nil {
		t.Fatalf("open storage directory failed: %v", err)
	}
	defer stream.Close()

	syncStrategy := newSyncMonitor(log, stream)
	defer syncStrategy.Close()

	controller := NewStreamController(log.Sugar(), syncStrategy, stream)

	reply, err := controller.append(context.Background(), &AppendRequest{
		Sync: true,
		Payload: []byte("hello-world"),
	})

	assert.Nil(t, err)
	assert.Equal(t, int64(1), reply.Offset)
}

func BenchmarkStreamController_Append_100k_messages_with_50_concurrent_clients_no_sync(b *testing.B) {
	log, _:= zap.NewDevelopment()
	work := sync.WaitGroup{}

	dir, err := ioutil.TempDir("", "streamdb")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	stream, err := storage.OpenLogStream(log, dir)
	if err != nil {
		b.Fatalf("open storage directory failed: %v", err)
	}
	defer stream.Close()

	syncStrategy := newSyncMonitor(log, stream)
	defer syncStrategy.Close()

	controller := NewStreamController(log.Sugar(), syncStrategy, stream)

	toSend := int32(1e5)

	b.ResetTimer()

	for n := 0; n < b.N; n++{
		for i := 0; i < 50; i++ {
			work.Add(1)

			go func(i int) {
				defer work.Done()

				for {
					_, err := controller.append(context.Background(), &AppendRequest{
						Sync:    false,
						Payload: []byte("hello-world"),
					})
					assert.Nil(b, err)

					if atomic.AddInt32(&toSend, -1) <= 0 {
						log.Debug("worked stopped", zap.Int("worker", i))
						return
					}
				}
			}(i)
		}

		work.Wait()
	}

	b.StopTimer()
}

func BenchmarkStreamController_Append_100k_messages_with_50_concurrent_clients_sync(b *testing.B) {
	log, _:= zap.NewDevelopment()
	work := sync.WaitGroup{}

	dir, err := ioutil.TempDir("", "streamdb")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	stream, err := storage.OpenLogStream(log, dir)
	if err != nil {
		b.Fatalf("open storage directory failed: %v", err)
	}
	defer stream.Close()

	syncStrategy := newSyncMonitor(log, stream)
	defer syncStrategy.Close()

	controller := NewStreamController(log.Sugar(), syncStrategy, stream)

	toSend := int32(1e5)

	b.ResetTimer()

	for n := 0; n < b.N; n++{
		for i := 0; i < 50; i++ {
			work.Add(1)

			go func(i int) {
				defer work.Done()

				for {
					_, err := controller.append(context.Background(), &AppendRequest{
						Sync:    true,
						Payload: []byte("hello-world"),
					})
					assert.Nil(b, err)

					if atomic.AddInt32(&toSend, -1) <= 0 {
						log.Debug("worked stopped", zap.Int("worker", i))
						return
					}
				}
			}(i)
		}

		work.Wait()
	}

	b.StopTimer()
}