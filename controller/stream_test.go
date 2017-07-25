package controller

import (
	"testing"
	"io/ioutil"
	"os"
	"github.com/pjvds/streamdb/storage"
	"go.uber.org/zap"
	"context"
	"github.com/stretchr/testify/assert"
)

func TestStreamController_Append(t *testing.T) {
	log, _ := zap.NewDevelopment()

	dir, err := ioutil.TempDir("", "streamdb")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	stream, err := storage.OpenLogStream(dir)
	if err != nil {
		t.Fatalf("open storage directory failed: %v", err)
	}
	defer stream.Close()

	syncStrategy := newSyncMonitor(log.Sugar(), stream)
	defer syncStrategy.Close()

	controller := NewStreamController(log.Sugar(), syncStrategy, stream)

	reply, err := controller.append(context.Background(), &AppendRequest{
		Sync: true,
		Payload: []byte("hello-world"),
	})

	assert.Nil(t, err)
	assert.Equal(t, int64(1), reply.Offset)
}