package storage_test

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/pjvds/streamdb/storage"
	"github.com/stretchr/testify/assert"
	"github.com/pjvds/randombytes"
	"strconv"
	"go.uber.org/zap"
)

func TestLogStream_Append_and_Read(t *testing.T) {
	log := zap.NewNop()

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


	payload := []byte("hello-world")
	offset, err := stream.Append(payload);

	assert.Nil(t, err)
	assert.Equal(t, storage.LogOffset{
		Offset: 1,
		Page: 1,
		Location: 0,
	}, offset)

	buffer := make([]byte, 512)
	n, err := stream.Read(offset, buffer)
	assert.Nil(t, err)

	messages := storage.NewMessageSet(offset, buffer[0:n]).Scan()

	assert.Len(t, messages, 1)
	assert.Equal(t, payload, messages[0].Payload)
}

func TestLogStream_Append_1_million_messages(t *testing.T) {
	log := zap.NewNop()
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


	buffer := make([]byte, 512)
	for i := 0; i < 1e6; i++ {
		payload := []byte(strconv.Itoa(i))

		offset, err := stream.Append(payload);
		assert.Nil(t, err, "append error")

		_, err = stream.Read(offset, buffer)
		assert.Nil(t, err, "read error")

		assert.Equal(t, payload, storage.NewMessageSet(offset, buffer).Scan()[0].Payload)
	}
}


func BenchmarkLogStream_Append_10_million_512_byte_messages(b *testing.B) {
	log := zap.NewNop()
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

	b.ResetTimer()

	payload := randombytes.Make(512)
	b.SetBytes(int64(len(payload)) * 10e6)

	for n := 0; n < b.N; n++ {
		for i := 0; i < 10e6; i++ {
			_, err := stream.Append(payload);
			assert.Nil(b, err, "append error")
		}
	}

	b.StopTimer()
}
