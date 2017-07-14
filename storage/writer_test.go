package storage_test

import (
	"os"
	"strconv"
	"testing"

	"github.com/pjvds/streamdb/storage"
)

func BenchmarkFileStorageManager_Append_5_Streams_256b_payload(b *testing.B) {
	manager, err := storage.OpenStorageDirectory(os.TempDir())
	if err != nil {
		b.Fatalf("open storage directory failed: %v", err)
	}

	payload := storage.Payload(make([]byte, 256))

	for i := 0; i < 5; i++ {
		id := storage.StreamID(strconv.Itoa(i % 5))
		_, err := manager.Append(id, payload)

		if err != nil {
			b.Fatalf("initial append to stream %v failed: %v", id, err)
		}
	}

	b.ResetTimer()

	// write to 5 streams asap
	for i := 0; i < b.N; i++ {
		id := storage.StreamID(strconv.Itoa(i % 5))
		_, err := manager.Append(id, payload)

		if err != nil {
			b.Fatalf("append to stream %v failed at benchmark iteration: %v", id, i, err)
		}
	}

	b.StopTimer()
}
