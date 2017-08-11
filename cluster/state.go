package cluster

import (
	"time"
)

type StateStore interface{
	UpdateNodeAddress(id string, address string) error
	UpdateNodeLastSeen(id string, lastSeen time.Time) error
}