package cluster

import (
	"time"
	"golang.org/x/net/context"
)

type NodeState interface{
	Name() string
	Run(ctx context.Context) (NodeState, error)
}



type StateStore interface{
	UpdateNodeAddress(id string, address string) error
	UpdateNodeLastSeen(id string, lastSeen time.Time) error
}