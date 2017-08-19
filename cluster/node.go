package cluster

import (
	"go.uber.org/zap"
	"golang.org/x/net/context"
	"time"
	"github.com/coreos/etcd/clientv3"
)

type Node struct{
	log *zap.Logger

	id string
	address string

	client *clientv3.Client

	ctx context.Context

	store StateStore
	elector Elector
}

type Elector interface{
	Elect(ctx context.Context) (Election)
	Follow(ctx context.Context) Follower
}

type LeaderState struct{
	IsMaster bool
	Value string
}

type MasterLock interface{
	Lost() <-chan struct{}
	Resign()
}

type Election <-chan MasterLock
type Follower <-chan string

type NodeOptions struct{
	Id string
	Address string
}

func NewCluster(log *zap.Logger, store StateStore, elector Elector, options NodeOptions) (*Node, error) {
	return &Node{
		id: options.Id,
		address: options.Address,
		store: store,

		log: log,
		elector: elector,
	}, nil
}

func (this *Node) Run(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		for {
			select{
			case <-runCtx.Done():
				return
			case <-time.After(5 * time.Second):
				if err := this.store.UpdateNodeLastSeen(this.id, time.Now().UTC()); err != nil {
					this.log.Debug("node last seen state update failed", zap.Error(err))
				}
			}
		}
	}()

	state := NodeState(NewInitState(this.log, this.elector))
	for {
		this.log.Debug("running state", zap.String("state", state.Name()))

		next, err := state.Run(runCtx)
		if next == nil || err != nil {
			return err
		}
		state = next
	}
}
