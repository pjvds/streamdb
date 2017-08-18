package cluster

import (
	"go.uber.org/zap"
	"golang.org/x/net/context"
	"github.com/pkg/errors"
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

type NodeState interface{
	Name() string
	Run(ctx context.Context) (NodeState, error)
}

type Elector interface{
	Elect(ctx context.Context) (Election, error)
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

type InitState struct{
	log *zap.Logger
	id string
	address string
	store StateStore
	elector Elector
}

func NewInitState(log *zap.Logger, id string, address string, store StateStore, elector Elector) (*InitState) {
	return &InitState{
		log: log,
		id: id,
		address: address,
		store: store,
		elector: elector,
	}
}

type MasterState struct{
	log *zap.Logger
	ctx context.Context
	lock MasterLock
	elector Elector
}

func (this *MasterState) Name() string {
	return "master"
}

func NewMasterState(log *zap.Logger, lock MasterLock, elector Elector) (*MasterState) {
	return &MasterState{
		log: log.Named("MasterState"),
		lock: lock,
		elector: elector,
	}
}

type FollowerState struct{
	log *zap.Logger
	elector Elector
}

func NewFollowerState(log *zap.Logger, elector Elector) (*FollowerState) {
	return &FollowerState{
		log: log.Named("FollowerState"),
		elector: elector,
	}
}

func (this *FollowerState) Name() string {
	return "follower"
}

func (this *FollowerState) Run(ctx context.Context) (NodeState, error) {
	localCtx, cancel := context.WithCancel(ctx)

	leaderChanges := this.elector.Follow(localCtx)
	wins, err := this.elector.Elect(localCtx)
	if err != nil {
		return nil, errors.WithMessage(err, "elector failed to elect")
	}

	defer cancel()

	leader := "<unknown>"

	for {
		select {
		case <-ctx.Done():
			this.log.Info("follower state done")
			return nil, nil
		case <-time.After(1 * time.Second):
			this.log.Info("following " + leader)
		case leader = <- leaderChanges:
			this.log.Info("new leader", zap.String("leader", leader))
		case lock, ok := <-wins:
			if !ok {
				return nil, errors.New("election closed")
			}
			return NewMasterState(this.log, lock, this.elector), nil
		}
	}
}

func (this *MasterState) Run(ctx context.Context) (NodeState, error) {
	for {
		select {
		case <-ctx.Done():
			this.log.Warn("context done", zap.Error(ctx.Err()));
			return nil, ctx.Err()
		case <-time.After(1 * time.Second):
			this.log.Info("mastering")
		case <-this.lock.Lost():
			this.log.Info("master lock lost")
			return NewFollowerState(this.log, this.elector), nil
		}
	}
}

func (this *InitState) Name() string {
	return "init"
}

func (this *InitState) Run(ctx context.Context) (NodeState, error) {
	if err := this.store.UpdateNodeAddress(this.id, this.address); err != nil {
		return nil, errors.WithMessage(err, "update node address failed")
	}
	if err := this.store.UpdateNodeLastSeen(this.id, time.Now().UTC()); err != nil {
		return nil, errors.WithMessage(err, "update node last seen failed")
	}

	return NewFollowerState(this.log, this.elector), nil
}

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

	state := NodeState(NewInitState(this.log, this.id, this.address, this.store, this.elector))
	for {
		this.log.Debug("running state", zap.String("state", state.Name()))

		next, err := state.Run(runCtx)
		if next == nil || err != nil {
			return err
		}
		state = next
	}
}
