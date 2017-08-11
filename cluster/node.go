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

	client *clientv3.Client

	options NodeOptions

	ctx context.Context

	elector MasterElection
}

type NodeState interface{
	Run(ctx context.Context) (NodeState, error)
}

type MasterElection interface{
	Run(context.Context) (Election, error)
}

type LeaderState struct{
	IsMaster bool
	Value string
}

type MasterLock interface{
	Lost() <-chan struct{}
}

type Election struct{
	Won chan MasterLock
}

type InitState struct{
	log *zap.Logger
	elector MasterElection
}

func NewInitState(log *zap.Logger, elector MasterElection) (*InitState) {
	return &InitState{
		log: log,
		elector: elector,
	}
}

type MasterState struct{
	log *zap.Logger
	ctx context.Context
	lock MasterLock
	election Election
}

func NewMasterState(log *zap.Logger, lock MasterLock, election Election) (*MasterState) {
	return &MasterState{
		log: log,
		lock: lock,
		election: election,
	}
}

type FollowerState struct{
	log *zap.Logger
	election Election
}

func NewFollowerState(log *zap.Logger, election Election) (*FollowerState) {
	return &FollowerState{
		log: log,
		election: election,
	}
}

func (this *FollowerState) Run(ctx context.Context) (NodeState, error) {
	for {
		select {
		case <-ctx.Done():
			this.log.Info("follower state done")
			return nil, nil
		case <-time.After(1 * time.Second):
			this.log.Info("following")
		case lock, ok := <-this.election.Won:
			if !ok {
				return nil, errors.New("election closed")
			}
			return NewMasterState(this.log, lock, this.election), nil
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
			return NewFollowerState(this.log, this.election), nil
		}
	}
}

func (this *InitState) Run(ctx context.Context) (NodeState, error) {
	election, err := this.elector.Run(ctx)
	if err != nil {
		return nil, errors.WithMessage(err, "elector run failed")
	}

	return NewFollowerState(this.log, election), nil
}

type NodeOptions struct{
	Address string
}

func NewCluster(log *zap.Logger, elector MasterElection, options NodeOptions) (*Node, error) {
	return &Node{
		options: options,

		log: log,
		elector: elector,
	}, nil
}

func (this *Node) Run(ctx context.Context) error {
	state := NodeState(NewInitState(this.log, this.elector))
	for {
		next, err := state.Run(ctx)
		if err != nil {
			return err
		}
		state = next
	}
}
