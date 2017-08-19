package cluster

import (
	"go.uber.org/zap"
	"time"
	"golang.org/x/net/context"
	"github.com/pkg/errors"
)

type FollowerState struct {
	log     *zap.Logger
	elector Elector
	leader string
}

func NewFollowerState(log *zap.Logger, leader string, elector Elector) (*FollowerState) {
	return &FollowerState{
		log:     log.With(zap.String("state", "follower")),
		leader: leader,
		elector: elector,
	}
}

func (this *FollowerState) Name() string {
	return "follower"
}

func (this *FollowerState) Run(ctx context.Context) (NodeState, error) {
	localCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	wins := this.elector.Elect(localCtx)
	leaderChanges := this.elector.Follow(localCtx)

	for {
		// TODO: handle channel close for all channels
		select {
		case <-ctx.Done():
			this.log.Info("follower state done")
			return nil, nil
		case <-time.After(1 * time.Second):
			this.log.Info("following " + this.leader)
		case value, ok := <-leaderChanges:
			if !ok {
				return nil, errors.New("election follower closed")
			}
			this.leader = value
			this.log.Info("new leader", zap.String("leader", this.leader))
		case lock, ok := <-wins:
			if !ok {
				return nil, errors.New("election closed")
			}
			return NewMasterState(this.log, lock, this.elector), nil
		}
	}
}
