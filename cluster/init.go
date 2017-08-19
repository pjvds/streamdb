package cluster

import (
	"go.uber.org/zap"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

type InitState struct{
	log *zap.Logger
	elector Elector
}

func NewInitState(log *zap.Logger, elector Elector) (*InitState) {
	return &InitState{
		log: log.With(zap.String("state", "init")),
		elector: elector,
	}
}

func (this *InitState) Name() string {
	return "init"
}

func (this *InitState) Run(ctx context.Context) (NodeState, error) {
	localCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	select{
		case <-ctx.Done():
			return nil, ctx.Err()
		case lock, ok := <-this.elector.Elect(localCtx):
			if !ok {
				return nil, errors.New("elect failed")
			}
			return NewMasterState(this.log, lock, this.elector), nil
		case leader, ok := <-this.elector.Follow(localCtx):
			if !ok {
				return nil, errors.New("follow failed")
			}
			return NewFollowerState(this.log, leader, this.elector), nil
	}
}
