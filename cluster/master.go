package cluster

import (
	"go.uber.org/zap"
	"time"
	"golang.org/x/net/context"
)

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
		log: log.With(zap.String("state","master")),
		lock: lock,
		elector: elector,
	}
}


func (this *MasterState) Run(ctx context.Context) (NodeState, error) {
	defer this.lock.Resign()

	for {
		select {
		case <-ctx.Done():
			this.log.Warn("context done", zap.Error(ctx.Err()));
			return nil, ctx.Err()
		case <-time.After(1 * time.Second):
			this.log.Info("mastering")
		case <-this.lock.Lost():
			this.log.Info("master lock lost")
			return NewInitState(this.log, this.elector), nil
		}
	}
}

