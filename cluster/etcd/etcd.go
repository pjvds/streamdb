package etcd

import (
	"go.uber.org/zap"
	"github.com/coreos/etcd/clientv3"
	"golang.org/x/net/context"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/pjvds/streamdb/cluster"
	"github.com/pkg/errors"
)

type EtcdMasterElection struct{
	log *zap.Logger
	client *clientv3.Client
	value string
}

func NewMasterElection(log *zap.Logger, client *clientv3.Client) (*EtcdMasterElection) {
	return &EtcdMasterElection{
		log: log,
		client: client,
	}
}


type MasterLock struct{
	session *concurrency.Session
}

func (this *MasterLock) Lost() <-chan struct{} {
	return this.session.Done()
}

func (this *EtcdMasterElection) Run(ctx context.Context) (cluster.Election, error) {
	lock := make(chan cluster.MasterLock)

	go func() {
		defer close(lock)

		for{
			// TODO: rerun on error
			session, err := concurrency.NewSession(this.client, concurrency.WithContext(ctx), concurrency.WithTTL(5))
			if err != nil {
				errors.WithMessage(err, "etcd concurrency session creation failed")
				return
			}

			runSession := func(session *concurrency.Session) {
				defer session.Close()
				for {
					election := concurrency.NewElection(session, "streamdb_leader")
					if err := election.Campaign(ctx, this.value); err != nil {
						this.log.Warn("etcd election campaign failed", zap.Error(err))
						return
					}
					this.log.Debug("etcd election campaign won")

					select {
					case <-ctx.Done():
						this.log.Debug("master election run stopped because context closed")
						return
					case lock <- &MasterLock{session: session}:
					case <-session.Done():
						return
					}

					// block till be lost our session
					<-session.Done()
				}
			}
			runSession(session)
		}


	}()

	return cluster.Election{
		Won: lock,
	}, nil
}