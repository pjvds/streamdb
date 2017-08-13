package etcd

import (
	"go.uber.org/zap"
	"github.com/coreos/etcd/clientv3"
	"golang.org/x/net/context"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/pjvds/streamdb/cluster"
)

type EtcdElector struct{
	log *zap.Logger
	client *clientv3.Client
	key string
	value string
}

func NewMasterElection(log *zap.Logger, client *clientv3.Client, key, value string) (*EtcdElector) {
	return &EtcdElector{
		log: log.Named("etcd.elector"),
		client: client,
		key: key,
		value: value,
	}
}


type MasterLock struct{
	session *concurrency.Session
	election concurrency.Election
}

func (this *MasterLock) Lost() <-chan struct{} {
	return this.session.Done()
}

func (this *MasterLock) Resign() {
	this.election.Resign(context.Background())
}

func (this *EtcdElector) Follow(ctx context.Context) cluster.Follower {
	changes := make(chan string)
	go this.follow(ctx, changes)
	return changes
}

func (this *EtcdElector) follow(ctx context.Context, changes chan string) {
	defer close(changes)

	// TODO: rerun on error
	session, err := concurrency.NewSession(this.client, concurrency.WithContext(ctx), concurrency.WithTTL(5))
	if err != nil {
		this.log.Error("failed to create concurrency session", zap.Error(err))
		return
	}

	election := concurrency.NewElection(session, this.key)
	for observation := range election.Observe(ctx) {
		value := string(observation.Kvs[0].Value)

		// Filter out our own value. If the current node became leader
		// it will get notified via the elect channel.
		if value != this.value {
			changes <- value
		}
	}
}

func (this *EtcdElector) Elect(ctx context.Context) (cluster.Election, error) {
	won := make(chan cluster.MasterLock)
	this.log.Debug("elector started")

	go func() {
		defer close(won)

		for{
			// TODO: rerun on error
			session, err := concurrency.NewSession(this.client, concurrency.WithContext(ctx), concurrency.WithTTL(5))
			if err != nil {
				this.log.Error("failed to create concurrency session", zap.Error(err))
				return
			}

			for {
				election := concurrency.NewElection(session, this.key)
				if err := election.Campaign(ctx, this.value); err != nil {
					this.log.Warn("etcd election campaign failed", zap.Error(err))
					return
				}
				this.log.Debug("etcd election campaign won", zap.String("key", this.key), zap.String("value", this.value))

				select {
				case <-ctx.Done():
					session.Close();

					this.log.Debug("master election run stopped because context closed")
					return
				case won <- &MasterLock{session: session}:
				case <-session.Done():
					break
				}

				// block till be lost our session
				<-session.Done()
				break
			}
		}


	}()

	return won, nil
}