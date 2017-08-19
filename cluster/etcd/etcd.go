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
	election *concurrency.Election
}

func (this *MasterLock) Lost() <-chan struct{} {
	return this.session.Done()
}

func (this *MasterLock) Resign() {
	this.election.Resign(context.Background())
}

func (this *MasterLock) Close() {
	this.session.Close()
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

func (this *EtcdElector) Elect(ctx context.Context) (cluster.Election) {
	won := make(chan cluster.MasterLock)
	log := this.log

	go func() {
		defer close(won)
		log.Debug("etcd election started", zap.String("key", this.key))

		session, err := concurrency.NewSession(this.client, concurrency.WithTTL(5))
		if err != nil {
			this.log.Error("etcd concurrency session creation failed", zap.Error(err))
			return
		}

		log := this.log.With(zap.Any("lease", session.Lease()))
		election := concurrency.NewElection(session, this.key)

		if err := election.Campaign(ctx, this.value); err != nil {
			log.Error("etcd election campaign failed", zap.Error(err))
			return
		}

		log.Debug("etcd election campaign won", zap.String("key", this.key), zap.String("value", this.value))

		select {
		case <-ctx.Done():
			session.Close()
		case won <- &MasterLock{session, election}:
		}
	}()

	return won
}