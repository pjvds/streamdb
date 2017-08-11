package etcd

import (
	"go.uber.org/zap"
	"github.com/coreos/etcd/clientv3"
	"golang.org/x/net/context"
	"time"
)

type StateStore struct{
	log *zap.Logger
	client *clientv3.Client
}

func NewStateStore(log *zap.Logger, client *clientv3.Client) *StateStore{
	return &StateStore{
		log: log,
		client: client,
	}
}

func (this *StateStore) UpdateNodeAddress(id string, address string) error {
	nodeLocation := "/nodes/"+id
	_, err := this.client.KV.Put(context.Background(), nodeLocation + "/address", address)
	return err
}

func (this *StateStore) UpdateNodeLastSeen(id string, seenAt time.Time) error {
	nodeLocation := "/nodes/"+id

	_, err := this.client.KV.Put(context.Background(), nodeLocation + "/lastSeen", seenAt.String())
	return err
}