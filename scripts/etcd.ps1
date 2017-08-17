docker run `
  --rm `
  -p 2379:2379 `
  -p 2380:2380 `
  --name etcd-1 `
  --volume=/tmp/etcd-data:/etcd-data `
  "gcr.io/etcd-development/etcd:v3.2" `
  /usr/local/bin/etcd `
  --name etcd-1 `
  --data-dir /etcd-data `
  --listen-client-urls http://0.0.0.0:2379 `
  --advertise-client-urls http://localhost:2379 `
  --listen-peer-urls http://0.0.0.0:2380 `
  --initial-advertise-peer-urls http://localhost:2380 `
  --initial-cluster my-etcd-1=http://localhost:2380 `
  --initial-cluster-state new `
  --auto-compaction-retention 1
