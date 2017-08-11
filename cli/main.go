package main

import (
	"os"
	"github.com/urfave/cli"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"github.com/pjvds/streamdb/controller"
	"github.com/pjvds/streamdb/cluster/etcd"
	"context"
	"bufio"
	"sync"
	"github.com/pjvds/streamdb/cluster"
	"github.com/coreos/etcd/clientv3"
	"github.com/rs/xid"
)


func main() {
	app := cli.NewApp()
	app.Name = "streamdb"
	app.Usage = ""
	app.Commands = []cli.Command{
		{
			Name: "append",
			Action: func(c *cli.Context) error {
				log, err := zap.NewProduction()
				if err != nil {
					panic(err)
				}

				conn, err := grpc.Dial("127.0.0.1:8888", grpc.WithInsecure())
				if err != nil {
					log.Error("failed to dial server", zap.Error(err))
					return err
				}
				defer conn.Close()

				client := controller.NewStreamControllerClient(conn)

				payloads := make(chan []byte)

				go func() {
					scanner := bufio.NewScanner(os.Stdin)
					defer close(payloads)

					for scanner.Scan() && scanner.Err() == nil {
						payloads <- scanner.Bytes()
					}

					if err := scanner.Err(); err != nil {
						log.Error("scan failed", zap.Error(err))
					}
				}()

				work := sync.WaitGroup{}
				for i := 0; i < 50; i++ {
					work.Add(1)
					go func(worker int) {
						defer work.Done()

						for payload := range payloads {
							reply, err := client.Append(context.Background(), &controller.AppendRequest{
								Payload: payload,
							})

							if err != nil {
								log.Error("failed to append", zap.Error(err))
								continue
							}

							log.Info("append success", zap.Stringer("offset", reply.Offset))
						}
					}(i)
				}

				work.Wait()
				return nil
			},
		},
		{
			Name: "serve",
			Flags: []cli.Flag{
				cli.StringFlag{
					Name: "address",
					Value: "127.0.0.1:8888",
				},
				cli.StringFlag{
					Name: "etcd",
					Value: "127.0.0.1:2379",
				},
				cli.StringFlag{
					Name: "id",
				},
			},
			Action: func(c *cli.Context) error {
				log, err := zap.NewDevelopment()
				if err != nil {
					panic(err)
				}

				etcdClient, err := clientv3.NewFromURL(c.String("etcd"))
				if err != nil {
					log.Error("dial etcd failed", zap.Error(err))
					return err
				}
				defer etcdClient.Close()

				id := c.String("id")
				if len(id) == 0 {
					id = xid.New().String()
				}

				store := cluster.StateStore(etcd.NewStateStore(log,etcdClient))
				elector := cluster.Elector(etcd.NewMasterElection(log, etcdClient, "/leader", id))

				cluster, err := cluster.NewCluster(log, store, elector, cluster.NodeOptions{
					Id: id,
					Address: c.String("address"),
				})
				if err != nil {
					log.Error("dial cluster failed", zap.Error(err))
					return err
				}

				if err := cluster.Run(context.Background()); err != nil {
					log.Error("run cluster failure", zap.Error(err))
					return err
				}
				return nil
			},
		},

	}

	app.Run(os.Args)
}