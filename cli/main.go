package main

import (
	"fmt"
	"os"
	"github.com/urfave/cli"
	"go.uber.org/zap"
	"net"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"github.com/pjvds/streamdb/controller"
	"io/ioutil"
	"github.com/pjvds/streamdb/storage"
	"context"
	"bufio"
	"github.com/cactus/go-statsd-client/statsd"
	statsdreporter "github.com/uber-go/tally/statsd"
	"time"
	"github.com/uber-go/tally"
	"sync"
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

							log.Info("append success", zap.String("offset", reply.Offset))
						}
					}(i)
				}

				work.Wait()
				return nil
			},
		},
		{
			Name: "serve",
			Action: func(c *cli.Context) error {
				log, err := zap.NewProduction()
				if err != nil {
					panic(err)
				}

				statter, err := statsd.NewBufferedClient("127.0.0.1:8125",
					"stats", 100*time.Millisecond, 1440)
				if err != nil {
					log.Error("could not create statsd client", zap.Error(err))
					return err
				}

				opts := statsdreporter.Options{}
				r := statsdreporter.NewReporter(statter, opts)
				scope, closer := tally.NewRootScope(tally.ScopeOptions{
					Prefix:   "streamdb-server",
					Tags:     map[string]string{},
					Reporter: r,
				}, 1*time.Second)
				defer closer.Close()

				dir, err := ioutil.TempDir("", "streamdb")
				if err != nil {
					log.Error("failed to create temp directory", zap.Error(err))
					return err
				}
				log.Info("created temp directory for data usage", zap.String("dir", dir))
				defer os.RemoveAll(dir)

				stream, err := storage.OpenLogStream(log, dir)
				if err != nil {
					log.Error("failed to open storage directory", zap.Error(err))
					return errors.Errorf("open storage directory failed: %v", err)
				}
				defer stream.Close()

				listener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", 8888))
				if err != nil {
					log.Error("failed to listen", zap.Error(err))
					return errors.Errorf("failed to listen: %v", err)
				}
				defer listener.Close()

				server := grpc.NewServer()
				streamController := controller.NewStreamController(log, scope, stream)

				controller.RegisterStreamControllerServer(server, streamController)

				log.Info("serving")
				return server.Serve(listener)
			},
		},

	}

	app.Run(os.Args)
}