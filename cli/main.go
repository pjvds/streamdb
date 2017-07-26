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

				scanner := bufio.NewScanner(os.Stdin)
				client := controller.NewStreamControllerClient(conn)

				for scanner.Scan() && scanner.Err() == nil {
					reply, err := client.Append(context.Background(), &controller.AppendRequest{
						Payload: scanner.Bytes(),
					})

					if err != nil {
						log.Error("failed to append", zap.Error(err))
						return err
					}

					log.Info("append success", zap.String("offset", reply.Offset))
				}

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
				streamController := controller.NewStreamController(log, stream)

				controller.RegisterStreamControllerServer(server, streamController)

				log.Info("serving")
				return server.Serve(listener)
			},
		},

	}

	app.Run(os.Args)
}