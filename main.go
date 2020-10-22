package main

import (
	"context"
	"github.com/goavengers/gafka/lib"
	"github.com/urfave/cli/v2"
	"log"
	"os"
	"time"
)

func main() {
	application := &cli.App{
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "bind-address",
				Usage:    "IP и порт сервера, например: 0.0.0.0:3000",
				Required: true,
				EnvVars:  []string{"GAFKA_BIND_ADDRESS"},
			},
		},
	}

	application.Action = func(c *cli.Context) error {
		ctx := context.Background()

		gafka := lib.Gafka(ctx, lib.Configuration{
			BatchSize:       10,
			ReclaimInterval: time.Second * 2,
			Topics:          nil,
		})

		gafka.WaitSysNotify()
		gafka.Shutdown()

		return nil
	}

	err := application.Run(os.Args)

	if err != nil {
		log.Fatal(err)
	}
}
