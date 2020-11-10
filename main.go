package main

import (
	"context"
	"github.com/urfave/cli/v2"
	"github.com/zikwall/gafka/src/core"
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
			&cli.StringFlag{
				Name:    "topic_list",
				Usage:   "Список названий топиков и их партиций через запятую в формате <String:Int> (topicName:partitions)",
				EnvVars: []string{"GAFKA_TOPIC_LIST"},
			},
		},
	}

	application.Action = func(c *cli.Context) error {
		ctx := context.Background()

		gafka := core.Gafka(ctx, core.Configuration{
			BatchSize:       10,
			ReclaimInterval: time.Second * 2,
			Topics: core.ResolveBootstrappedTopics(
				c.String("topic_list"),
			),
		})

		gafka.WaitInternalNotify()
		gafka.Shutdown()

		return nil
	}

	err := application.Run(os.Args)

	if err != nil {
		log.Fatal(err)
	}
}
