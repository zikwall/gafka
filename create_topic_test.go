package main

import (
	"context"
	"fmt"
	"github.com/goavengers/gafka/lib"
	"log"
	"sync"
	"testing"
	"time"
)

func TestCreateTopic(t *testing.T) {
	t.Run("it should be topic created and receive 50 messages", func(t *testing.T) {
		collect := collection{
			mu:           sync.RWMutex{},
			accumulation: make([]string, 0, 50),
		}

		topic := "dynamic"

		ctx := context.Background()

		gafka := lib.Gafka(ctx, lib.Configuration{
			BatchSize:       10,
			ReclaimInterval: time.Millisecond * 100,
			Topics:          nil,
		})

		err := gafka.CreateTopic(lib.Topic{
			Name:       topic,
			Partitions: 4,
		})

		if err != nil {
			log.Fatal(err)
		}

		go func() {
			for i := 0; i < 50; i++ {
				if err := gafka.Publish(topic, fmt.Sprintf("message #%d", i)); err != nil {
					fmt.Println(err)
				}

				time.Sleep(1 * time.Millisecond)
			}
		}()

		err, unsubscribe := gafka.Subscribe(ctx, lib.SubscribeConf{
			Topic: topic,
			Group: "group1",
			Handler: func(message lib.ReceiveMessage) {
				collect.append(message.Messages)
			},
		})

		if err != nil {
			log.Fatal(err)
		}

		defer unsubscribe()

		go func() {
			time.Sleep(time.Second * 1)

			err, _ := gafka.Subscribe(ctx, lib.SubscribeConf{
				Topic: topic,
				Group: "group1",
				Handler: func(message lib.ReceiveMessage) {
					collect.append(message.Messages)
				},
			})

			if err != nil {
				log.Fatal(err)
			}
		}()

		time.Sleep(10 * time.Second)

		if len(collect.accumulation) != 50 {
			t.Log("Give count messages:", len(collect.accumulation))
			t.Fatal("Give wrong number of messages")
		}
	})
}
