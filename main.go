package main

import (
	"context"
	"fmt"
	"github.com/goavengers/gafka/lib"
	"log"
	"time"
)

func main() {
	ctx := context.Background()

	gafka := lib.Gafka(ctx, []lib.Topic{
		{Name: "testTopicName", Partitions: 4},
	})

	go func() {
		if err := gafka.Subscribe(ctx, "testTopicName", "group1", func(message []string) {
			log.Println("First ", message)
		}); err != nil {
			log.Fatal(err)
		}
	}()

	go func() {
		if err := gafka.Subscribe(ctx, "testTopicName", "group1", func(message []string) {
			log.Println("Second ", message)
		}); err != nil {
			log.Fatal(err)
		}
	}()

	for i := 0; i <= 100; i++ {
		gafka.Publish("testTopicName", fmt.Sprintf("message #%d", i))

		time.Sleep(1 * time.Second)
	}

	// todo pubsriber.Wait()
	time.Sleep(100 * time.Second)
}
