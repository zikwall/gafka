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
		if err := gafka.Subscribe(ctx, "testTopicName", "group2", func(message []string) {
			log.Println("Second ", message)
		}); err != nil {
			log.Fatal(err)
		}
	}()

	for i := 0; i <= 100; i++ {
		gafka.Publish("testTopicName", fmt.Sprintf("message #%d", i))
	}

	// todo pubsriber.Wait()
	time.Sleep(100 * time.Second)
}
