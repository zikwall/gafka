package main

import (
	"context"
	"fmt"
	"github.com/goavengers/gafka/lib"
	"log"
	"time"
)

func main() {
	pubscriber := lib.NewPubScriber([]string{
		"testTopicName",
	})

	ctx := context.Background()

	go func() {
		if err := pubscriber.Subscribe(ctx, "testTopicName", func(message string) {
			log.Println("First ", message)
		}); err != nil {
			log.Fatal(err)
		}
	}()

	go func() {
		if err := pubscriber.Subscribe(ctx, "testTopicName", func(message string) {
			log.Println("Second ", message)
		}); err != nil {
			log.Fatal(err)
		}
	}()

	for i := 0; i <= 100; i++ {
		pubscriber.Publish("testTopicName", fmt.Sprintf("message #%d", i))
	}

	// todo pubsriber.Wait()
	time.Sleep(100 * time.Second)
}
