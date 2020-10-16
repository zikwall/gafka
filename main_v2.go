package main

import (
	"context"
	"fmt"
	"github.com/goavengers/gafka/lib/v2"
	"log"
	"time"
)

func main() {
	ctx := context.Background()

	gafka := v2.Gafka(ctx, []v2.Topic{
		{Name: "testTopicName", Partitions: 4},
	})

	err, unsubscribe := gafka.Subscribe(ctx, "testTopicName", "group1", func(messages []string) {
		log.Println("First ", messages)
	})

	defer unsubscribe()

	if err != nil {
		log.Fatal(err)
	}

	err, unsubscribe2 := gafka.Subscribe(ctx, "testTopicName", "group1", func(messages []string) {
		log.Println("Second ", messages)
	})

	go func() {
		time.Sleep(5 * time.Second)
		unsubscribe2()
	}()

	if err != nil {
		log.Fatal(err)
	}

	err, unsubscribe3 := gafka.Subscribe(ctx, "testTopicName", "group2", func(messages []string) {
		log.Println("Third ", messages)
	})

	defer unsubscribe3()

	if err != nil {
		log.Fatal(err)
	}

	/*go func() {
		time.Sleep(3 * time.Second)

		err, unsubscribe := gafka.Subscribe(ctx, "testTopicName", "group1", func(messages []string) {
			log.Println("Four ", messages)
		})

		defer unsubscribe()

		if err != nil {
			log.Fatal(err)
		}

		time.Sleep(100 * time.Second)
	}()*/

	for i := 0; i <= 100; i++ {
		if err := gafka.Publish("testTopicName", fmt.Sprintf("message #%d", i)); err != nil {
			fmt.Println(err)
		}

		time.Sleep(1 * time.Second)
	}

	// todo pubsriber.Wait()
	time.Sleep(100 * time.Second)
}
