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

	bootstrapTopics := []v2.Topic{
		{Name: "testTopicName", Partitions: 6},
	}

	gafka := v2.Gafka(ctx, v2.Configuration{
		BatchSize:       10,
		ReclaimInterval: time.Second * 2,
		Topics:          bootstrapTopics,
	})

	err, unsubscribe := gafka.Subscribe(ctx, "testTopicName", "group1", func(messages []string) {
		log.Println("First ", messages)
	})

	defer unsubscribe()

	if err != nil {
		log.Fatal(err)
	}

	err, unsubscribe2 := gafka.Subscribe(ctx, "testTopicName", "group2", func(messages []string) {
		log.Println("Second ", messages)
	})

	defer unsubscribe2()

	//go func() {
	//time.Sleep(13 * time.Second)
	//unsubscribe2()
	//}()

	if err != nil {
		log.Fatal(err)
	}

	/*err, unsubscribe3 := gafka.Subscribe(ctx, "testTopicName", "group1", func(messages []string) {
		log.Println("Third ", messages)
	})

	defer unsubscribe3()

	if err != nil {
		log.Fatal(err)
	}*/

	go func() {
		time.Sleep(3 * time.Second)

		err, unsubscribe := gafka.Subscribe(ctx, "testTopicName", "group1", func(messages []string) {
			log.Println("Four ", messages)
		})

		defer unsubscribe()

		if err != nil {
			log.Fatal(err)
		}

		time.Sleep(1000 * time.Second)
	}()

	go func() {
		time.Sleep(5 * time.Second)

		err, unsubscribe := gafka.Subscribe(ctx, "testTopicName", "group1", func(messages []string) {
			log.Println("Five ", messages)
		})

		defer unsubscribe()

		if err != nil {
			log.Fatal(err)
		}

		time.Sleep(1000 * time.Second)
	}()

	go func() {
		time.Sleep(8 * time.Second)

		err, unsubscribe := gafka.Subscribe(ctx, "testTopicName", "group2", func(messages []string) {
			log.Println("Five ", messages)
		})

		defer unsubscribe()

		if err != nil {
			log.Fatal(err)
		}

		time.Sleep(1000 * time.Second)
	}()

	for i := 0; i <= 100; i++ {
		if err := gafka.Publish("testTopicName", fmt.Sprintf("message #%d", i)); err != nil {
			fmt.Println(err)
		}

		time.Sleep(100 * time.Millisecond)
	}

	// todo pubsriber.Wait()
	time.Sleep(1000 * time.Second)
}
