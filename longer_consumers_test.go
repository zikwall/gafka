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

type collection struct {
	mu           sync.RWMutex
	accumulation []string
}

func (c *collection) append(messages []string) {
	c.mu.Lock()
	c.accumulation = append(c.accumulation, messages...)
	c.mu.Unlock()
}

func TestConsumers(t *testing.T) {
	t.Run("it should be equal 50 messages", func(t *testing.T) {
		collect := collection{
			mu:           sync.RWMutex{},
			accumulation: make([]string, 0, 50),
		}

		ctx := context.Background()

		bootstrapTopics := []lib.Topic{
			{Name: "testTopicName", Partitions: 6},
		}

		gafka := lib.Gafka(ctx, lib.Configuration{
			BatchSize:       10,
			ReclaimInterval: time.Second * 1,
			Topics:          bootstrapTopics,
		})

		err, unsubscribe := gafka.Subscribe(ctx, "testTopicName", "group1", func(message lib.ReceiveMessage) {
			collect.append(message.Messages)
		})

		defer unsubscribe()

		if err != nil {
			log.Fatal(err)
		}

		for i := 0; i < 50; i++ {
			if err := gafka.Publish("testTopicName", fmt.Sprintf("message #%d", i)); err != nil {
				fmt.Println(err)
			}
		}

		// synthetic wait consume all messages
		time.Sleep(10 * time.Second)

		if len(collect.accumulation) != 50 {
			t.Log("Give count messages:", len(collect.accumulation))
			t.Fatal("Give wrong number of messages")
		}
	})

	t.Run("it should be equal 20 messages with cancel consumer", func(t *testing.T) {
		collect := collection{
			mu:           sync.RWMutex{},
			accumulation: make([]string, 0, 50),
		}

		ctx := context.Background()

		bootstrapTopics := []lib.Topic{
			{Name: "testTopicName", Partitions: 6},
		}

		gafka := lib.Gafka(ctx, lib.Configuration{
			BatchSize:       10,
			ReclaimInterval: time.Second * 1,
			Topics:          bootstrapTopics,
		})

		err, unsubscribe := gafka.Subscribe(ctx, "testTopicName", "group1", func(message lib.ReceiveMessage) {
			collect.append(message.Messages)
		})

		defer unsubscribe()

		if err != nil {
			log.Fatal(err)
		}

		err, unsubscribe2 := gafka.Subscribe(ctx, "testTopicName", "group1", func(message lib.ReceiveMessage) {
			collect.append(message.Messages)
		})

		go func() {
			time.Sleep(time.Second * 1)

			t.Log("Cancel consumer")

			defer unsubscribe2()
		}()

		if err != nil {
			log.Fatal(err)
		}

		for i := 0; i < 50; i++ {
			if err := gafka.Publish("testTopicName", fmt.Sprintf("message #%d", i)); err != nil {
				fmt.Println(err)
			}
		}

		// synthetic wait consume all messages
		time.Sleep(15 * time.Second)

		if len(collect.accumulation) != 50 {
			t.Log("Give count messages:", len(collect.accumulation))
			t.Fatal("Give wrong number of messages")
		}
	})

	t.Run("it should be equal 100 messages", func(t *testing.T) {
		collect := collection{
			mu:           sync.RWMutex{},
			accumulation: make([]string, 0, 100),
		}

		ctx := context.Background()

		bootstrapTopics := []lib.Topic{
			{Name: "testTopicName", Partitions: 6},
		}

		gafka := lib.Gafka(ctx, lib.Configuration{
			BatchSize:       10,
			ReclaimInterval: time.Second * 2,
			Topics:          bootstrapTopics,
		})

		// GROUP 1
		err, unsubscribe := gafka.Subscribe(ctx, "testTopicName", "group1", func(message lib.ReceiveMessage) {
			collect.append(message.Messages)
		})

		defer unsubscribe()

		// GROUP 2
		err, unsubscribe2 := gafka.Subscribe(ctx, "testTopicName", "group2", func(message lib.ReceiveMessage) {
			collect.append(message.Messages)
		})

		defer unsubscribe2()

		if err != nil {
			log.Fatal(err)
		}

		// 50 for two consumer group = 50 x 2 messages
		for i := 0; i < 50; i++ {
			if err := gafka.Publish("testTopicName", fmt.Sprintf("message #%d", i)); err != nil {
				fmt.Println(err)
			}
		}

		// synthetic wait consume all messages
		time.Sleep(12 * time.Second)

		if len(collect.accumulation) != 100 {
			t.Log("Give count messages:", len(collect.accumulation))
			t.Fatal("Give wrong number of messages")
		}
	})
}