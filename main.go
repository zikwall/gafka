package main

import (
	"context"
	"fmt"
	"github.com/goavengers/gafka/lib"
	"log"
	"time"
)

func main() {
	topic := "testTopicName"

	ctx := context.Background()

	bootstrapTopics := []lib.Topic{
		{Name: "testTopicName", Partitions: 6},
	}

	gafka := lib.Gafka(ctx, lib.Configuration{
		BatchSize:       10,
		ReclaimInterval: time.Second * 2,
		Topics:          bootstrapTopics,
	})

	err, unsubscribe := gafka.Subscribe(ctx, lib.SubscribeConf{
		Topic: topic,
		Group: "group1",
		Handler: func(message lib.ReceiveMessage) {
			//log.Println("First")
			//log.Println(message.Topic)
			//log.Println(message.Partition)
			//log.Println(message.Messages)
		},
	})

	go func() {
		// advanced test
		// cancel consumer after 11 seconds
		time.Sleep(time.Second * 6)
		unsubscribe()
	}()

	if err != nil {
		log.Fatal(err)
	}

	go func() {
		time.Sleep(3 * time.Second)

		err, unsubscribe := gafka.Subscribe(ctx, lib.SubscribeConf{
			Topic: topic,
			Group: "group1",
			Handler: func(message lib.ReceiveMessage) {
				//log.Println("Second")
				//log.Println(message.Topic)
				//log.Println(message.Partition)
				//log.Println(message.Messages)
			},
		})

		defer unsubscribe()

		if err != nil {
			log.Fatal(err)
		}

		time.Sleep(1000 * time.Second)
	}()

	go func() {
		time.Sleep(5 * time.Second)

		err, unsubscribe := gafka.Subscribe(ctx, lib.SubscribeConf{
			Topic: topic,
			Group: "group1",
			Handler: func(message lib.ReceiveMessage) {
				//log.Println("Third")
				//log.Println(message.Topic)
				//log.Println(message.Partition)
				//log.Println(message.Messages)
			},
		})

		defer unsubscribe()

		if err != nil {
			log.Fatal(err)
		}

		time.Sleep(1000 * time.Second)
	}()

	go func() {
		time.Sleep(7 * time.Second)

		err, unsubscribe := gafka.Subscribe(ctx, lib.SubscribeConf{
			Topic: topic,
			Group: "group1",
			Handler: func(message lib.ReceiveMessage) {
				//log.Println("Four")
				//log.Println(message.Topic)
				//log.Println(message.Partition)
				//log.Println(message.Messages)
			},
		})

		defer unsubscribe()

		if err != nil {
			log.Fatal(err)
		}

		time.Sleep(1000 * time.Second)
	}()

	go func() {
		time.Sleep(9 * time.Second)

		err, unsubscribe := gafka.Subscribe(ctx, lib.SubscribeConf{
			Topic: topic,
			Group: "group1",
			Handler: func(message lib.ReceiveMessage) {
				//log.Println("Five")
				//log.Println(message.Topic)
				//log.Println(message.Partition)
				//log.Println(message.Messages)
			},
		})

		defer unsubscribe()

		if err != nil {
			log.Fatal(err)
		}

		time.Sleep(1000 * time.Second)
	}()

	go func() {
		time.Sleep(11 * time.Second)

		err, unsubscribe := gafka.Subscribe(ctx, lib.SubscribeConf{
			Topic: topic,
			Group: "group1",
			Handler: func(message lib.ReceiveMessage) {
				//log.Println("Six")
				//log.Println(message.Topic)
				//log.Println(message.Partition)
				//log.Println(message.Messages)
			},
		})

		defer unsubscribe()

		if err != nil {
			log.Fatal(err)
		}

		time.Sleep(1000 * time.Second)
	}()

	go func() {
		time.Sleep(13 * time.Second)

		err, unsubscribe := gafka.Subscribe(ctx, lib.SubscribeConf{
			Topic: topic,
			Group: "group1",
			Handler: func(message lib.ReceiveMessage) {
				//log.Println("Seven")
				//log.Println(message.Topic)
				//log.Println(message.Partition)
				//log.Println(message.Messages)
			},
		})

		defer unsubscribe()

		if err != nil {
			log.Fatal(err)
		}

		time.Sleep(1000 * time.Second)
	}()

	for i := 0; i <= 500; i++ {
		if err := gafka.Publish(topic, fmt.Sprintf("message #%d", i)); err != nil {
			fmt.Println(err)
		}

		time.Sleep(100 * time.Millisecond)
	}

	// todo gafka.Wait() - for simple
	// or advanced
	// go func() {
	//    if err := gafka.Listen(); err != nil {
	// 		...
	//    }
	// }()
	//
	// gafka.WaitSysNotify() with gafka.Shutdown()
	time.Sleep(1000 * time.Second)
}
