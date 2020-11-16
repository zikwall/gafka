package core

import (
	"fmt"
	"testing"
)

func TestNewInMemoryStorageSharded(t *testing.T) {
	inMemorySharded := NewInMemoryStorageSharded(
		Fnv32Hasher{},
	)

	t.Run("it should be common test", func(t *testing.T) {
		inMemorySharded.CreateShard("test", 4)
		topic, exist := inMemorySharded.GetTopic("test")

		// direct
		if !exist {
			t.Fatal("Failure, the topic was expected to exist")
		}

		if !inMemorySharded.HasTopic("test") {
			t.Fatal("Failure, the topic was expected to exist")
		}

		if inMemorySharded.HasTopic("test_no_no_no") {
			t.Fatal("The failure, it was expected that the topic does not exist")
		}

		if err := inMemorySharded.NewTopic("test", 4); err == nil {
			t.Fatal("Failure, we expected to get an error re-creating the topic")
		}

		messages, exist := topic.GetPartition(3)

		// direct
		if !exist {
			t.Fatal("Failure, it was expected that the partition exists")
		}

		// direct
		if len(messages) != 0 {
			t.Fatal("Expect 0 messages")
		}

		topic.PushBack(2, "message_1")
		topic.PushBack(2, "message_2")
		topic.PushBack(2, "message_3")

		topic.PushBack(1, "message_1")
		topic.PushBack(3, "message_1")
		topic.PushBack(4, "message_1")

		if inMemorySharded.PeekLength("test", 2) != 3 {
			t.Fatal("Failure, expected to receive 3 messages")
		}

		if inMemorySharded.PeekLength("test", 1) != 1 {
			t.Fatal("Failure, expected to receive 1 messages for part `1`")
		}

		if inMemorySharded.PeekLength("test", 3) != 1 {
			t.Fatal("Failure, expected to receive 1 messages for part `3`")
		}

		if inMemorySharded.PeekLength("test", 4) != 1 {
			t.Fatal("Failure, expected to receive 1 messages for part `4`")
		}

		topic.PushBack(4, "message_2")

		if inMemorySharded.PeekLength("test", 4) != 2 {
			t.Fatal("Failure, expected to receive 2 messages for part `4`")
		}

		messages = inMemorySharded.PeekOffset("test", 2, 0, 3)

		if len(messages) != 3 {
			t.Fatal("Failure, expected to receive 3 messages for part `2`")
		}

		if messages[1] != "message_2" {
			t.Fatal("Failure, expected to receive messages `message_2`")
		}
	})

	t.Run("it should be common test by another topic", func(t *testing.T) {
		inMemorySharded.CreateShard("another_topic", 6)

		if !inMemorySharded.HasTopic("another_topic") {
			t.Fatal("Failure, the topic was expected to exist")
		}

		if err := inMemorySharded.NewTopic("another_topic", 4); err == nil {
			t.Fatal("Failure, the topic was expected to exist")
		}

		if inMemorySharded.PeekLength("another_topic", 2) != 0 {
			t.Fatal("Failure, expected to receive 0 messages")
		}

		topic, exist := inMemorySharded.GetTopic("another_topic")

		// direct
		if !exist {
			t.Fatal("Failure, the topic was expected to exist")
		}

		topic.PushBack(3, "message_1")
		topic.PushBack(3, "message_2")
		topic.PushBack(3, "message_3")

		if inMemorySharded.PeekLength("another_topic", 3) != 3 {
			t.Fatal("Failure, expected to receive 3 messages")
		}

		if inMemorySharded.PeekLength("another_topic", 2) != 0 {
			t.Fatal("Failure, expected to receive 0 messages")
		}

		for _, in := range inMemorySharded.shards {
			fmt.Println(in)
		}
	})
}
