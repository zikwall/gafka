package core

import (
	"fmt"
	"testing"
)

func TestNewInMemoryStorageSharded(t *testing.T) {
	inMemorySharded := NewInMemoryStorageSharded()

	t.Run("it should be common test", func(t *testing.T) {
		inMemorySharded.CreateShard("test", 4)
		topic, exist := inMemorySharded.GetTopic("test")

		if !exist {
			t.Fatal("Topic not exist")
		}

		if !inMemorySharded.HasTopic("test") {
			t.Fatal("Topic not exist")
		}

		if inMemorySharded.HasTopic("test_no_no_no") {
			t.Fatal("Topic exist, expect not exist")
		}

		if err := inMemorySharded.InitTopic("test", 4); err == nil {
			t.Fatal("Expect error")
		}

		messages, exist := topic.GetPartition(3)

		if !exist {
			t.Fatal("Partition not exist")
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

		if inMemorySharded.PeekPartitionLength("test", 2) != 3 {
			t.Fatal("Error expect 3 messages")
		}

		if inMemorySharded.PeekPartitionLength("test", 1) != 1 {
			t.Fatal("Error expect 1 message")
		}

		if inMemorySharded.PeekPartitionLength("test", 3) != 1 {
			t.Fatal("Error expect 1 message")
		}

		if inMemorySharded.PeekPartitionLength("test", 4) != 1 {
			t.Fatal("Error expect 1 message")
		}

		topic.PushBack(4, "message_2")

		if inMemorySharded.PeekPartitionLength("test", 4) != 2 {
			t.Fatal("Error expect 2 message")
		}
	})

	t.Run("it shoul be common test by another topic", func(t *testing.T) {
		inMemorySharded.CreateShard("another_topic", 6)

		if !inMemorySharded.HasTopic("another_topic") {
			t.Fatal("Topic not exist")
		}

		if err := inMemorySharded.InitTopic("another_topic", 4); err == nil {
			t.Fatal("Expect error")
		}

		if inMemorySharded.PeekPartitionLength("another_topic", 2) != 0 {
			t.Fatal("Error expect 0 messages")
		}

		topic, exist := inMemorySharded.GetTopic("another_topic")

		if !exist {
			t.Fatal("Topic not exist")
		}

		topic.PushBack(3, "message_1")
		topic.PushBack(3, "message_2")
		topic.PushBack(3, "message_3")

		if inMemorySharded.PeekPartitionLength("another_topic", 3) != 3 {
			t.Fatal("Error expect 3 messages")
		}

		if inMemorySharded.PeekPartitionLength("another_topic", 2) != 0 {
			t.Fatal("Error expect 0 messages")
		}

		for _, in := range inMemorySharded {
			fmt.Println(in)
		}
	})
}
