// +build unit

package core

import (
	"fmt"
	"testing"
)

func TestNewInMemoryStorage(t *testing.T) {
	inMemory := NewInMemoryStorage()

	t.Run("it should be success create topic", func(t *testing.T) {
		if err := inMemory.NewTopic("test", 4); err != nil {
			t.Fatal(err)
		}

		t.Run("it should be receive error `Topic already exist`", func(t *testing.T) {
			if err := inMemory.NewTopic("test", 4); err == nil {
				t.Fatal("I expected to get an error creating a topic")
			}
		})

		if _, ok := inMemory.messages["test"]; !ok {
			t.Fatal("Error, topic `test` is not created")
		}

		if len(inMemory.messages["test"]) < 4 {
			t.Fatal("Get incorrect data, expected to receive 4 partitions")
		}
	})

	t.Run("it should be stored 10 messages", func(t *testing.T) {
		for i := 0; i < 4; i++ {
			inMemory.Write("test", 1, fmt.Sprintf("test_message_%d", i))
		}

		for i := 0; i < 6; i++ {
			inMemory.Write("test", 2, fmt.Sprintf("test_message_%d", i))
		}

		all := 0

		for _, partition := range inMemory.messages["test"] {
			all += len(partition)
		}

		if all != 10 {
			t.Fatal("Get incorrect data, expected to receive 10 messages")
		}
	})

	t.Run("it should be receive 4 and 6 messages from first and second partitions", func(t *testing.T) {
		if inMemory.PeekLength("test", 1) != 4 {
			t.Fatal("Get incorrect data, expected to receive 4 messages from first partition")
		}

		if inMemory.PeekLength("test", 2) != 6 {
			t.Fatal("Get incorrect data, expected to receive 6 messages from second partition")
		}
	})

	t.Run("it should be success peek messages", func(t *testing.T) {
		messages := inMemory.PeekOffset("test", 1, 0, 2)

		if len(messages) != 2 {
			t.Fatal("Get incorrect data, expected to receive 2 messages from first partition")
		}

		if messages[0] != "test_message_0" || messages[1] != "test_message_1" {
			t.Fatal("Get incorrect data")
		}
	})
}
