// +build unit

package core

import "testing"

func TestResolveBootstrappedTopics(t *testing.T) {
	t.Run("it should be success create topics", func(t *testing.T) {
		t.Run("common step", func(t *testing.T) {
			topics := ResolveBootstrappedTopics("topic1:4, topic2:5, topic3: 2 , topic4 ; 2, topic5 : 4")

			if len(topics) != 4 {
				t.Fatal("Incorrect value received, expected to get 4")
			}

			if topics[1].Name != "topic2" {
				t.Fatal("Incorrect value received, expected to get `topic5`")
			}

			if topics[1].Partitions != 5 {
				t.Fatal("Incorrect value received, expected to get 4")
			}

			if topics[3].Name != "topic5" {
				t.Fatal("Incorrect value received, expected to get `topic5`")
			}

			if topics[3].Partitions != 4 {
				t.Fatal("Incorrect value received, expected to get 4")
			}
		})

		t.Run("with incorrect topics", func(t *testing.T) {
			topics := ResolveBootstrappedTopics("topic4 ; 2,")

			if len(topics) != 0 {
				t.Fatal("Incorrect value received, expected to get 0")
			}
		})

		t.Run("with empty topic list", func(t *testing.T) {
			topics := ResolveBootstrappedTopics("")

			if len(topics) != 0 {
				t.Fatal("Incorrect value received, expected to get 0")
			}

			t.Run("with incorrect topic lists", func(t *testing.T) {
				topics := ResolveBootstrappedTopics(", , , .. , , , ,,:.,")

				if len(topics) != 0 {
					t.Fatal("Incorrect value received, expected to get 0")
				}
			})
		})

		t.Run("it should be create topic with minimal partiton size -> 1", func(t *testing.T) {
			topics := ResolveBootstrappedTopics("topic:0")

			if len(topics) != 1 {
				t.Fatal("Incorrect value received, expected to get 1")
			}

			if topics[0].Partitions != 1 {
				t.Fatal("Incorrect value received, expected to get 1")
			}
		})
	})
}
