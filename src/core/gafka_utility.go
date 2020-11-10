package core

import (
	"github.com/zikwall/gafka/src/util"
	"strings"
)

func ResolveBootstrappedTopics(list string) []Topic {
	var topics []Topic

	_topics := strings.Split(list, ",")

	for _, _topic := range _topics {
		desc := strings.Split(_topic, ":")

		// only format: TOPIC_NAME:PARTITION_COUNT
		if len(desc) != 2 {
			continue
		}

		name := strings.TrimSpace(desc[0])
		partitions := strings.TrimSpace(desc[1])

		if name == "" {
			continue
		}

		immutablePartitions := util.ImmutableInt(partitions)

		if immutablePartitions == 0 {
			immutablePartitions = 1
		}

		topics = append(topics, Topic{
			Name:       name,
			Partitions: immutablePartitions,
		})
	}

	return topics
}
