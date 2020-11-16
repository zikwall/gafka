package core

import "errors"

func (gf *GafkaEmitter) CreateTopic(topic Topic) error {
	if topic.Name == "" {
		return errors.New("Topic name can not be empty")
	}

	if topic.Partitions == 0 {
		topic.Partitions = 1
	}

	gf.mu.Lock()
	defer gf.mu.Unlock()

	if _, exist := gf.topics[topic.Name]; exist {
		return errors.New("Topic already exist")
	}

	if err := gf.storage.NewTopic(topic.Name, topic.Partitions); err != nil {
		return err
	}

	gf.changeNotifier[topic.Name] = make(chan consumerChangeNotifier)
	gf.topics[topic.Name] = topic.Partitions
	gf.offsets[topic.Name] = map[string]map[int]uint64{}
	gf.messagePools[topic.Name] = make(chan string)
	gf.consumers[topic.Name] = map[string]map[int][]int{}
	gf.freePartitions[topic.Name] = map[string]map[int]int{}

	gf.makeListener(topic.Name, topic.Partitions)
	gf.makeCoordinator(topic.Name, topic.Partitions)

	return nil
}

// todo
// topic list
// topic describe
// topic stats
// consumer list
// describe consumer group
// delete topic
// delete consumer group
// count messages topic
