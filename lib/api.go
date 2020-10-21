package lib

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

	gf.UNSAFE_CreateTopic(topic)

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
