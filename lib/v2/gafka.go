package v2

import (
	"context"
	"sync"
	"time"
)

type (
	Topic struct {
		Name       string
		Partitions int
	}

	// тут будем заводить основные конфигурации
	Configuration struct {
		BatchSize       uint8
		ReclaimInterval time.Duration
		Topics          []Topic
	}

	GafkaEmitter struct {
		mu      sync.RWMutex
		closed  bool
		context context.Context
		cancel  context.CancelFunc
		config  Configuration

		// topic:group:unique -> message channel
		consumers map[string]map[string]map[int]chan []string

		// topic:group:partition -> offset for consumer group
		offsets map[string]map[string]map[int]uint64

		// topic:partitions
		topics map[string]int

		// topic:group:partition:unique
		partitionListeners map[string]map[string]map[int][]int

		// topic:group:partition
		// возможно позже получится упростить схему, но пока так
		// суть этой фигни в том, чтобы следить какие РАЗДЕЛЫ свободны и их надя занять
		// и какие разделы собственно уже заняты целиком и полностью, собственно  их тогда тут не будет, кек
		freePartitions map[string]map[string]map[int]int

		// topic -> message pool
		messagePools map[string]chan string

		// topic -> message storage
		// тут надо подумать над интерфейсом MessageStorage
		// например, ДИСК, ПАМЯТЬ, еще хер знает где
		messages map[string]map[int][]string
	}
)

func Gafka(ctx context.Context, c Configuration) *GafkaEmitter {
	gf := new(GafkaEmitter)

	if c.BatchSize == 0 {
		c.BatchSize = 10
	}

	if c.ReclaimInterval == 0 {
		c.ReclaimInterval = time.Second * 1
	}

	gf.config = c
	gf.context, gf.cancel = context.WithCancel(ctx)

	// ну тут конечно полный трешак, нужен конкретный ревью
	// возможно от части можно вообще избавиться
	gf.consumers = map[string]map[string]map[int]chan []string{}
	gf.offsets = map[string]map[string]map[int]uint64{}
	gf.topics = make(map[string]int, len(c.Topics))
	gf.messages = map[string]map[int][]string{}
	gf.messagePools = map[string]chan string{}
	gf.partitionListeners = map[string]map[string]map[int][]int{}
	gf.freePartitions = map[string]map[string]map[int]int{}

	for _, topic := range c.Topics {
		gf.UNSAFE_CreateTopic(topic)
	}

	// надо подумать над целесообразностью запуска в горутине, пока вроде норм
	go gf.initialize()

	return gf
}

// назнача слушаетелей по ТЕМАМ и их РАЗДЕЛАМ
// нужно будет добавить возможность динамического формирования ТЕМ
// gafka.AddTopic("topic_name_here", 10)
func (gf *GafkaEmitter) initialize() {
	for topic, partitions := range gf.topics {
		for partition := 1; partition <= partitions; partition++ {
			gf.createTopicPartitionListener(topic, partition)
		}
	}
}

// Распаралеливаем сообщения ТЕМЫ по разным РАЗДЕЛАМ, каждый раздел работает в своем потоке
// возможно есть варианты получше, надо думать
func (gf *GafkaEmitter) createTopicPartitionListener(topic string, partition int) {
	go func(top string, part int) {
		// для каждого слушателя по своему контексту, образованного от ведущего контекста всей Gafkd
		ctx, cancel := context.WithCancel(gf.context)

		defer func() {
			cancel()

			logln("Cancel topic listener: ", top, " partition: ", part)
		}()

		logln("Make topic listener: ", top, " partition: ", part)

		for {
			select {
			case <-ctx.Done():
				return
			case message := <-gf.messagePools[top]:
				gf.addMessage(top, part, message)
			}
		}

	}(topic, partition)
}

// добавляем сообщение в конкретный РАЗДЕЛ целевой ТЕМЫ
// заменить эту херь интефейсом
func (gf *GafkaEmitter) addMessage(topic string, part int, message string) {
	gf.mu.Lock()
	gf.messages[topic][part] = append(gf.messages[topic][part], message)
	gf.mu.Unlock()
}
