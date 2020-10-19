package lib

import (
	"context"
	"math"
	"sync"
	"time"
)

const (
	InConsumer  = 1
	OutConsumer = 0
)

type direction struct {
	topic string
	group string
	id    int
	in    int
}

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

		// topic:group
		// канал куда отправляются изменения состояний подписчиков для дальнейшей перебалансировки
		changeConsumers map[string]chan direction
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
	gf.changeConsumers = map[string]chan direction{}

	for _, topic := range c.Topics {
		gf.UNSAFE_CreateTopic(topic)
	}

	// надо подумать над целесообразностью запуска в горутине, пока вроде норм
	go gf.initialize()
	go gf.coordinator()

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

// назначается слушатель по ТЕМАМ и РАЗДЕЛАМ для возможности пере-/балансировки ПОДПИСЧИКОВ
func (gf *GafkaEmitter) coordinator() {
	for topic, partitions := range gf.topics {
		gf.createTopicCoordinatorListener(topic, partitions)
	}
}

// балансировщик ТЕМ, РАЗДЕЛОВ и шрупп ПОТРЕБИТЕЛЕЙ
func (gf *GafkaEmitter) createTopicCoordinatorListener(topic string, partitions int) {
	go func(t string, parts int) {
		// тут надо подумать ибо есть некоторое дублирование
		// существует еще глобальное состояние ПОДПИСЧИКОВ
		// потом надо будет отредактировать
		consumerInGroup := map[string]map[int]int{}

		for {
			select {
			case <-gf.context.Done():
				return
			case change := <-gf.changeConsumers[t]:
				logln("Обнаружены изменения в балансе подписчиков, начинаю процес РЕБАЛАНСИРОВКИ подписчиков")

				topic := t
				group := change.group
				uniqId := change.id
				partition := parts

				if _, ok := consumerInGroup[group]; !ok {
					consumerInGroup[group] = map[int]int{}
				}

				if change.in == InConsumer {
					consumerInGroup[group][uniqId] = 1
				} else {
					delete(consumerInGroup[group], uniqId)
				}

				gf.mu.Lock()

				// check partition identifiers
				if _, ok := gf.freePartitions[topic][group]; !ok {
					// create new identifiers
					gf.UNSAFE_CreateFreePartitions(topic, group)
				}

				partOneConsumer := float64(partition) / float64(len(consumerInGroup[group]))

				// 4 + 0.5 => 5
				// 3.9 + 0.5 => 4
				need := int(math.Round(partOneConsumer + 0.49))

				gf.partitionListeners[topic][group] = map[int][]int{}
				gf.UNSAFE_CreateFreePartitions(topic, group)

				for consumer := range consumerInGroup[group] {
					for part := range gf.freePartitions[topic][group] {
						if len(gf.partitionListeners[topic][group][consumer]) >= need {
							break
						}

						gf.partitionListeners[topic][group][consumer] = append(gf.partitionListeners[topic][group][consumer], part)
						delete(gf.freePartitions[topic][group], part)
					}
				}

				logln("Подписчик", uniqId, "слушает селудющие РАЗДЕЛЫ", gf.partitionListeners[topic][group][uniqId])

				gf.mu.Unlock()
			}
		}
	}(topic, partitions)
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
