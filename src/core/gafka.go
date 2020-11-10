package core

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

type observer struct {
	topic   string
	group   string
	id      int
	in      int
	channel chan ReceiveMessage
}

type (
	SubscribeConf struct {
		Topic   string
		Group   string
		Handler func(message ReceiveMessage)
	}
	Topic struct {
		Name       string
		Partitions int
	}
	ReceiveMessage struct {
		Topic     string
		Partition int
		Messages  []string
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
		done    chan struct{}
		wg      sync.WaitGroup

		// topic:group:partition -> offset for consumer group
		offsets map[string]map[string]map[int]uint64

		// topic:partitions
		topics map[string]int

		// topic:group:unique:partitions
		consumers map[string]map[string]map[int][]int

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
		observers map[string]chan observer
	}
)

func Gafka(ctx context.Context, c Configuration) *GafkaEmitter {
	gf := new(GafkaEmitter)
	gf.done = make(chan struct{})
	gf.wg = sync.WaitGroup{}

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
	gf.offsets = map[string]map[string]map[int]uint64{}
	gf.topics = make(map[string]int, len(c.Topics))
	gf.messages = map[string]map[int][]string{}
	gf.messagePools = map[string]chan string{}
	gf.consumers = map[string]map[string]map[int][]int{}
	gf.freePartitions = map[string]map[string]map[int]int{}
	gf.observers = map[string]chan observer{}

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
	gf.wg.Add(1)

	go func(t string, parts int) {
		defer gf.wg.Done()

		// тут надо подумать ибо есть некоторое дублирование
		// существует еще глобальное состояние ПОДПИСЧИКОВ
		// потом надо будет отредактировать
		consumerInGroup := map[string]map[int]int{}

		for {
			select {
			case <-gf.context.Done():
				return
			case change := <-gf.observers[t]:
				logln("Обнаружены изменения в балансе подписчиков, начинаю процес РЕБАЛАНСИРОВКИ подписчиков")

				topic := t
				group := change.group
				uniqId := change.id
				partition := parts

				if _, ok := consumerInGroup[group]; !ok {
					consumerInGroup[group] = map[int]int{}
				}

				// Пока только один глобальный лок, надо подумать в целесообразности множества маленьких
				gf.mu.Lock()

				if change.in == InConsumer {
					logln("У нас новый подписчик для ТЕМЫ", topic, "это чудо входит в группу", group, "его уникальный айди", uniqId)

					consumerInGroup[group][uniqId] = 1

					if _, ok := gf.offsets[topic][group]; !ok {
						gf.offsets[topic][group] = map[int]uint64{}
					}

					if _, ok := gf.consumers[topic][group]; !ok {
						gf.consumers[topic][group] = map[int][]int{}
					}

					if _, ok := gf.freePartitions[topic][group]; !ok {
						gf.UNSAFE_CreateFreePartitions(topic, group)
					}

				} else {
					delete(consumerInGroup[group], uniqId)

					logln("Подписчик ушел с ТЕМЫ", topic, "и группы", group, "его айди был", uniqId)

					// todo эта хрень вообще не нужна, вообще
					// удаляем все прослушиваемые РАЗДЕЛЫ целевой ТЕМЫ, где был замечен этот хитрый слушатель
					for _, part := range gf.consumers[topic][group][uniqId] {
						gf.freePartitions[topic][group][part] = 1
						logln("Метим РАЗДЕЛ", part, "для группы", group, "свободным")
						delete(gf.consumers[topic][group], uniqId)
						logln("Удаляем слушаетля", uniqId, "из группы", group, "и раздела", part)
					}
				}

				partOneConsumer := float64(partition) / float64(len(consumerInGroup[group]))

				// todo нужен более продвинутый алгоритм для равномерно-плавного распределения
				// потребителей по разделам топика, что они слушают
				// 4 + 0.5 => 5
				// 3.9 + 0.5 => 4
				need := int(math.Round(partOneConsumer + 0.49))

				gf.UNSAFE_FlushConsumerPartitions(topic, group)
				gf.UNSAFE_CreateFreePartitions(topic, group)

				for consumer := range consumerInGroup[group] {
					for part := range gf.freePartitions[topic][group] {
						if len(gf.consumers[topic][group][consumer]) >= need {
							break
						}

						gf.UNSAFE_LinkConsumerToPartiton(topic, group, consumer, part)
						gf.UNSAFE_TakePartition(topic, group, part)
					}
				}

				if change.in == InConsumer {
					logln("Подписчик", uniqId, "слушает селудющие РАЗДЕЛЫ", gf.consumers[topic][group][uniqId])
				}

				logln("Новый порядок таков", gf.consumers[topic][group])

				gf.mu.Unlock()
			}
		}
	}(topic, partitions)
}

// Распаралеливаем сообщения ТЕМЫ по разным РАЗДЕЛАМ, каждый раздел работает в своем потоке
// возможно есть варианты получше, надо думать
func (gf *GafkaEmitter) createTopicPartitionListener(topic string, partition int) {
	gf.wg.Add(1)

	go func(top string, part int) {
		// для каждого слушателя по своему контексту, образованного от ведущего контекста всей Gafkd
		ctx, cancel := context.WithCancel(gf.context)

		defer func() {
			cancel()
			gf.wg.Done()

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
