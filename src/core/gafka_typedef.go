package core

import (
	"context"
	"sync"
	"time"
)

// Задают направления изменений потребителей пришел/ушел
type consumerDirection int

const (
	consumerJoin consumerDirection = 1
	consumerLeft consumerDirection = 0
)

type (
	// Стурктура для отправки оповещений в изменении баланса Потребителей (Consumer)
	consumerChangeNotifier struct {
		topic     string
		group     string
		id        int
		direction consumerDirection
	}
	// Стурктура конфигурации Потребителей (Consumer)
	SubscribeConf struct {
		Topic   string
		Group   string
		Handler func(message ReceiveMessage)
	}
	// Стурктура сигнатуры Топика, его название и количество партиций в нем
	Topic struct {
		Name       string
		Partitions int
	}
	// Структура сообщения, отправляемое в Топик и партицию
	ReceiveMessage struct {
		Topic     string
		Partition int
		Messages  []string
	}
	// Структура основной конфигурации
	Configuration struct {
		BatchSize       uint8
		ReclaimInterval time.Duration
		Topics          []Topic
	}
	// Сама структура Gafka
	GafkaEmitter struct {
		config Configuration
		// Используется для организации синхронизации по основным свойствам
		mu sync.RWMutex
		// Используются для корректного завершения системы
		closed  bool
		context context.Context
		cancel  context.CancelFunc
		done    chan struct{}

		// Используется для ожидания завершения всех слушателей
		// - Topic Consumer Group Coordinator Listener
		// - Partition Coordinator Listener
		wg sync.WaitGroup

		// Свойство хранит все оффсеты для группы потребителей
		// topic:group:partition -> offset for consumer group
		offsets map[string]map[string]map[int]uint64

		// Свойство хранит список всех доступных топиков
		// topic:partitions
		topics map[string]int

		// Свойство хранит всех активных потребителей сообщений
		// topic:group:unique:partitions
		consumers map[string]map[string]map[int][]int

		// topic:group:partition
		// возможно позже получится упростить схему, но пока так
		// суть этой фигни в том, чтобы следить какие РАЗДЕЛЫ свободны и их надя занять
		// и какие разделы собственно уже заняты целиком и полностью, собственно  их тогда тут не будет, кек
		freePartitions map[string]map[string]map[int]int

		// Свойство, задающее пулл сообщений для кажлого топика
		// topic -> message pool
		messagePools map[string]chan string

		// Своство хранит все сообщения
		// topic -> message storage
		// тут надо подумать над интерфейсом MessageStorage
		// например, ДИСК, ПАМЯТЬ, еще хер знает где
		messages map[string]map[int][]string

		// topic:group
		// канал куда отправляются изменения состояний подписчиков для дальнейшей перебалансировки
		changeNotifier map[string]chan consumerChangeNotifier
	}
)
