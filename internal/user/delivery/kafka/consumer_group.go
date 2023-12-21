package kafka

import (
	"context"
	"encoding/json"
	"kafka/config"
	"kafka/internal/models"
	"kafka/internal/user"
	"sync"

	"github.com/go-playground/validator/v10"
	"github.com/rs/zerolog"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/compress"
)

// UsersConsumerGroup struct
type UsersConsumerGroup struct {
	Brokers     []string
	GroupID     string
	log         *zerolog.Logger
	cfg         *config.Config
	userService user.Service
	validate    *validator.Validate
}

// NewUsersConsumerGroup constructor
func NewUsersConsumerGroup(
	log *zerolog.Logger,
	cfg *config.Config,
	userService user.Service,
	validate *validator.Validate,
) *UsersConsumerGroup {
	return &UsersConsumerGroup{
		Brokers:     cfg.KafkaBrokers,
		GroupID:     cfg.KafkaConsumerGroup,
		log:         log,
		cfg:         cfg,
		userService: userService,
		validate:    validate,
	}
}

func (ucg *UsersConsumerGroup) getNewKafkaReader(kafkaURL []string, topic, groupID string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:                kafkaURL,
		GroupID:                groupID,
		Topic:                  topic,
		MinBytes:               minBytes,
		MaxBytes:               maxBytes,
		QueueCapacity:          queueCapacity,
		HeartbeatInterval:      heartbeatInterval,
		CommitInterval:         commitInterval,
		PartitionWatchInterval: partitionWatchInterval,
		//		Logger:                 kafka.LoggerFunc(ucg.log.Printf),
		ErrorLogger: kafka.LoggerFunc(ucg.log.Printf),
		MaxAttempts: maxAttempts,
		Dialer: &kafka.Dialer{
			Timeout: dialTimeout,
		},
	})
}

func (ucg *UsersConsumerGroup) getNewKafkaWriter(topic string) *kafka.Writer {
	w := &kafka.Writer{
		Addr:         kafka.TCP(ucg.Brokers...),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: writerRequiredAcks,
		MaxAttempts:  writerMaxAttempts,
		//		Logger:       kafka.LoggerFunc(ucg.log.Printf),
		ErrorLogger:  kafka.LoggerFunc(ucg.log.Printf),
		Compression:  compress.Snappy,
		ReadTimeout:  writerReadTimeout,
		WriteTimeout: writerWriteTimeout,
	}
	return w
}

func (ucg *UsersConsumerGroup) consumeCreateUser(
	ctx context.Context,
	cancel context.CancelFunc,
	groupID string,
	topic string,
	workersNum int,
) {
	r := ucg.getNewKafkaReader(ucg.Brokers, topic, groupID)
	defer cancel()
	defer func() {
		if err := r.Close(); err != nil {
			ucg.log.Error().Err(err).Msg("r.Close")
			cancel()
		}
	}()

	w := ucg.getNewKafkaWriter(ucg.cfg.DeadLetterQueueTopic)
	defer func() {
		if err := w.Close(); err != nil {
			ucg.log.Error().Err(err).Msg("w.Close")
			cancel()
		}
	}()

	ucg.log.Info().Msgf("Starting consumer group: %v", r.Config().GroupID)

	wg := &sync.WaitGroup{}
	for i := 0; i <= workersNum; i++ {
		wg.Add(1)
		go ucg.createUserWorker(ctx, cancel, r, w, wg, i)
	}
	wg.Wait()
}

func (ucg *UsersConsumerGroup) consumeUpdateUser(
	ctx context.Context,
	cancel context.CancelFunc,
	groupID string,
	topic string,
	workersNum int,
) {
	r := ucg.getNewKafkaReader(ucg.Brokers, topic, groupID)
	defer cancel()
	defer func() {
		if err := r.Close(); err != nil {
			ucg.log.Error().Err(err).Msg("r.Close")
			cancel()
		}
	}()

	w := ucg.getNewKafkaWriter(ucg.cfg.DeadLetterQueueTopic)
	defer func() {
		if err := w.Close(); err != nil {
			ucg.log.Error().Err(err).Msg("w.Close")
			cancel()
		}
	}()

	ucg.log.Info().Msgf("Starting consumer group: %v", r.Config().GroupID)

	wg := &sync.WaitGroup{}
	for i := 0; i <= workersNum; i++ {
		wg.Add(1)
		go ucg.updateUserWorker(ctx, cancel, r, w, wg, i)
	}
	wg.Wait()
}

func (ucg *UsersConsumerGroup) consumeDeadLetters(
	ctx context.Context,
	cancel context.CancelFunc,
	groupID string,
) {
	r := ucg.getNewKafkaReader(ucg.Brokers, ucg.cfg.DeadLetterQueueTopic, groupID)
	defer cancel()
	defer func() {
		if err := r.Close(); err != nil {
			ucg.log.Error().Err(err).Msg("r.Close")
			cancel()
		}
	}()

	ucg.log.Info().Msgf("Starting consumer group: %v", r.Config().GroupID)

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go ucg.deadLettersWorker(ctx, cancel, r, nil, wg, 0)
	wg.Wait()
}

func (ucg *UsersConsumerGroup) publishErrorMessage(ctx context.Context, w *kafka.Writer, m kafka.Message, err error) error {
	errMsg := &models.ErrorMessage{
		Offset:    m.Offset,
		Error:     err.Error(),
		Time:      m.Time.UTC(),
		Partition: m.Partition,
		Data:      string(m.Value),
		Topic:     m.Topic,
	}

	errMsgBytes, err := json.Marshal(errMsg)
	if err != nil {
		return err
	}

	return w.WriteMessages(ctx, kafka.Message{
		Value: errMsgBytes,
	})
}

// RunConsumers run kafka consumers
func (ucg *UsersConsumerGroup) RunConsumers(ctx context.Context, cancel context.CancelFunc, ConsumerGroup string) {
	go ucg.consumeCreateUser(ctx, cancel, ConsumerGroup, createUserTopic, createUserWorkers)
	go ucg.consumeUpdateUser(ctx, cancel, ConsumerGroup, updateUserTopic, updateUserWorkers)
	go ucg.consumeDeadLetters(ctx, cancel, ConsumerGroup)
}
