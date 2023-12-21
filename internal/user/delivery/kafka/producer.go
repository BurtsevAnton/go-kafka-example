package kafka

import (
	"context"
	"github.com/rs/zerolog"
	"kafka/config"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/compress"
)

// UsersProducer interface
type UsersProducer interface {
	PublishCreate(ctx context.Context, msgs ...kafka.Message) error
	PublishUpdate(ctx context.Context, msgs ...kafka.Message) error
	Close()
	Run()
	GetNewKafkaWriter(topic string) *kafka.Writer
}

type usersProducer struct {
	log          zerolog.Logger
	cfg          *config.Config
	createWriter *kafka.Writer
	updateWriter *kafka.Writer
}

// NewUsersProducer constructor
func NewUsersProducer(log zerolog.Logger, cfg *config.Config) *usersProducer {
	return &usersProducer{log: log, cfg: cfg}
}

// GetNewKafkaWriter Create new kafka writer
func (p *usersProducer) GetNewKafkaWriter(topic string) *kafka.Writer {
	w := &kafka.Writer{
		Addr:         kafka.TCP(p.cfg.KafkaBrokers...),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: writerRequiredAcks,
		MaxAttempts:  writerMaxAttempts,
		//		Logger:       kafka.LoggerFunc(p.log.Printf),
		ErrorLogger:  kafka.LoggerFunc(p.log.Printf),
		Compression:  compress.Snappy,
		ReadTimeout:  writerReadTimeout,
		WriteTimeout: writerWriteTimeout,
	}
	return w
}

// Run init producers writers
func (p *usersProducer) Run() {
	p.createWriter = p.GetNewKafkaWriter(createUserTopic)
	p.updateWriter = p.GetNewKafkaWriter(updateUserTopic)
}

// Close closes writers
func (p *usersProducer) Close() {
	p.createWriter.Close()
	p.updateWriter.Close()
}

// PublishCreate publish messages to create topic
func (p *usersProducer) PublishCreate(ctx context.Context, msgs ...kafka.Message) error {
	return p.createWriter.WriteMessages(ctx, msgs...)
}

// PublishUpdate publish messages to update topic
func (p *usersProducer) PublishUpdate(ctx context.Context, msgs ...kafka.Message) error {
	return p.updateWriter.WriteMessages(ctx, msgs...)
}
