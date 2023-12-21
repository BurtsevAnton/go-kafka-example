package config

import (
	"github.com/gobuffalo/envy"
	"github.com/kelseyhightower/envconfig"
	"github.com/pkg/errors"
)

// Config is reflecting an app configuration.
type Config struct {
	KafkaBrokers       []string `envconfig:"KAFKA_CONSUMER_BROKERS" required:"true"`
	KafkaConsumerGroup string   `envconfig:"KAFKA_CONSUMER_GROUP" default:"kbt"`

	// Топик "dead-letter-queue" в Kafka обычно используется для обработки устаревших,
	// ошибочных или "мертвых" сообщений (dead-letter messages),
	// которые не удалось обработать в основном потоке обработки сообщений.
	//
	// Когда сообщение не может быть обработано из-за ошибки или других причин,
	// оно может быть перенаправлено в топик "dead-letter-queue" для дальнейшего
	// анализа и обработки. Это позволяет изолировать проблемные сообщения и
	// принимать решения относительно их дальнейшей обработки, например,
	// анализировать ошибки или переотправлять их для повторной обработки.
	DeadLetterQueueTopic string `envconfig:"DEAD_LETTER_QUEUE_TOPIC" default:"dead-letter-queue"`
}

// New returns an instance of config.
func New() (*Config, error) {
	var conf Config

	err := envy.Load()
	if err != nil {
		return nil, err
	}

	if err := envconfig.Process("", &conf); err != nil {
		return nil, errors.Wrap(err, "failed to config process")
	}

	return &conf, nil
}
