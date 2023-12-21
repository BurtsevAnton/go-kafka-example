package kafka

import (
	"time"
)

const (
	minBytes               = 10e3 // 10KB
	maxBytes               = 10e6 // 10MB
	queueCapacity          = 100
	heartbeatInterval      = 3 * time.Second
	commitInterval         = 0
	partitionWatchInterval = 5 * time.Second
	maxAttempts            = 3
	dialTimeout            = 3 * time.Minute

	writerReadTimeout  = 10 * time.Second
	writerWriteTimeout = 10 * time.Second
	writerRequiredAcks = -1
	writerMaxAttempts  = 3

	// Названия топиков
	// Эти константы можно вычитывать из .env
	createUserTopic = "create-user"
	updateUserTopic = "update-user"

	// Эти константы могут соответствовать количеству партиций в топике.
	// Это может распределить нагрузку на кафку
	// Эти константы тоже можно вычитывать из .env
	createUserWorkers = 3
	updateUserWorkers = 3
)
