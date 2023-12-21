package main

import (
	"github.com/rs/zerolog"
	"kafka/config"
	"kafka/internal/server"
	"kafka/pkg/kafka"
	"os"
)

func main() {
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	cfg, err := config.New()
	if err != nil {
		logger.Fatal().Err(err).Msg("config")
	}

	logger.Info().Msg("Starting products microservice")

	conn, err := kafka.NewKafkaConn(cfg)
	if err != nil {
		logger.Fatal().Err(err).Msg("NewKafkaConn")
	}
	defer conn.Close()

	brokers, err := conn.Brokers()
	if err != nil {
		logger.Fatal().Err(err).Msg("conn.Brokers")
	}
	logger.Info().Msgf("Kafka connected: %v", brokers)

	s := server.NewServer(logger, cfg)
	err = s.Run()
	if err != nil {
		logger.Fatal().Err(err)
	}
}
