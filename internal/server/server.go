package server

import (
	"context"
	"github.com/go-playground/validator/v10"
	"github.com/rs/zerolog"
	"kafka/config"
	"kafka/internal/user/delivery/kafka"
	"kafka/internal/user/service"
	"os"
	"os/signal"
	"syscall"
)

// server
type server struct {
	log zerolog.Logger
	cfg *config.Config
	//	db  *sqlx.DB
}

// NewServer constructor
func NewServer(log zerolog.Logger, cfg *config.Config) *server {
	return &server{log: log, cfg: cfg}
}

// Run Start server
func (s *server) Run() error {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	validate := validator.New()

	usersProducer := kafka.NewUsersProducer(s.log, s.cfg)
	usersProducer.Run()
	defer usersProducer.Close()

	//	userRepository := repository.NewUserPostgreRepo(s.db)
	//	userService := service.NewUserService(userRepository, s.log, usersProducer)
	userService := service.NewUserService(s.log, usersProducer)

	usersCG := kafka.NewUsersConsumerGroup(&s.log, s.cfg, userService, validate)
	usersCG.RunConsumers(ctx, cancel, s.cfg.KafkaConsumerGroup)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)

	select {
	case v := <-quit:
		s.log.Error().Msgf("signal.Notify: %v", v)
	case done := <-ctx.Done():
		s.log.Error().Msgf("ctx.Done: %v", done)
	}

	return nil
}
