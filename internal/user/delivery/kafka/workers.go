package kafka

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/avast/retry-go"
	"github.com/segmentio/kafka-go"
	"kafka/internal/models"
)

const (
	retryAttempts = 1
	retryDelay    = 1 * time.Second
)

func (ucg *UsersConsumerGroup) createUserWorker(
	ctx context.Context,
	cancel context.CancelFunc,
	r *kafka.Reader,
	w *kafka.Writer,
	wg *sync.WaitGroup,
	workerID int,
) {
	defer wg.Done()
	defer cancel()

	for {
		// Читаем из кафки
		m, err := r.FetchMessage(ctx)
		if err != nil {
			ucg.log.Error().Err(err).Msg("FetchMessage")
			return
		}

		// смотрим что и откуда вычитали
		ucg.log.Info().Msgf(
			"WORKER: %v, message at topic/partition/offset %v/%v/%v: %s = %s\n",
			workerID,
			m.Topic,
			m.Partition,
			m.Offset,
			string(m.Key),
			string(m.Value),
		)

		var user models.User

		if err := json.Unmarshal(m.Value, &user); err != nil {
			ucg.log.Error().Err(err).Msg("json.Unmarshal")
			continue
		}

		// проверяем валидность
		if err := ucg.validate.StructCtx(ctx, user); err != nil {
			ucg.log.Error().Err(err).Msg("validate.StructCtx")
			continue
		}

		// пробуем создать объект (записать в бд)
		if err := retry.Do(func() error {
			created, err := ucg.userService.Create(ctx, &user)
			if err != nil {
				return err
			}
			ucg.log.Info().Msgf("created user: %v", created)
			return nil
		},
			retry.Attempts(retryAttempts),
			retry.Delay(retryDelay),
			retry.Context(ctx),
		); err != nil {
			if err := ucg.publishErrorMessage(ctx, w, m, err); err != nil {
				ucg.log.Error().Err(err).Msgf("publishErrorMessage")
				continue
			}
			ucg.log.Error().Err(err).Msg("userService.Create.publishErrorMessage")
			continue
		}

		if err := r.CommitMessages(ctx, m); err != nil {
			ucg.log.Error().Err(err).Msg("CommitMessages")
			continue
		}
	}
}

func (ucg *UsersConsumerGroup) updateUserWorker(
	ctx context.Context,
	cancel context.CancelFunc,
	r *kafka.Reader,
	w *kafka.Writer,
	wg *sync.WaitGroup,
	workerID int,
) {
	defer wg.Done()
	defer cancel()

	for {
		m, err := r.FetchMessage(ctx)
		if err != nil {
			ucg.log.Error().Err(err).Msg("FetchMessage")
			return
		}

		ucg.log.Info().Msgf(
			"WORKER: %v, message at topic/partition/offset %v/%v/%v: %s = %s\n",
			workerID,
			m.Topic,
			m.Partition,
			m.Offset,
			string(m.Key),
			string(m.Value),
		)

		var user models.User
		if err := json.Unmarshal(m.Value, &user); err != nil {
			ucg.log.Error().Err(err).Msg("json.Unmarshal")
			continue
		}

		if err := ucg.validate.StructCtx(ctx, user); err != nil {
			ucg.log.Error().Err(err).Msg("validate.StructCtx")
			continue
		}

		if err := retry.Do(func() error {
			updated, err := ucg.userService.Update(ctx, &user)
			if err != nil {
				return err
			}
			ucg.log.Debug().Msgf("updated user: %v", updated)
			return nil
		},
			retry.Attempts(retryAttempts),
			retry.Delay(retryDelay),
			retry.Context(ctx),
		); err != nil {

			if err := ucg.publishErrorMessage(ctx, w, m, err); err != nil {
				ucg.log.Error().Err(err).Msg("publishErrorMessage")
				continue
			}
			ucg.log.Error().Err(err).Msg("userService.Create.publishErrorMessage")
			continue
		}

		if err := r.CommitMessages(ctx, m); err != nil {
			ucg.log.Error().Err(err).Msg("CommitMessages")
			continue
		}
	}
}

func (ucg *UsersConsumerGroup) deadLettersWorker(
	ctx context.Context,
	cancel context.CancelFunc,
	r *kafka.Reader,
	w *kafka.Writer,
	wg *sync.WaitGroup,
	workerID int,
) {
	defer wg.Done()
	defer cancel()

	for {
		m, err := r.FetchMessage(ctx)
		if err != nil {
			ucg.log.Error().Err(err).Msg("FetchMessage")
			return
		}

		ucg.log.Info().Msgf(
			"WORKER: %v, message at topic/partition/offset %v/%v/%v: %s = %s\n",
			workerID,
			m.Topic,
			m.Partition,
			m.Offset,
			string(m.Key),
			string(m.Value),
		)

		if err := r.CommitMessages(ctx, m); err != nil {
			ucg.log.Error().Err(err).Msg("CommitMessages")
			continue
		}
	}
}
