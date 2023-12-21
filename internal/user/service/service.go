package service

import (
	"context"
	"fmt"
	"github.com/rs/zerolog"
	"kafka/internal/models"
	"kafka/internal/user/delivery/kafka"
	"math/rand"
	"time"
)

// userService
type userService struct {
	//	userRepo     user.PostgreRepository
	log          zerolog.Logger
	userProducer kafka.UsersProducer
}

// NewUserService constructor
func NewUserService(
	//	userRepo user.PostgreRepository,
	log zerolog.Logger,
	userProducer kafka.UsersProducer,
) *userService {
	//	return &userService{userRepo: userRepo, log: log, userProducer: userProducer}
	return &userService{log: log, userProducer: userProducer}
}

// Create creates a new user
func (u *userService) Create(ctx context.Context, user *models.User) (*models.User, error) {
	// ToDo
	// Если случайное число больше 5, то вернем ошибку. Для теста топика Мертвых сообщений
	rnd := rand.New(rand.NewSource(time.Now().UnixNano())).Intn(10)
	if rnd > 5 {
		return nil, fmt.Errorf("some error")
	}

	return &models.User{
		ID:        1,
		Name:      "Tmp",
		Surname:   "STmp",
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}, nil
}

// Update updates an exists user
func (u *userService) Update(ctx context.Context, user *models.User) (*models.User, error) {
	// ToDo
	return &models.User{
		ID:        1,
		Name:      "TmpUpd",
		Surname:   "STmpUpd",
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}, nil
}
