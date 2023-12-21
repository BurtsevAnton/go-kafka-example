package user

import (
	"context"
	"kafka/internal/models"
)

// Service User is ...
type Service interface {
	Create(ctx context.Context, user *models.User) (*models.User, error)
	Update(ctx context.Context, user *models.User) (*models.User, error)
}
