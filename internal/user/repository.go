package user

import (
	"context"
	"kafka/internal/models"
)

// PostgreRepository User is ...
type PostgreRepository interface {
	Create(ctx context.Context, user *models.User) (*models.User, error)
	Update(ctx context.Context, user *models.User) (*models.User, error)
}
