package repository

import (
	"context"
	"github.com/jmoiron/sqlx"
	"kafka/internal/models"
)

type userPostgreRepo struct {
	sqlDB *sqlx.DB
}

// NewUserPostgreRepo userPostgreRepo constructor
func NewUserPostgreRepo(sqlDB *sqlx.DB) *userPostgreRepo {
	return &userPostgreRepo{sqlDB: sqlDB}
}

// Create creates a new user
func (p *userPostgreRepo) Create(ctx context.Context, user *models.User) (*models.User, error) {
	// ToDo
	panic("implement me!")
}

// Update updates an exists user
func (p *userPostgreRepo) Update(ctx context.Context, user *models.User) (*models.User, error) {
	// ToDo
	panic("implement me!")
}
