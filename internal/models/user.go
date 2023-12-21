package models

import (
	"time"
)

// User model.
type User struct {
	ID        int       `json:"userId"`
	Name      string    `json:"name,omitempty" validate:"required,min=3,max=250"`
	Surname   string    `json:"surname,omitempty" validate:"required,min=3,max=250"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}
