package models

import "time"

// ErrorMessage is ...
type ErrorMessage struct {
	MessageID string    `json:"messageId"`
	Offset    int64     `json:"offset"`
	Partition int       `json:"partition"`
	Topic     string    `json:"topic"`
	Data      string    `json:"data"`
	Error     string    `json:"error"`
	Time      time.Time `json:"time"`
}

// Validation Error
// {"userId": 0, "name": "", "surname": "", "createdAt": "2022-01-01T10:30:00Z", "updatedAt": "2022-01-01T15:45:00Z"}

// Unmarshal Error
// {rId": 0, "name": "", "surname": "", "createdAt": "2022-01-01T10:30:00Z", "updatedAt": "2022-01-01T15:45:"

// Correct
// {"userId": 2, "name": "John", "surname": "Smith", "createdAt": "2022-01-01T10:30:00Z", "updatedAt": "2022-01-01T15:45:00Z"}
