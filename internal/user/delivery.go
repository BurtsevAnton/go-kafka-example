package user

import "github.com/gin-gonic/gin"

// HttpDelivery http delivery
type HttpDelivery interface {
	CreateUser() gin.HandlerFunc
	UpdateUser() gin.HandlerFunc
}
