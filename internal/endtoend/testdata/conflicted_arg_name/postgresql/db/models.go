// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.29.0

package db

import (
	"time"

	"github.com/google/uuid"
)

type Foo struct {
	Time  time.Time
	Time2 time.Time
	Uuid  uuid.UUID
	Uuid2 uuid.UUID
}
