// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.23.0

package querytest

import (
	"database/sql"
)

type SuperUser struct {
	ID          int32
	FirstName   string
	LastName    sql.NullString
	DateOfBirth sql.NullTime
}

type User struct {
	ID       int32
	LastName sql.NullString
	Age      int32
}
