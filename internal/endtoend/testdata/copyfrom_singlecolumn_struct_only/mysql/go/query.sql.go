// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.29.0
// source: query.sql

package querytest

import (
	"database/sql"
)

const insertSingleValue = `-- name: InsertSingleValue :copyfrom
INSERT INTO foo (a) VALUES (?)
`

type InsertSingleValueParams struct {
	A sql.NullString
}
