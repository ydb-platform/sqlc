// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.29.0
// source: query.sql

package querytest

import (
	"context"
	"database/sql"
)

const deleteBarByID = `-- name: DeleteBarByID :execresult
DELETE FROM bar WHERE id = $1
`

func (q *Queries) DeleteBarByID(ctx context.Context, id int32) (sql.Result, error) {
	return q.db.ExecContext(ctx, deleteBarByID, id)
}
