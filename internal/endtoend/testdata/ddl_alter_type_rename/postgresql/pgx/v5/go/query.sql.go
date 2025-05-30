// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.29.0
// source: query.sql

package querytest

import (
	"context"
)

const listAuthors = `-- name: ListAuthors :many
SELECT id, status FROM log_lines
`

func (q *Queries) ListAuthors(ctx context.Context) ([]LogLine, error) {
	rows, err := q.db.Query(ctx, listAuthors)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []LogLine
	for rows.Next() {
		var i LogLine
		if err := rows.Scan(&i.ID, &i.Status); err != nil {
			return nil, err
		}
		items = append(items, i)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}
