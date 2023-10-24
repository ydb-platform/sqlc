// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.23.0
// source: query.sql

package querytest

import (
	"context"
)

const subqueryCalcColumn = `-- name: SubqueryCalcColumn :many
SELECT sum FROM (SELECT a + b AS sum FROM foo) AS f
`

func (q *Queries) SubqueryCalcColumn(ctx context.Context) ([]int64, error) {
	rows, err := q.db.QueryContext(ctx, subqueryCalcColumn)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []int64
	for rows.Next() {
		var sum int64
		if err := rows.Scan(&sum); err != nil {
			return nil, err
		}
		items = append(items, sum)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}
