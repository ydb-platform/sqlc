// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.29.0
// source: query.sql

package querytest

import (
	"context"
)

const joinNoConstraints = `-- name: JoinNoConstraints :many
SELECT foo.barid
FROM foo
CROSS JOIN bar
WHERE bar.id = $2 AND owner = $1
`

type JoinNoConstraintsParams struct {
	Owner string
	ID    int32
}

func (q *Queries) JoinNoConstraints(ctx context.Context, arg JoinNoConstraintsParams) ([]int32, error) {
	rows, err := q.db.Query(ctx, joinNoConstraints, arg.Owner, arg.ID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []int32
	for rows.Next() {
		var barid int32
		if err := rows.Scan(&barid); err != nil {
			return nil, err
		}
		items = append(items, barid)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const joinParamWhereClause = `-- name: JoinParamWhereClause :many
SELECT foo.barid
FROM foo
JOIN bar ON bar.id = $2
WHERE owner = $1
`

type JoinParamWhereClauseParams struct {
	Owner string
	ID    int32
}

func (q *Queries) JoinParamWhereClause(ctx context.Context, arg JoinParamWhereClauseParams) ([]int32, error) {
	rows, err := q.db.Query(ctx, joinParamWhereClause, arg.Owner, arg.ID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []int32
	for rows.Next() {
		var barid int32
		if err := rows.Scan(&barid); err != nil {
			return nil, err
		}
		items = append(items, barid)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const joinWhereClause = `-- name: JoinWhereClause :many
SELECT foo.barid
FROM foo
JOIN bar ON bar.id = barid
WHERE owner = $1
`

func (q *Queries) JoinWhereClause(ctx context.Context, owner string) ([]int32, error) {
	rows, err := q.db.Query(ctx, joinWhereClause, owner)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []int32
	for rows.Next() {
		var barid int32
		if err := rows.Scan(&barid); err != nil {
			return nil, err
		}
		items = append(items, barid)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}
