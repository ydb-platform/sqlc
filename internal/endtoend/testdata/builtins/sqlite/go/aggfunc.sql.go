// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.23.0
// source: aggfunc.sql

package querytest

import (
	"context"
	"database/sql"
)

const getAvg = `-- name: GetAvg :one
SELECT avg(int_val) FROM test
`

func (q *Queries) GetAvg(ctx context.Context) (sql.NullFloat64, error) {
	row := q.db.QueryRowContext(ctx, getAvg)
	var avg sql.NullFloat64
	err := row.Scan(&avg)
	return avg, err
}

const getCount = `-- name: GetCount :one
SELECT count(*) FROM test
`

func (q *Queries) GetCount(ctx context.Context) (int64, error) {
	row := q.db.QueryRowContext(ctx, getCount)
	var count int64
	err := row.Scan(&count)
	return count, err
}

const getCountId = `-- name: GetCountId :one
SELECT count(id) FROM test
`

func (q *Queries) GetCountId(ctx context.Context) (int64, error) {
	row := q.db.QueryRowContext(ctx, getCountId)
	var count int64
	err := row.Scan(&count)
	return count, err
}

const getGroupConcatInt = `-- name: GetGroupConcatInt :one
SELECT group_concat(int_val) FROM test
`

func (q *Queries) GetGroupConcatInt(ctx context.Context) (string, error) {
	row := q.db.QueryRowContext(ctx, getGroupConcatInt)
	var group_concat string
	err := row.Scan(&group_concat)
	return group_concat, err
}

const getGroupConcatInt2 = `-- name: GetGroupConcatInt2 :one
SELECT group_concat(1, ':') FROM test
`

func (q *Queries) GetGroupConcatInt2(ctx context.Context) (string, error) {
	row := q.db.QueryRowContext(ctx, getGroupConcatInt2)
	var group_concat string
	err := row.Scan(&group_concat)
	return group_concat, err
}

const getGroupConcatText = `-- name: GetGroupConcatText :one
SELECT group_concat(text_val) FROM test
`

func (q *Queries) GetGroupConcatText(ctx context.Context) (string, error) {
	row := q.db.QueryRowContext(ctx, getGroupConcatText)
	var group_concat string
	err := row.Scan(&group_concat)
	return group_concat, err
}

const getGroupConcatText2 = `-- name: GetGroupConcatText2 :one
SELECT group_concat(text_val, ':') FROM test
`

func (q *Queries) GetGroupConcatText2(ctx context.Context) (string, error) {
	row := q.db.QueryRowContext(ctx, getGroupConcatText2)
	var group_concat string
	err := row.Scan(&group_concat)
	return group_concat, err
}

const getMaxInt = `-- name: GetMaxInt :one
SELECT max(int_val) FROM test
`

func (q *Queries) GetMaxInt(ctx context.Context) (interface{}, error) {
	row := q.db.QueryRowContext(ctx, getMaxInt)
	var max interface{}
	err := row.Scan(&max)
	return max, err
}

const getMaxText = `-- name: GetMaxText :one
SELECT max(text_val) FROM test
`

func (q *Queries) GetMaxText(ctx context.Context) (interface{}, error) {
	row := q.db.QueryRowContext(ctx, getMaxText)
	var max interface{}
	err := row.Scan(&max)
	return max, err
}

const getMinInt = `-- name: GetMinInt :one
SELECT min(int_val) FROM test
`

func (q *Queries) GetMinInt(ctx context.Context) (interface{}, error) {
	row := q.db.QueryRowContext(ctx, getMinInt)
	var min interface{}
	err := row.Scan(&min)
	return min, err
}

const getMinText = `-- name: GetMinText :one
SELECT min(text_val) FROM test
`

func (q *Queries) GetMinText(ctx context.Context) (interface{}, error) {
	row := q.db.QueryRowContext(ctx, getMinText)
	var min interface{}
	err := row.Scan(&min)
	return min, err
}

const getSumInt = `-- name: GetSumInt :one
SELECT sum(int_val) FROM test
`

func (q *Queries) GetSumInt(ctx context.Context) (sql.NullFloat64, error) {
	row := q.db.QueryRowContext(ctx, getSumInt)
	var sum sql.NullFloat64
	err := row.Scan(&sum)
	return sum, err
}

const getSumText = `-- name: GetSumText :one
SELECT sum(text_val) FROM test
`

func (q *Queries) GetSumText(ctx context.Context) (sql.NullFloat64, error) {
	row := q.db.QueryRowContext(ctx, getSumText)
	var sum sql.NullFloat64
	err := row.Scan(&sum)
	return sum, err
}

const getTotalInt = `-- name: GetTotalInt :one
SELECT total(int_val) FROM test
`

func (q *Queries) GetTotalInt(ctx context.Context) (float64, error) {
	row := q.db.QueryRowContext(ctx, getTotalInt)
	var total float64
	err := row.Scan(&total)
	return total, err
}
