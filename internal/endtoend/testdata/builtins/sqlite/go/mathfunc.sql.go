// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.23.0
// source: mathfunc.sql

package querytest

import (
	"context"
)

const getAcos = `-- name: GetAcos :one
select acos(1.0)
`

func (q *Queries) GetAcos(ctx context.Context) (float64, error) {
	row := q.db.QueryRowContext(ctx, getAcos)
	var acos float64
	err := row.Scan(&acos)
	return acos, err
}

const getAcosh = `-- name: GetAcosh :one
select acosh(1.0)
`

func (q *Queries) GetAcosh(ctx context.Context) (float64, error) {
	row := q.db.QueryRowContext(ctx, getAcosh)
	var acosh float64
	err := row.Scan(&acosh)
	return acosh, err
}

const getAsin = `-- name: GetAsin :one
select asin(1.0)
`

func (q *Queries) GetAsin(ctx context.Context) (float64, error) {
	row := q.db.QueryRowContext(ctx, getAsin)
	var asin float64
	err := row.Scan(&asin)
	return asin, err
}

const getAsinh = `-- name: GetAsinh :one
select asinh(1.0)
`

func (q *Queries) GetAsinh(ctx context.Context) (float64, error) {
	row := q.db.QueryRowContext(ctx, getAsinh)
	var asinh float64
	err := row.Scan(&asinh)
	return asinh, err
}

const getAtan = `-- name: GetAtan :one
select atan(1.0)
`

func (q *Queries) GetAtan(ctx context.Context) (float64, error) {
	row := q.db.QueryRowContext(ctx, getAtan)
	var atan float64
	err := row.Scan(&atan)
	return atan, err
}

const getAtan2 = `-- name: GetAtan2 :one
select atan2(1.0, 0.5)
`

func (q *Queries) GetAtan2(ctx context.Context) (float64, error) {
	row := q.db.QueryRowContext(ctx, getAtan2)
	var atan2 float64
	err := row.Scan(&atan2)
	return atan2, err
}

const getAtanh = `-- name: GetAtanh :one
select atanh(1.0)
`

func (q *Queries) GetAtanh(ctx context.Context) (float64, error) {
	row := q.db.QueryRowContext(ctx, getAtanh)
	var atanh float64
	err := row.Scan(&atanh)
	return atanh, err
}

const getCeil = `-- name: GetCeil :one
select ceil(1.0)
`

func (q *Queries) GetCeil(ctx context.Context) (int64, error) {
	row := q.db.QueryRowContext(ctx, getCeil)
	var ceil int64
	err := row.Scan(&ceil)
	return ceil, err
}

const getCeilin = `-- name: GetCeilin :one
select ceiling(1.0)
`

func (q *Queries) GetCeilin(ctx context.Context) (int64, error) {
	row := q.db.QueryRowContext(ctx, getCeilin)
	var ceiling int64
	err := row.Scan(&ceiling)
	return ceiling, err
}

const getCos = `-- name: GetCos :one
select cos(1.0)
`

func (q *Queries) GetCos(ctx context.Context) (float64, error) {
	row := q.db.QueryRowContext(ctx, getCos)
	var cos float64
	err := row.Scan(&cos)
	return cos, err
}

const getCosh = `-- name: GetCosh :one
select cosh(1.0)
`

func (q *Queries) GetCosh(ctx context.Context) (float64, error) {
	row := q.db.QueryRowContext(ctx, getCosh)
	var cosh float64
	err := row.Scan(&cosh)
	return cosh, err
}

const getDegrees = `-- name: GetDegrees :one
select degrees(1.0)
`

func (q *Queries) GetDegrees(ctx context.Context) (float64, error) {
	row := q.db.QueryRowContext(ctx, getDegrees)
	var degrees float64
	err := row.Scan(&degrees)
	return degrees, err
}

const getExp = `-- name: GetExp :one
select exp(1.0)
`

func (q *Queries) GetExp(ctx context.Context) (float64, error) {
	row := q.db.QueryRowContext(ctx, getExp)
	var exp float64
	err := row.Scan(&exp)
	return exp, err
}

const getFloor = `-- name: GetFloor :one
select floor(1.0)
`

func (q *Queries) GetFloor(ctx context.Context) (int64, error) {
	row := q.db.QueryRowContext(ctx, getFloor)
	var floor int64
	err := row.Scan(&floor)
	return floor, err
}

const getLn = `-- name: GetLn :one
select ln(1.0)
`

func (q *Queries) GetLn(ctx context.Context) (float64, error) {
	row := q.db.QueryRowContext(ctx, getLn)
	var ln float64
	err := row.Scan(&ln)
	return ln, err
}

const getLog = `-- name: GetLog :one
select log(1.0)
`

func (q *Queries) GetLog(ctx context.Context) (float64, error) {
	row := q.db.QueryRowContext(ctx, getLog)
	var log float64
	err := row.Scan(&log)
	return log, err
}

const getLog10 = `-- name: GetLog10 :one
select log10(1.0)
`

func (q *Queries) GetLog10(ctx context.Context) (float64, error) {
	row := q.db.QueryRowContext(ctx, getLog10)
	var log10 float64
	err := row.Scan(&log10)
	return log10, err
}

const getLog2 = `-- name: GetLog2 :one
select log2(1.0)
`

func (q *Queries) GetLog2(ctx context.Context) (float64, error) {
	row := q.db.QueryRowContext(ctx, getLog2)
	var log2 float64
	err := row.Scan(&log2)
	return log2, err
}

const getLogBase = `-- name: GetLogBase :one
select log(1.0, 2.0)
`

func (q *Queries) GetLogBase(ctx context.Context) (float64, error) {
	row := q.db.QueryRowContext(ctx, getLogBase)
	var log float64
	err := row.Scan(&log)
	return log, err
}

const getMod = `-- name: GetMod :one
select mod(1, 2)
`

func (q *Queries) GetMod(ctx context.Context) (float64, error) {
	row := q.db.QueryRowContext(ctx, getMod)
	var mod float64
	err := row.Scan(&mod)
	return mod, err
}

const getPi = `-- name: GetPi :one
select pi()
`

func (q *Queries) GetPi(ctx context.Context) (float64, error) {
	row := q.db.QueryRowContext(ctx, getPi)
	var pi float64
	err := row.Scan(&pi)
	return pi, err
}

const getPow = `-- name: GetPow :one
select pow(1, 2)
`

func (q *Queries) GetPow(ctx context.Context) (float64, error) {
	row := q.db.QueryRowContext(ctx, getPow)
	var pow float64
	err := row.Scan(&pow)
	return pow, err
}

const getPower = `-- name: GetPower :one
select power(1, 2)
`

func (q *Queries) GetPower(ctx context.Context) (float64, error) {
	row := q.db.QueryRowContext(ctx, getPower)
	var power float64
	err := row.Scan(&power)
	return power, err
}

const getRadians = `-- name: GetRadians :one
select radians(1)
`

func (q *Queries) GetRadians(ctx context.Context) (float64, error) {
	row := q.db.QueryRowContext(ctx, getRadians)
	var radians float64
	err := row.Scan(&radians)
	return radians, err
}

const getSin = `-- name: GetSin :one
select sin(1.0)
`

func (q *Queries) GetSin(ctx context.Context) (float64, error) {
	row := q.db.QueryRowContext(ctx, getSin)
	var sin float64
	err := row.Scan(&sin)
	return sin, err
}

const getSinh = `-- name: GetSinh :one
select sinh(1.0)
`

func (q *Queries) GetSinh(ctx context.Context) (float64, error) {
	row := q.db.QueryRowContext(ctx, getSinh)
	var sinh float64
	err := row.Scan(&sinh)
	return sinh, err
}

const getSqrt = `-- name: GetSqrt :one
select sqrt(1.0)
`

func (q *Queries) GetSqrt(ctx context.Context) (float64, error) {
	row := q.db.QueryRowContext(ctx, getSqrt)
	var sqrt float64
	err := row.Scan(&sqrt)
	return sqrt, err
}

const getTan = `-- name: GetTan :one
select tan(1.0)
`

func (q *Queries) GetTan(ctx context.Context) (float64, error) {
	row := q.db.QueryRowContext(ctx, getTan)
	var tan float64
	err := row.Scan(&tan)
	return tan, err
}

const getTrunc = `-- name: GetTrunc :one
select trunc(1.0)
`

func (q *Queries) GetTrunc(ctx context.Context) (int64, error) {
	row := q.db.QueryRowContext(ctx, getTrunc)
	var trunc int64
	err := row.Scan(&trunc)
	return trunc, err
}
