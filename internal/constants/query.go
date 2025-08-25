package constants

// Flags
const (
	QueryFlagParam              = "@param"
	QueryFlagSqlcVetDisable     = "@sqlc-vet-disable"
	QueryFlagYDBRetryIdempotent = "@ydb-retry-idempotent"
	QueryFlagYDBWithTx          = "@ydb-with-tx"
	QueryFlagYDBBudget          = "@ydb-budget"
	QueryFlagDirectTx           = "@ydb-direct-tx"
)

// Rules
const (
	QueryRuleDbPrepare = "sqlc/db-prepare"
)
