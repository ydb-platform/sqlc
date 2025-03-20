package ydb

import "github.com/sqlc-dev/sqlc/internal/sql/catalog"

func newTestCatalog() *catalog.Catalog {
	return catalog.New("main")
}
