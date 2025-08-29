package local

import (
	"context"
	"fmt"
	"hash/fnv"
	"math/rand"
	"os"
	"testing"
	"time"

	migrate "github.com/sqlc-dev/sqlc/internal/migrations"
	"github.com/sqlc-dev/sqlc/internal/sql/sqlpath"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func YDB(t *testing.T, migrations []string) *ydb.Driver {
	return link_YDB(t, migrations, true, false)
}

func YDBTLS(t *testing.T, migrations []string) *ydb.Driver {
	return link_YDB(t, migrations, true, true)
}

func ReadOnlyYDB(t *testing.T, migrations []string) *ydb.Driver {
	return link_YDB(t, migrations, false, false)
}

func ReadOnlyYDBTLS(t *testing.T, migrations []string) *ydb.Driver {
	return link_YDB(t, migrations, false, true)
}

func link_YDB(t *testing.T, migrations []string, rw bool, tls bool) *ydb.Driver {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dbuiri := os.Getenv("YDB_SERVER_URI")
	if dbuiri == "" {
		t.Skip("YDB_SERVER_URI is empty")
	}

	baseDB := os.Getenv("YDB_DATABASE")
	if baseDB == "" {
		baseDB = "/local"
	}

	var seed []string
	files, err := sqlpath.Glob(migrations)
	if err != nil {
		t.Fatal(err)
	}
	h := fnv.New64()
	for _, f := range files {
		blob, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		h.Write(blob)
		seed = append(seed, migrate.RemoveRollbackStatements(string(blob)))
	}

	var name string
	if rw {
		name = fmt.Sprintf("sqlc_test_%s", "test_new")
	} else {
		name = fmt.Sprintf("sqlc_test_%x", h.Sum(nil))
	}
	var connectionString string
	if tls {
		connectionString = fmt.Sprintf("grpcs://%s%s", dbuiri, baseDB)
	} else {
		connectionString = fmt.Sprintf("grpc://%s%s", dbuiri, baseDB)
	}
	t.Logf("→ Opening YDB connection: %s", connectionString)

	db, err := ydb.Open(ctx, connectionString,
		ydb.WithInsecure(),
		ydb.WithDiscoveryInterval(time.Hour),
	)
	if err != nil {
		t.Fatalf("failed to open YDB connection: %s", err)
	}

	prefix := fmt.Sprintf("%s/%s", baseDB, name)
	t.Logf("→ Using prefix: %s", prefix)

	err = sugar.RemoveRecursive(ctx, db, prefix)
	if err != nil {
		t.Logf("Warning: failed to remove old data: %s", err)
	}

	t.Log("→ Applying migrations to prefix: ", prefix)

	for _, stmt := range seed {
		err := db.Query().Exec(ctx, stmt,
			query.WithTxControl(query.EmptyTxControl()),
		)
		if err != nil {
			t.Fatalf("failed to apply migration: %s\nSQL: %s", err, stmt)
		}
	}

	return db
}
