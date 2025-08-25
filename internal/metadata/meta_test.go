package metadata

import (
	"testing"
)

func TestParseQueryNameAndType(t *testing.T) {

	for _, query := range []string{
		`-- name: CreateFoo, :one`,
		`-- name: 9Foo_, :one`,
		`-- name: CreateFoo :two`,
		`-- name: CreateFoo`,
		`-- name: CreateFoo :one something`,
		`-- name: `,
		`--name: CreateFoo :one`,
		`--name CreateFoo :one`,
		`--name: CreateFoo :two`,
		"-- name:CreateFoo",
		`--name:CreateFoo :two`,
	} {
		if _, _, err := ParseQueryNameAndType(query, CommentSyntax{Dash: true}); err == nil {
			t.Errorf("expected invalid metadata: %q", query)
		}
	}

	for _, query := range []string{
		`-- some comment`,
		`-- name comment`,
		`--name comment`,
	} {
		if _, _, err := ParseQueryNameAndType(query, CommentSyntax{Dash: true}); err != nil {
			t.Errorf("expected valid comment: %q", query)
		}
	}

	for query, cs := range map[string]CommentSyntax{
		`-- name: CreateFoo :one`:    {Dash: true},
		`# name: CreateFoo :one`:     {Hash: true},
		`/* name: CreateFoo :one */`: {SlashStar: true},
	} {
		queryName, queryCmd, err := ParseQueryNameAndType(query, cs)
		if err != nil {
			t.Errorf("expected valid metadata: %q", query)
		}
		if queryName != "CreateFoo" {
			t.Errorf("incorrect queryName parsed: (%q) %q", queryName, query)
		}
		if queryCmd != CmdOne {
			t.Errorf("incorrect queryCmd parsed: (%q) %q", queryCmd, query)
		}
	}

}

func TestParseQueryParams(t *testing.T) {
	for _, comments := range [][]string{
		{
			" name: CreateFoo :one",
			" @param foo_id UUID",
		},
		{
			" name: CreateFoo :one ",
			" @param foo_id UUID ",
		},
		{
			" name: CreateFoo :one",
			"@param foo_id UUID",
			" invalid",
		},
		{
			" name: CreateFoo :one",
			" @invalid",
			" @param foo_id UUID",
		},
		{
			" name: GetFoos :many ",
			" @param foo_id UUID ",
			" @param @invalid UUID ",
		},
		{
			" name: YDBQuery :one",
			" @ydb-label critical-operation",
		},
		{
			" name: YDBQuery :one",
			" @ydb-fast-backoff 100ms 5 0.1",
		},
		{
			" name: YDBQuery :one",
			" @ydb-slow-backoff 1s 10 0.2",
		},
		{
			" name: YDBQuery :one",
			" @ydb-tx-options {\"isolation\":\"serializable\",\"readonly\":false}",
		},
		{
			" name: YDBQuery :one",
			" @ydb-budget {\"ttl\":\"30s\",\"limit\":5}",
		},
	} {
		params, _, _, err := ParseCommentFlags(comments)
		if err != nil {
			t.Errorf("expected comments to parse, got err: %s", err)
		}

		pt, ok := params["foo_id"]
		if !ok {
			t.Errorf("expected param not found")
		}

		if pt != "UUID" {
			t.Error("unexpected param metadata:", pt)
		}

		_, ok = params["invalid"]
		if ok {
			t.Errorf("unexpected param found")
		}

		if txOpts, ok := params["@ydb-tx-options"]; ok {
			expected := `{"isolation":"serializable","readonly":false}`
			if txOpts != expected {
				t.Errorf("unexpected YDB tx options param: %s, expected: %s", txOpts, expected)
			}
		}

		if budget, ok := params["@ydb-budget"]; ok {
			expected := `{"ttl":"30s","limit":5}`
			if budget != expected {
				t.Errorf("unexpected YDB budget param: %s, expected: %s", budget, expected)
			}
		}
	}
}

func TestParseQueryFlags(t *testing.T) {
	for _, comments := range [][]string{
		{
			" name: CreateFoo :one",
			" @flag-foo",
		},
		{
			" name: CreateFoo :one ",
			"@flag-foo ",
		},
		{
			" name: CreateFoo :one",
			" @flag-foo @flag-bar",
		},
		{
			" name: GetFoos :many",
			" @param @flag-bar UUID",
			" @flag-foo",
		},
		{
			" name: GetFoos :many",
			" @flag-foo",
			" @param @flag-bar UUID",
		},
		{
			" name: YDBQuery :one",
			" @ydb-retry-idempotent",
		},
	} {
		_, flags, _, err := ParseCommentFlags(comments)
		if err != nil {
			t.Errorf("expected comments to parse, got err: %s", err)
		}

		if !flags["@flag-foo"] {
			t.Errorf("expected flag not found")
		}

		if flags["@flag-bar"] {
			t.Errorf("unexpected flag found")
		}

		if !flags["@ydb-retry-idempotent"] {
			t.Errorf("expected flag not found")
		}
	}
}

func TestParseQueryRuleSkiplist(t *testing.T) {
	for _, comments := range [][]string{
		{
			" name: CreateFoo :one",
			" @sqlc-vet-disable sqlc/db-prepare delete-without-where ",
		},
		{
			" name: CreateFoo :one ",
			" @sqlc-vet-disable sqlc/db-prepare ",
			" @sqlc-vet-disable delete-without-where ",
		},
		{
			" name: CreateFoo :one",
			" @sqlc-vet-disable sqlc/db-prepare ",
			" update-without where",
			" @sqlc-vet-disable delete-without-where ",
		},
	} {
		_, flags, ruleSkiplist, err := ParseCommentFlags(comments)
		if err != nil {
			t.Errorf("expected comments to parse, got err: %s", err)
		}

		if !flags["@sqlc-vet-disable"] {
			t.Errorf("expected @sqlc-vet-disable flag not found")
		}

		if _, ok := ruleSkiplist["sqlc/db-prepare"]; !ok {
			t.Errorf("expected rule not found in skiplist")
		}

		if _, ok := ruleSkiplist["delete-without-where"]; !ok {
			t.Errorf("expected rule not found in skiplist")
		}

		if _, ok := ruleSkiplist["update-without-where"]; ok {
			t.Errorf("unexpected rule found in skiplist")
		}
	}
}
