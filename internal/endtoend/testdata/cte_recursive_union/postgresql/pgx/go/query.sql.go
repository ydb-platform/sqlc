// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.23.0
// source: query.sql

package querytest

import (
	"context"

	"github.com/jackc/pgx/v5/pgtype"
)

const listCaseIntentHistory = `-- name: ListCaseIntentHistory :many
WITH RECURSIVE descendants AS
   ( SELECT case_intent_parent_id AS parent, case_intent_id AS child, 1 AS lvl
     FROM case_intent_parent_join
     UNION ALL
     SELECT d.parent as parent, p.case_intent_id as child, d.lvl + 1 as lvl
     FROM descendants d
              JOIN case_intent_parent_join p
                   ON d.child = p.case_intent_parent_id
   )
select distinct child, 'child' group_
from descendants
where parent = $1
union
select distinct parent, 'parent' group_
from descendants
where child = $1
ORDER BY child
`

type ListCaseIntentHistoryRow struct {
	Child int64
	Group string
}

func (q *Queries) ListCaseIntentHistory(ctx context.Context, caseIntentID pgtype.Int8) ([]ListCaseIntentHistoryRow, error) {
	rows, err := q.db.Query(ctx, listCaseIntentHistory, caseIntentID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []ListCaseIntentHistoryRow
	for rows.Next() {
		var i ListCaseIntentHistoryRow
		if err := rows.Scan(&i.Child, &i.Group); err != nil {
			return nil, err
		}
		items = append(items, i)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}
