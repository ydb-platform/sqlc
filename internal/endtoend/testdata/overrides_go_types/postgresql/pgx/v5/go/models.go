// Code generated by sqlc. DO NOT EDIT.
// versions:
//   sqlc v1.23.0

package override

import (
	orm "database/sql"
	fuid "github.com/gofrs/uuid"
	uuid "github.com/gofrs/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	null "github.com/volatiletech/null/v8"
	null_v4 "gopkg.in/guregu/null.v4"
)

type Bar struct {
	ID      uuid.UUID
	OtherID fuid.UUID
	MoreID  fuid.UUID
	Age     pgtype.Int4
	Balance pgtype.Float8
	Bio     pgtype.Text
	About   pgtype.Text
}

type Foo struct {
	ID      uuid.UUID
	OtherID fuid.UUID
	Age     orm.NullInt32
	Balance null.Float32
	Bio     null_v4.String
	About   *string
}
