package golang

import (
	"log"
	"strings"

	"github.com/sqlc-dev/sqlc/internal/codegen/golang/opts"
	"github.com/sqlc-dev/sqlc/internal/codegen/sdk"
	"github.com/sqlc-dev/sqlc/internal/debug"
	"github.com/sqlc-dev/sqlc/internal/plugin"
)

func YDBType(req *plugin.GenerateRequest, options *opts.Options, col *plugin.Column) string {
	columnType := strings.ToLower(sdk.DataType(col.Type))
	notNull := col.NotNull || col.IsArray
	emitPointersForNull := options.EmitPointersForNullTypes

	// https://ydb.tech/docs/ru/yql/reference/types/
	switch columnType {
	// decimal types
	case "bool":
		if notNull {
			return "bool"
		}
		if emitPointersForNull {
			return "*bool"
		}
		return "sql.NullBool"

	case "int8":
		if notNull {
			return "int8"
		}
		if emitPointersForNull {
			return "*int8"
		}
		// The database/sql package does not have a sql.NullInt8 type, so we
		// use the smallest type they have which is NullInt16
		return "sql.NullInt16"
	case "int16":
		if notNull {
			return "int16"
		}
		if emitPointersForNull {
			return "*int16"
		}
		return "sql.NullInt16"
	case "int32":
		if notNull {
			return "int32"
		}
		if emitPointersForNull {
			return "*int32"
		}
		return "sql.NullInt32"
	case "int64":
		if notNull {
			return "int64"
		}
		if emitPointersForNull {
			return "*int64"
		}
		return "sql.NullInt64"

	case "uint8":
		if emitPointersForNull {
			return "*uint8"
		}
		return "uint8"
	case "uint16":
		if emitPointersForNull {
			return "*uint16"
		}
		return "uint16"
	case "uint32":
		if emitPointersForNull {
			return "*uint32"
		}
		return "uint32"
	case "uint64":
		if emitPointersForNull {
			return "*uint64"
		}
		return "uint64"

	case "float":
		if notNull {
			return "float32"
		}
		if emitPointersForNull {
			return "*float32"
		}
		// The database/sql package does not have a sql.NullFloat32 type, so we
		// use the smallest type they have which is NullFloat64
		return "sql.NullFloat64"
	case "double":
		if notNull {
			return "float64"
		}
		if emitPointersForNull {
			return "*float64"
		}
		return "sql.NullFloat64"

	// string types
	case "string", "utf8", "text":
		if notNull {
			return "string"
		}
		if emitPointersForNull {
			return "*string"
		}
		return "sql.NullString"

	// serial types
	case "smallserial", "serial2":
		if notNull {
			return "int16"
		}
		if emitPointersForNull {
			return "*int16"
		}
		return "sql.NullInt16"

	case "serial", "serial4":
		if notNull {
			return "int32"
		}
		if emitPointersForNull {
			return "*int32"
		}
		return "sql.NullInt32"

	case "bigserial", "serial8":
		if notNull {
			return "int64"
		}
		if emitPointersForNull {
			return "*int64"
		}
		return "sql.NullInt64"

	case "null":
		return "sql.Null"

	default:
		if debug.Active {
			log.Printf("unknown SQLite type: %s\n", columnType)
		}

		return "interface{}"
	}

}
