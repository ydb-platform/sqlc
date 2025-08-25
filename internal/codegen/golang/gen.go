package golang

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go/format"
	"strconv"
	"strings"
	"text/template"
	"time"

	"github.com/sqlc-dev/sqlc/internal/codegen/golang/opts"
	"github.com/sqlc-dev/sqlc/internal/codegen/sdk"
	"github.com/sqlc-dev/sqlc/internal/metadata"
	"github.com/sqlc-dev/sqlc/internal/plugin"
)

type tmplCtx struct {
	Q           string
	Package     string
	SQLDriver   opts.SQLDriver
	Enums       []Enum
	Structs     []Struct
	GoQueries   []Query
	SqlcVersion string

	// TODO: Race conditions
	SourceName string

	EmitJSONTags              bool
	JsonTagsIDUppercase       bool
	EmitDBTags                bool
	EmitPreparedQueries       bool
	EmitInterface             bool
	EmitEmptySlices           bool
	EmitMethodsWithDBArgument bool
	EmitEnumValidMethod       bool
	EmitAllEnumValues         bool
	UsesCopyFrom              bool
	UsesBatch                 bool
	OmitSqlcVersion           bool
	BuildTags                 string
	WrapErrors                bool
	EnableYDBRetry            bool
	YDBRetryIdempotent        bool
}

func (t *tmplCtx) OutputQuery(sourceName string) bool {
	return t.SourceName == sourceName
}

func (t *tmplCtx) codegenDbarg() string {
	if t.EmitMethodsWithDBArgument {
		if t.EnableYDBRetry {
			return "db *sql.DB, "
		}
		return "db DBTX, "
	}
	return ""
}

// Called as a global method since subtemplate queryCodeStdExec does not have
// access to the toplevel tmplCtx
func (t *tmplCtx) codegenEmitPreparedQueries() bool {
	return t.EmitPreparedQueries
}

func (t *tmplCtx) codegenQueryMethod(q Query) string {
	db := "q.db"
	if t.EmitMethodsWithDBArgument {
		db = "db"
		if t.EnableYDBRetry && q.RetryMode == "direct" {
			db = "tx"
		}
	} else if t.EnableYDBRetry && q.RetryMode == "direct" {
		db = "q.tx"
	}

	switch q.Cmd {
	case ":one":
		if t.EmitPreparedQueries {
			return "q.queryRow"
		}
		return db + ".QueryRowContext"

	case ":many":
		if t.EmitPreparedQueries {
			return "q.query"
		}
		return db + ".QueryContext"

	default:
		if t.EmitPreparedQueries {
			return "q.exec"
		}
		return db + ".ExecContext"
	}
}

func (t *tmplCtx) codegenQueryRetval(q Query) (string, error) {
	switch q.Cmd {
	case ":one":
		return "row :=", nil
	case ":many":
		return "rows, err :=", nil
	case ":exec":
		return "_, err :=", nil
	case ":execrows", ":execlastid":
		if t.EnableYDBRetry {
			return "", fmt.Errorf("YDB doesn't support %q code generation", q.Cmd)
		}
		return "result, err :=", nil
	case ":execresult":
		if t.WrapErrors || t.EnableYDBRetry {
			return "result, err :=", nil
		}
		return "return", nil
	default:
		return "", fmt.Errorf("unhandled q.Cmd case %q", q.Cmd)
	}
}

// Helper function to parse YDB metadata from comments
func parseYDBMetadata(comments *[]string) (string, string, string) {
	retryMode := "do"
	var retryOptions []string
	var txOptionsStr string

	flags := make(map[string]bool)
	params := make(map[string]string)

	idx := 0
	for _, comment := range *comments {
		if after, found := strings.CutPrefix(comment, "YDB_FLAG:"); found {
			flags[after] = true
		} else if after, found := strings.CutPrefix(comment, "YDB_PARAM:"); found {
			if idx := strings.Index(after, "="); idx >= 0 {
				key := after[:idx]
				value := after[idx+1:]
				params[key] = value
			}
		} else if !strings.Contains(comment, "@ydb-") {
			(*comments)[idx] = comment
			idx++
		}
	}
	*comments = (*comments)[:idx]

	if flags["@ydb-direct-tx"] {
		retryMode = "direct"
	} else if options, ok := params["@ydb-with-tx"]; ok {
		retryMode = "dotx"
		txOptionsStr = fmt.Sprintf("retry.WithTxOptions(%s)", codegenYDBTxOptions(options))
	}

	if budget, ok := params["@ydb-budget"]; ok && budget != "" {
		retryOptions = append(retryOptions, fmt.Sprintf("retry.WithBudget(%s)", codegenYDBBudget(budget)))
	}

	if label, ok := params["@ydb-retry-label"]; ok {
		retryOptions = append(retryOptions, fmt.Sprintf("retry.WithLabel(%q)", label))
	}

	if flags["@ydb-retry-idempotent"] {
		retryOptions = append(retryOptions, "retry.WithIdempotent(true)")
	}

	var retryOptsStr string
	if len(retryOptions) > 0 {
		retryOptsStr = ",\n" + strings.Join(retryOptions, ",\n")
		if txOptionsStr != "" {
			retryOptsStr += ",\n" + txOptionsStr
		}
	} else if txOptionsStr != "" {
		retryOptsStr = ",\n" + txOptionsStr
	}

	if retryOptsStr != "" {
		retryOptsStr += ",\n"
	}

	return retryMode, retryOptsStr, txOptionsStr
}

func codegenYDBBudget(budgetStr string) string {
	budgetStr = strings.TrimSpace(budgetStr)

	if strings.HasPrefix(budgetStr, "{") {
		var budget struct {
			TTL   string `json:"ttl"`
			Limit int    `json:"limit"`
		}
		if err := json.Unmarshal([]byte(budgetStr), &budget); err == nil {
			if budget.TTL != "" {
				if _, err := time.ParseDuration(budget.TTL); err == nil {
					return fmt.Sprintf("budget.WithTTL(%s, %d)",
						formatDuration(budget.TTL), budget.Limit)
				}
			}
			return fmt.Sprintf("budget.Limited(%d)", budget.Limit)
		}
	}

	if limit, err := strconv.Atoi(budgetStr); err == nil {
		return fmt.Sprintf("budget.Limited(%d)", limit)
	}

	return "budget.Limited(5)"
}

func codegenYDBTxOptions(optionsStr string) string {
	txOptions := "&sql.TxOptions{\n\tIsolation: %s,\n\tReadOnly:  %t,\n}"
	if optionsStr == "" {
		return fmt.Sprintf(txOptions, "sql.LevelDefault", false)
	}

	var opts struct {
		Isolation string `json:"isolation"`
		ReadOnly  bool   `json:"readonly"`
	}

	if err := json.Unmarshal([]byte(optionsStr), &opts); err != nil {
		return fmt.Sprintf(txOptions, "sql.LevelDefault", false)
	}

	var isolation string
	switch strings.ToLower(opts.Isolation) {
	case "serializable":
		isolation = "sql.LevelSerializable"
	case "read_committed", "readcommitted":
		isolation = "sql.LevelReadCommitted"
	case "repeatable_read", "repeatableread":
		isolation = "sql.LevelRepeatableRead"
	case "read_uncommitted", "readuncommitted":
		isolation = "sql.LevelReadUncommitted"
	case "write_committed", "writecommitted":
		isolation = "sql.LevelWriteCommitted"
	case "snapshot":
		isolation = "sql.LevelSnapshot"
	case "linearizable":
		isolation = "sql.LevelLinearizable"
	default:
		isolation = "sql.LevelDefault"
	}

	return fmt.Sprintf(txOptions, isolation, opts.ReadOnly)
}

func formatDuration(durationStr string) string {
	d, err := time.ParseDuration(durationStr)
	if err != nil {
		return durationStr
	}

	if d < time.Microsecond {
		return fmt.Sprintf("%d * time.Nanosecond", d.Nanoseconds())
	}
	if d < time.Millisecond {
		return fmt.Sprintf("%d * time.Microsecond", d.Nanoseconds()/1000)
	}
	if d < time.Second {
		return fmt.Sprintf("%d * time.Millisecond", d.Nanoseconds()/1000000)
	}
	if d < time.Minute {
		return fmt.Sprintf("%d * time.Second", int64(d.Seconds()))
	}
	if d < time.Hour {
		return fmt.Sprintf("%d * time.Minute", int64(d.Minutes()))
	}
	return fmt.Sprintf("%d * time.Hour", int64(d.Hours()))
}

func (t *tmplCtx) codegenYDBRetryMode(q Query) string {
	return q.RetryMode
}

func (t *tmplCtx) codegenYDBRetryOptions(q Query) string {
	return q.RetryOptions
}

func (t *tmplCtx) codegenYDBTxOptions(q Query) string {
	return q.TxOptions
}

func Generate(ctx context.Context, req *plugin.GenerateRequest) (*plugin.GenerateResponse, error) {
	options, err := opts.Parse(req)
	if err != nil {
		return nil, err
	}

	if err := opts.ValidateOpts(options); err != nil {
		return nil, err
	}

	enums := buildEnums(req, options)
	structs := buildStructs(req, options)
	queries, err := buildQueries(req, options, structs)
	if err != nil {
		return nil, err
	}

	if options.OmitUnusedStructs {
		enums, structs = filterUnusedStructs(enums, structs, queries)
	}

	if err := validate(options, enums, structs, queries); err != nil {
		return nil, err
	}

	return generate(req, options, enums, structs, queries)
}

func validate(options *opts.Options, enums []Enum, structs []Struct, queries []Query) error {
	enumNames := make(map[string]struct{})
	for _, enum := range enums {
		enumNames[enum.Name] = struct{}{}
		enumNames["Null"+enum.Name] = struct{}{}
	}
	structNames := make(map[string]struct{})
	for _, struckt := range structs {
		if _, ok := enumNames[struckt.Name]; ok {
			return fmt.Errorf("struct name conflicts with enum name: %s", struckt.Name)
		}
		structNames[struckt.Name] = struct{}{}
	}
	if !options.EmitExportedQueries {
		return nil
	}
	for _, query := range queries {
		if _, ok := enumNames[query.ConstantName]; ok {
			return fmt.Errorf("query constant name conflicts with enum name: %s", query.ConstantName)
		}
		if _, ok := structNames[query.ConstantName]; ok {
			return fmt.Errorf("query constant name conflicts with struct name: %s", query.ConstantName)
		}
	}
	return nil
}

func generate(req *plugin.GenerateRequest, options *opts.Options, enums []Enum, structs []Struct, queries []Query) (*plugin.GenerateResponse, error) {
	i := &importer{
		Options: options,
		Queries: queries,
		Enums:   enums,
		Structs: structs,
	}

	tctx := tmplCtx{
		EmitInterface:             options.EmitInterface,
		EmitJSONTags:              options.EmitJsonTags,
		JsonTagsIDUppercase:       options.JsonTagsIdUppercase,
		EmitDBTags:                options.EmitDbTags,
		EmitPreparedQueries:       options.EmitPreparedQueries,
		EmitEmptySlices:           options.EmitEmptySlices,
		EmitMethodsWithDBArgument: options.EmitMethodsWithDbArgument,
		EmitEnumValidMethod:       options.EmitEnumValidMethod,
		EmitAllEnumValues:         options.EmitAllEnumValues,
		UsesCopyFrom:              usesCopyFrom(queries),
		UsesBatch:                 usesBatch(queries),
		SQLDriver:                 parseDriver(options.SqlPackage),
		Q:                         "`",
		Package:                   options.Package,
		Enums:                     enums,
		Structs:                   structs,
		SqlcVersion:               req.SqlcVersion,
		BuildTags:                 options.BuildTags,
		OmitSqlcVersion:           options.OmitSqlcVersion,
		WrapErrors:                options.WrapErrors,
		EnableYDBRetry:            options.EnableYDBRetry,
		YDBRetryIdempotent:        options.YDBRetryIdempotent,
	}

	if tctx.UsesCopyFrom && !tctx.SQLDriver.IsPGX() && options.SqlDriver != opts.SQLDriverGoSQLDriverMySQL {
		return nil, errors.New(":copyfrom is only supported by pgx and github.com/go-sql-driver/mysql")
	}

	if tctx.UsesCopyFrom && options.SqlDriver == opts.SQLDriverGoSQLDriverMySQL {
		if err := checkNoTimesForMySQLCopyFrom(queries); err != nil {
			return nil, err
		}
		tctx.SQLDriver = opts.SQLDriverGoSQLDriverMySQL
	}

	if tctx.UsesBatch && !tctx.SQLDriver.IsPGX() {
		return nil, errors.New(":batch* commands are only supported by pgx")
	}

	funcMap := template.FuncMap{
		"lowerTitle": sdk.LowerTitle,
		"comment":    sdk.DoubleSlashComment,
		"escape":     sdk.EscapeBacktick,
		"imports":    i.Imports,
		"hasImports": i.HasImports,
		"hasPrefix":  strings.HasPrefix,

		// These methods are Go specific, they do not belong in the codegen package
		// (as that is language independent)
		"dbarg":               tctx.codegenDbarg,
		"emitPreparedQueries": tctx.codegenEmitPreparedQueries,
		"queryMethod":         tctx.codegenQueryMethod,
		"queryRetval":         tctx.codegenQueryRetval,

		// YDB specific methods
		"ydbRetryMode":    tctx.codegenYDBRetryMode,
		"ydbRetryOptions": tctx.codegenYDBRetryOptions,
		"ydbTxOptions":    tctx.codegenYDBTxOptions,
	}

	tmpl := template.Must(
		template.New("table").
			Funcs(funcMap).
			ParseFS(
				templates,
				"templates/*.tmpl",
				"templates/*/*.tmpl",
			),
	)

	output := map[string]string{}

	execute := func(name, templateName string) error {
		imports := i.Imports(name)
		replacedQueries := replaceConflictedArg(imports, queries)

		var b bytes.Buffer
		w := bufio.NewWriter(&b)
		tctx.SourceName = name
		tctx.GoQueries = replacedQueries
		err := tmpl.ExecuteTemplate(w, templateName, &tctx)
		w.Flush()
		if err != nil {
			return err
		}
		code, err := format.Source(b.Bytes())
		if err != nil {
			fmt.Println(b.String())
			return fmt.Errorf("source error: %w", err)
		}

		if templateName == "queryFile" && options.OutputFilesSuffix != "" {
			name += options.OutputFilesSuffix
		}

		if !strings.HasSuffix(name, ".go") {
			name += ".go"
		}
		output[name] = string(code)
		return nil
	}

	dbFileName := "db.go"
	if options.OutputDbFileName != "" {
		dbFileName = options.OutputDbFileName
	}
	modelsFileName := "models.go"
	if options.OutputModelsFileName != "" {
		modelsFileName = options.OutputModelsFileName
	}
	querierFileName := "querier.go"
	if options.OutputQuerierFileName != "" {
		querierFileName = options.OutputQuerierFileName
	}
	copyfromFileName := "copyfrom.go"
	if options.OutputCopyfromFileName != "" {
		copyfromFileName = options.OutputCopyfromFileName
	}

	batchFileName := "batch.go"
	if options.OutputBatchFileName != "" {
		batchFileName = options.OutputBatchFileName
	}

	if err := execute(dbFileName, "dbFile"); err != nil {
		return nil, err
	}
	if err := execute(modelsFileName, "modelsFile"); err != nil {
		return nil, err
	}
	if options.EmitInterface {
		if err := execute(querierFileName, "interfaceFile"); err != nil {
			return nil, err
		}
	}
	if tctx.UsesCopyFrom {
		if err := execute(copyfromFileName, "copyfromFile"); err != nil {
			return nil, err
		}
	}
	if tctx.UsesBatch {
		if err := execute(batchFileName, "batchFile"); err != nil {
			return nil, err
		}
	}

	files := map[string]struct{}{}
	for _, gq := range queries {
		files[gq.SourceName] = struct{}{}
	}

	for source := range files {
		if err := execute(source, "queryFile"); err != nil {
			return nil, err
		}
	}
	resp := plugin.GenerateResponse{}

	for filename, code := range output {
		resp.Files = append(resp.Files, &plugin.File{
			Name:     filename,
			Contents: []byte(code),
		})
	}

	return &resp, nil
}

func usesCopyFrom(queries []Query) bool {
	for _, q := range queries {
		if q.Cmd == metadata.CmdCopyFrom {
			return true
		}
	}
	return false
}

func usesBatch(queries []Query) bool {
	for _, q := range queries {
		for _, cmd := range []string{metadata.CmdBatchExec, metadata.CmdBatchMany, metadata.CmdBatchOne} {
			if q.Cmd == cmd {
				return true
			}
		}
	}
	return false
}

func checkNoTimesForMySQLCopyFrom(queries []Query) error {
	for _, q := range queries {
		if q.Cmd != metadata.CmdCopyFrom {
			continue
		}
		for _, f := range q.Arg.CopyFromMySQLFields() {
			if f.Type == "time.Time" {
				return fmt.Errorf("values with a timezone are not yet supported")
			}
		}
	}
	return nil
}

func filterUnusedStructs(enums []Enum, structs []Struct, queries []Query) ([]Enum, []Struct) {
	keepTypes := make(map[string]struct{})

	for _, query := range queries {
		if !query.Arg.isEmpty() {
			keepTypes[query.Arg.Type()] = struct{}{}
			if query.Arg.IsStruct() {
				for _, field := range query.Arg.Struct.Fields {
					keepTypes[field.Type] = struct{}{}
				}
			}
		}
		if query.hasRetType() {
			keepTypes[query.Ret.Type()] = struct{}{}
			if query.Ret.IsStruct() {
				for _, field := range query.Ret.Struct.Fields {
					keepTypes[field.Type] = struct{}{}
					for _, embedField := range field.EmbedFields {
						keepTypes[embedField.Type] = struct{}{}
					}
				}
			}
		}
	}

	keepEnums := make([]Enum, 0, len(enums))
	for _, enum := range enums {
		_, keep := keepTypes[enum.Name]
		_, keepNull := keepTypes["Null"+enum.Name]
		if keep || keepNull {
			keepEnums = append(keepEnums, enum)
		}
	}

	keepStructs := make([]Struct, 0, len(structs))
	for _, st := range structs {
		if _, ok := keepTypes[st.Name]; ok {
			keepStructs = append(keepStructs, st)
		}
	}

	return keepEnums, keepStructs
}
