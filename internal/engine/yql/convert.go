package yql

import (
	"github.com/antlr4-go/antlr/v4"
	"github.com/sqlc-dev/sqlc/internal/debug"
	"github.com/sqlc-dev/sqlc/internal/sql/ast"
	parser "github.com/ydb-platform/yql-parsers/go"
	"log"
	"strconv"
	"strings"
)

type cc struct {
	paramCount int
}

type node interface {
	GetParser() antlr.Parser
}

func todo(funcname string, n node) *ast.TODO {
	if debug.Active {
		log.Printf("sqlite.%s: Unknown node type %T\n", funcname, n)
	}
	return &ast.TODO{}
}

func identifier(id string) string {
	if len(id) >= 2 && id[0] == '"' && id[len(id)-1] == '"' {
		unquoted, _ := strconv.Unquote(id)
		return unquoted
	}
	return strings.ToLower(id)
}

func NewIdentifier(t string) *ast.String {
	return &ast.String{Str: identifier(t)}
}

func (c *cc) convertAlter_table_stmtContext(n *parser.Alter_table_stmtContext) ast.Node {
	tableRef := parseTableName(n.Simple_table_ref().Simple_table_ref_core())

	stmt := &ast.AlterTableStmt{
		Table: tableRef,
		Cmds:  &ast.List{},
	}
	for _, action := range n.AllAlter_table_action() {
		if add := action.Alter_table_add_column(); add != nil {
		}
	}
	return stmt
}

func (c *cc) convertSelectStmtContext(n *parser.Select_stmtContext) ast.Node {
	skp := n.Select_kind_parenthesis(0)
	if skp == nil {
		return nil
	}
	partial := skp.Select_kind_partial()
	if partial == nil {
		return nil
	}
	sk := partial.Select_kind()
	if sk == nil {
		return nil
	}
	selectStmt := &ast.SelectStmt{}

	switch {
	case sk.Process_core() != nil:
		cnode := c.convert(sk.Process_core())
		stmt, ok := cnode.(*ast.SelectStmt)
		if !ok {
			return nil
		}
		selectStmt = stmt
	case sk.Select_core() == nil:
		cnode := c.convert(sk.Select_core())
		stmt, ok := cnode.(*ast.SelectStmt)
		if !ok {
			return nil
		}
		selectStmt = stmt
	case sk.Reduce_core() == nil:
		cnode := c.convert(sk.Reduce_core())
		stmt, ok := cnode.(*ast.SelectStmt)
		if !ok {
			return nil
		}
		selectStmt = stmt
	}

	// todo: cover process and reduce core,
	// todo: cover LIMIT and OFFSET

	return selectStmt
}

func (c *cc) convertSelectCoreContext(n *parser.Select_coreContext) ast.Node {
	stmt := &ast.SelectStmt{}
	if n.Opt_set_quantifier() != nil {
		oq := n.Opt_set_quantifier()
		if oq.DISTINCT() != nil {
			// todo: add distinct support
			stmt.DistinctClause = &ast.List{}
		}
	}
	resultCols := n.AllResult_column()
	if len(resultCols) > 0 {
		var items []ast.Node
		for _, rc := range resultCols {
			resCol, ok := rc.(*parser.Result_columnContext)
			if !ok {
				continue
			}
			convNode := c.convertResultColumn(resCol)
			if convNode != nil {
				items = append(items, convNode)
			}
		}
		stmt.TargetList = &ast.List{
			Items: items,
		}
	}
	jsList := n.AllJoin_source()
	if len(jsList) > 0 {
		var fromItems []ast.Node
		for _, js := range jsList {
			jsCon, ok := js.(*parser.Join_sourceContext)
			if !ok {
				continue
			}

			joinNode := c.convertJoinSource(jsCon)
			if joinNode != nil {
				fromItems = append(fromItems, joinNode)
			}
		}
		stmt.FromClause = &ast.List{
			Items: fromItems,
		}
	}
	if n.WHERE() != nil {
		whereCtx := n.Expr(0)
		if whereCtx != nil {
			stmt.WhereClause = c.convert(whereCtx)
		}
	}
	return stmt
}

func (c *cc) convertResultColumn(n *parser.Result_columnContext) ast.Node {
	exprCtx := n.Expr()
	if exprCtx == nil {
		return nil
	}
	target := &ast.ResTarget{
		Location: n.GetStart().GetStart(),
	}
	var val ast.Node
	iexpr := n.Expr()
	switch {
	case n.ASTERISK() != nil:
		val = c.convertWildCardField(n)
	case iexpr != nil:
		val = c.convert(iexpr)
	}

	if val == nil {
		return nil
	}
	switch {
	case n.AS() != nil && n.An_id_or_type() != nil:
		name := parseAnIdOrType(n.An_id_or_type())
		target.Name = &name
	case n.An_id_as_compat() != nil:
		// todo: parse as_compat
	}
	target.Val = val
	return target
}

func (c *cc) convertJoinSource(n *parser.Join_sourceContext) ast.Node {
	fsList := n.AllFlatten_source()
	if len(fsList) == 0 {
		return nil
	}
	joinOps := n.AllJoin_op()
	joinConstraints := n.AllJoin_constraint()

	// todo: add ANY support

	leftNode := c.convertFlattenSource(fsList[0])
	if leftNode == nil {
		return nil
	}
	for i, jopCtx := range joinOps {
		if i+1 >= len(fsList) {
			break
		}
		rightNode := c.convertFlattenSource(fsList[i+1])
		if rightNode == nil {
			return leftNode
		}
		jexpr := &ast.JoinExpr{
			Larg: leftNode,
			Rarg: rightNode,
		}
		if jopCtx.NATURAL() != nil {
			jexpr.IsNatural = true
		}
		// todo: cover semi/only/exclusion/
		switch {
		case jopCtx.LEFT() != nil:
			jexpr.Jointype = ast.JoinTypeLeft
		case jopCtx.RIGHT() != nil:
			jexpr.Jointype = ast.JoinTypeRight
		case jopCtx.FULL() != nil:
			jexpr.Jointype = ast.JoinTypeFull
		case jopCtx.INNER() != nil:
			jexpr.Jointype = ast.JoinTypeInner
		case jopCtx.COMMA() != nil:
			jexpr.Jointype = ast.JoinTypeInner
		default:
			jexpr.Jointype = ast.JoinTypeInner
		}
		if i < len(joinConstraints) {
			jc := joinConstraints[i]

			if jc != nil {
				switch {
				case jc.ON() != nil:
					if exprCtx := jc.Expr(); exprCtx != nil {
						jexpr.Quals = c.convert(exprCtx)
					}
				case jc.USING() != nil:
					if pureListCtx := jc.Pure_column_or_named_list(); pureListCtx != nil {
						var using ast.List
						pureItems := pureListCtx.AllPure_column_or_named()
						for _, pureCtx := range pureItems {
							if anID := pureCtx.An_id(); anID != nil {
								using.Items = append(using.Items, NewIdentifier(parseAnId(anID)))
							} else if bp := pureCtx.Bind_parameter(); bp != nil {
								bindPar := c.convert(bp)
								using.Items = append(using.Items, bindPar)
							}
						}
						jexpr.UsingClause = &using
					}
				}
			}
		}
		leftNode = jexpr
	}
	return leftNode
}

func (c *cc) convertFlattenSource(n parser.IFlatten_sourceContext) ast.Node {
	if n == nil {
		return nil
	}
	nss := n.Named_single_source()
	if nss == nil {
		return nil
	}
	namedSingleSource, ok := nss.(*parser.Named_single_sourceContext)
	if !ok {
		return nil
	}
	return c.convertNamedSingleSource(namedSingleSource)
}

func (c *cc) convertNamedSingleSource(n *parser.Named_single_sourceContext) ast.Node {
	ss := n.Single_source()
	if ss == nil {
		return nil
	}
	SingleSource, ok := ss.(*parser.Single_sourceContext)
	if !ok {
		return nil
	}
	base := c.convertSingleSource(SingleSource)

	if n.AS() != nil && n.An_id() != nil {
		aliasText := parseAnId(n.An_id())
		switch source := base.(type) {
		case *ast.RangeVar:
			source.Alias = &ast.Alias{Aliasname: &aliasText}
		case *ast.RangeSubselect:
			source.Alias = &ast.Alias{Aliasname: &aliasText}
		}
	} else if n.An_id_as_compat() != nil {
		// todo: parse as_compat
	}
	return base
}

func (c *cc) convertSingleSource(n *parser.Single_sourceContext) ast.Node {
	if n.Table_ref() != nil {
		tableName := n.Table_ref().GetText() // !! debug !!
		return &ast.RangeVar{
			Relname:  &tableName,
			Location: n.GetStart().GetStart(),
		}
	}

	if n.Select_stmt() != nil {
		subquery := c.convert(n.Select_stmt())
		return &ast.RangeSubselect{
			Subquery: subquery,
		}

	}
	// todo: Values stmt

	return nil
}

func (c *cc) convertBindParameter(n *parser.Bind_parameterContext) ast.Node {
	// !!debug later!!
	if n.DOLLAR() != nil {
		if n.TRUE() != nil {
			return &ast.Boolean{
				Boolval: true,
			}
		}
		if n.FALSE() != nil {
			return &ast.Boolean{
				Boolval: false,
			}
		}

		if an := n.An_id_or_type(); an != nil {
			idText := parseAnIdOrType(an)
			if num, err := strconv.Atoi(idText); err == nil {
				c.paramCount++
				return &ast.ParamRef{
					Number:   num,
					Location: n.GetStart().GetStart(),
					Dollar:   true,
				}
			}
			return &ast.A_Expr{
				Name:     &ast.List{Items: []ast.Node{&ast.String{Str: "@"}}},
				Rexpr:    &ast.String{Str: idText},
				Location: n.GetStart().GetStart(),
			}
		}
		c.paramCount++
		return &ast.ParamRef{
			Number:   c.paramCount,
			Location: n.GetStart().GetStart(),
			Dollar:   true,
		}
	}
	return nil
}

func (c *cc) convertWildCardField(n *parser.Result_columnContext) *ast.ColumnRef {
	prefixCtx := n.Opt_id_prefix()
	prefix := c.convertOptIdPrefix(prefixCtx)

	items := []ast.Node{}
	if prefix != "" {
		items = append(items, NewIdentifier(prefix))
	}

	items = append(items, &ast.A_Star{})
	return &ast.ColumnRef{
		Fields:   &ast.List{Items: items},
		Location: n.GetStart().GetStart(),
	}
}

func (c *cc) convertOptIdPrefix(ctx parser.IOpt_id_prefixContext) string {
	if ctx == nil {
		return ""
	}
	if ctx.An_id() != nil {
		return ctx.An_id().GetText()
	}
	return ""
}

func (c *cc) convertCreate_table_stmtContext(n *parser.Create_table_stmtContext) ast.Node {
	stmt := &ast.CreateTableStmt{
		Name:        parseTableName(n.Simple_table_ref().Simple_table_ref_core()),
		IfNotExists: n.EXISTS() != nil,
	}
	for _, idef := range n.AllCreate_table_entry() {
		if def, ok := idef.(*parser.Create_table_entryContext); ok {
			switch {
			case def.Column_schema() != nil:
				if colCtx, ok := def.Column_schema().(*parser.Column_schemaContext); ok {
					colDef := c.convertColumnSchema(colCtx)
					if colDef != nil {
						stmt.Cols = append(stmt.Cols, colDef)
					}
				}
			case def.Table_constraint() != nil:
				if conCtx, ok := def.Table_constraint().(*parser.Table_constraintContext); ok {
					switch {
					case conCtx.PRIMARY() != nil && conCtx.KEY() != nil:
						for _, cname := range conCtx.AllAn_id() {
							for _, col := range stmt.Cols {
								if col.Colname == parseAnId(cname) {
									col.PrimaryKey = true
								}
							}
						}
					case conCtx.PARTITION() != nil && conCtx.BY() != nil:
						_ = conCtx
						// todo: partition by constraint
					case conCtx.ORDER() != nil && conCtx.BY() != nil:
						_ = conCtx
						// todo: order by constraint
					}
				}

			case def.Table_index() != nil:
				if indCtx, ok := def.Table_index().(*parser.Table_indexContext); ok {
					_ = indCtx
					// todo
				}
			case def.Family_entry() != nil:
				if famCtx, ok := def.Family_entry().(*parser.Family_entryContext); ok {
					_ = famCtx
					// todo
				}
			case def.Changefeed() != nil: // таблица ориентированная
				if cgfCtx, ok := def.Changefeed().(*parser.ChangefeedContext); ok {
					_ = cgfCtx
					// todo
				}
			}
		}
	}
	return stmt
}

func (c *cc) convertColumnSchema(n *parser.Column_schemaContext) *ast.ColumnDef {

	col := &ast.ColumnDef{}

	if anId := n.An_id_schema(); anId != nil {
		col.Colname = identifier(parseAnIdSchema(anId))
	}
	if tnb := n.Type_name_or_bind(); tnb != nil {
		col.TypeName = c.convertTypeNameOrBind(tnb)
	}
	if colCons := n.Opt_column_constraints(); colCons != nil {
		col.IsNotNull = colCons.NOT() != nil && colCons.NULL() != nil
		//todo: cover exprs if needed
	}
	// todo: family

	return col
}

func (c *cc) convertTypeNameOrBind(n parser.IType_name_or_bindContext) *ast.TypeName {
	if t := n.Type_name(); t != nil {
		return c.convertTypeName(t)
	} else if b := n.Bind_parameter(); b != nil {
		return &ast.TypeName{Name: "BIND:" + identifier(parseAnIdOrType(b.An_id_or_type()))}
	}
	return nil
}

func (c *cc) convertTypeName(n parser.IType_nameContext) *ast.TypeName {
	// todo: implement all types
	return &ast.TypeName{
		Name: n.GetText(),
	}
}

func (c *cc) convertSqlStmtCore(n parser.ISql_stmt_coreContext) ast.Node {
	if n == nil {
		return nil
	}

	if stmt := n.Pragma_stmt(); stmt != nil {
		return c.convert(stmt)
	}
	if stmt := n.Select_stmt(); stmt != nil {
		return c.convert(stmt)
	}
	if stmt := n.Named_nodes_stmt(); stmt != nil {
		return c.convert(stmt)
	}
	if stmt := n.Create_table_stmt(); stmt != nil {
		return c.convert(stmt)
	}
	if stmt := n.Drop_table_stmt(); stmt != nil {
		return c.convert(stmt)
	}
	if stmt := n.Use_stmt(); stmt != nil {
		return c.convert(stmt)
	}
	if stmt := n.Into_table_stmt(); stmt != nil {
		return c.convert(stmt)
	}
	if stmt := n.Commit_stmt(); stmt != nil {
		return c.convert(stmt)
	}
	if stmt := n.Update_stmt(); stmt != nil {
		return c.convert(stmt)
	}
	if stmt := n.Delete_stmt(); stmt != nil {
		return c.convert(stmt)
	}
	if stmt := n.Rollback_stmt(); stmt != nil {
		return c.convert(stmt)
	}
	if stmt := n.Declare_stmt(); stmt != nil {
		return c.convert(stmt)
	}
	if stmt := n.Import_stmt(); stmt != nil {
		return c.convert(stmt)
	}
	if stmt := n.Export_stmt(); stmt != nil {
		return c.convert(stmt)
	}
	if stmt := n.Alter_table_stmt(); stmt != nil {
		return c.convert(stmt)
	}
	if stmt := n.Alter_external_table_stmt(); stmt != nil {
		return c.convert(stmt)
	}
	if stmt := n.Do_stmt(); stmt != nil {
		return c.convert(stmt)
	}
	if stmt := n.Define_action_or_subquery_stmt(); stmt != nil {
		return c.convert(stmt)
	}
	if stmt := n.If_stmt(); stmt != nil {
		return c.convert(stmt)
	}
	if stmt := n.For_stmt(); stmt != nil {
		return c.convert(stmt)
	}
	if stmt := n.Values_stmt(); stmt != nil {
		return c.convert(stmt)
	}
	if stmt := n.Create_user_stmt(); stmt != nil {
		return c.convert(stmt)
	}
	if stmt := n.Alter_user_stmt(); stmt != nil {
		return c.convert(stmt)
	}
	if stmt := n.Create_group_stmt(); stmt != nil {
		return c.convert(stmt)
	}
	if stmt := n.Alter_group_stmt(); stmt != nil {
		return c.convert(stmt)
	}
	if stmt := n.Drop_role_stmt(); stmt != nil {
		return c.convert(stmt)
	}
	if stmt := n.Create_object_stmt(); stmt != nil {
		return c.convert(stmt)
	}
	if stmt := n.Alter_object_stmt(); stmt != nil {
		return c.convert(stmt)
	}
	if stmt := n.Drop_object_stmt(); stmt != nil {
		return c.convert(stmt)
	}
	if stmt := n.Create_external_data_source_stmt(); stmt != nil {
		return c.convert(stmt)
	}
	if stmt := n.Alter_external_data_source_stmt(); stmt != nil {
		return c.convert(stmt)
	}
	if stmt := n.Drop_external_data_source_stmt(); stmt != nil {
		return c.convert(stmt)
	}
	if stmt := n.Create_replication_stmt(); stmt != nil {
		return c.convert(stmt)
	}
	if stmt := n.Drop_replication_stmt(); stmt != nil {
		return c.convert(stmt)
	}
	if stmt := n.Create_topic_stmt(); stmt != nil {
		return c.convert(stmt)
	}
	if stmt := n.Alter_topic_stmt(); stmt != nil {
		return c.convert(stmt)
	}
	if stmt := n.Drop_topic_stmt(); stmt != nil {
		return c.convert(stmt)
	}
	if stmt := n.Grant_permissions_stmt(); stmt != nil {
		return c.convert(stmt)
	}
	if stmt := n.Revoke_permissions_stmt(); stmt != nil {
		return c.convert(stmt)
	}
	if stmt := n.Alter_table_store_stmt(); stmt != nil {
		return c.convert(stmt)
	}
	if stmt := n.Upsert_object_stmt(); stmt != nil {
		return c.convert(stmt)
	}
	if stmt := n.Create_view_stmt(); stmt != nil {
		return c.convert(stmt)
	}
	if stmt := n.Drop_view_stmt(); stmt != nil {
		return c.convert(stmt)
	}
	if stmt := n.Alter_replication_stmt(); stmt != nil {
		return c.convert(stmt)
	}
	if stmt := n.Create_resource_pool_stmt(); stmt != nil {
		return c.convert(stmt)
	}
	if stmt := n.Alter_resource_pool_stmt(); stmt != nil {
		return c.convert(stmt)
	}
	if stmt := n.Drop_resource_pool_stmt(); stmt != nil {
		return c.convert(stmt)
	}
	if stmt := n.Create_backup_collection_stmt(); stmt != nil {
		return c.convert(stmt)
	}
	if stmt := n.Alter_backup_collection_stmt(); stmt != nil {
		return c.convert(stmt)
	}
	if stmt := n.Drop_backup_collection_stmt(); stmt != nil {
		return c.convert(stmt)
	}
	if stmt := n.Analyze_stmt(); stmt != nil {
		return c.convert(stmt)
	}
	if stmt := n.Create_resource_pool_classifier_stmt(); stmt != nil {
		return c.convert(stmt)
	}
	if stmt := n.Alter_resource_pool_classifier_stmt(); stmt != nil {
		return c.convert(stmt)
	}
	if stmt := n.Drop_resource_pool_classifier_stmt(); stmt != nil {
		return c.convert(stmt)
	}
	if stmt := n.Backup_stmt(); stmt != nil {
		return c.convert(stmt)
	}
	if stmt := n.Restore_stmt(); stmt != nil {
		return c.convert(stmt)
	}
	if stmt := n.Alter_sequence_stmt(); stmt != nil {
		return c.convert(stmt)
	}
	return nil
}

func (c *cc) convertExpr(n *parser.ExprContext) ast.Node {
	if n == nil {
		return nil
	}

	if tn := n.Type_name_composite(); tn != nil {
		return c.convertTypeNameComposite(tn)
	}

	orSubs := n.AllOr_subexpr()
	if len(orSubs) == 0 {
		return nil
	}

	orSub, ok := orSubs[0].(*parser.Or_subexprContext)
	if !ok {
		return nil
	}

	left := c.convertOrSubExpr(orSub)
	for i := 1; i < len(orSubs); i++ {
		orSub, ok = orSubs[i].(*parser.Or_subexprContext)
		if !ok {
			return nil
		}
		right := c.convertOrSubExpr(orSub)
		left = &ast.BoolExpr{
			Boolop: ast.BoolExprTypeOr,
			Args: &ast.List{
				Items: []ast.Node{
					left,
					right,
				},
			},
		}
	}
	return left
}

func (c *cc) convertOrSubExpr(n *parser.Or_subexprContext) ast.Node {
	if n == nil {
		return nil
	}
	andSubs := n.AllAnd_subexpr()
	if len(andSubs) == 0 {
		return nil
	}
	andSub, ok := andSubs[0].(*parser.And_subexprContext)
	if !ok {
		return nil
	}

	left := c.convertAndSubexpr(andSub)
	for i := 1; i < len(andSubs); i++ {
		andSub, ok = andSubs[i].(*parser.And_subexprContext)
		if !ok {
			return nil
		}
		right := c.convertAndSubexpr(andSub)
		left = &ast.BoolExpr{
			Boolop: ast.BoolExprTypeAnd,
			Args: &ast.List{
				Items: []ast.Node{
					left,
					right,
				},
			},
		}
	}
	return left
}

func (c *cc) convertAndSubexpr(n *parser.And_subexprContext) ast.Node {
	if n == nil {
		return nil
	}

	xors := n.AllXor_subexpr()
	if len(xors) == 0 {
		return nil
	}

	xor, ok := xors[0].(*parser.Xor_subexprContext)
	if !ok {
		return nil
	}

	left := c.convertXorSubexpr(xor)
	for i := 1; i < len(xors); i++ {
		xor, ok = xors[i].(*parser.Xor_subexprContext)
		if !ok {
			return nil
		}
		right := c.convertXorSubexpr(xor)
		left = &ast.A_Expr{
			Name:  &ast.List{Items: []ast.Node{&ast.String{Str: "XOR"}}}, // !! debug !!
			Lexpr: left,
			Rexpr: right,
		}
	}
	return left
}

func (c *cc) convertXorSubexpr(n *parser.Xor_subexprContext) ast.Node {
	if n == nil {
		return nil
	}
	es := n.Eq_subexpr()
	if es == nil {
		return nil
	}
	subExpr, ok := es.(*parser.Eq_subexprContext)
	if !ok {
		return nil
	}
	base := c.convertEqSubexpr(subExpr)
	if cond := n.Cond_expr(); cond != nil {
		return c.convertCondExpr(cond, base)
	}
	return base
}

func (c *cc) convertEqSubexpr(n *parser.Eq_subexprContext) ast.Node {
	if n == nil {
		return nil
	}
	neqList := n.AllNeq_subexpr()
	if len(neqList) == 0 {
		return nil
	}
	neq, ok := neqList[0].(*parser.Neq_subexprContext)
	if !ok {
		return nil
	}
	left := c.convertNeqSubexpr(neq)
	ops := c.collectComparisonOps(n)
	for i := 1; i < len(neqList); i++ {
		neq, ok = neqList[i].(*parser.Neq_subexprContext)
		if !ok {
			return nil
		}
		right := c.convertNeqSubexpr(neq)
		opText := ops[i-1].GetText()
		left = &ast.A_Expr{
			Name:  &ast.List{Items: []ast.Node{&ast.String{Str: opText}}},
			Lexpr: left,
			Rexpr: right,
		}
	}
	return left
}

func (c *cc) collectComparisonOps(n parser.IEq_subexprContext) []antlr.TerminalNode {
	var ops []antlr.TerminalNode
	for _, child := range n.GetChildren() {
		if tn, ok := child.(antlr.TerminalNode); ok {
			switch tn.GetText() {
			case "<", "<=", ">", ">=":
				ops = append(ops, tn)
			}
		}
	}
	return ops
}

func (c *cc) convertNeqSubexpr(n *parser.Neq_subexprContext) ast.Node {
	if n == nil {
		return nil
	}
	bitList := n.AllBit_subexpr()
	if len(bitList) == 0 {
		return nil
	}

	bl, ok := bitList[0].(*parser.Bit_subexprContext)
	if !ok {
		return nil
	}
	left := c.convertBitSubexpr(bl)
	ops := c.collectBitwiseOps(n)
	for i := 1; i < len(bitList); i++ {
		bl, ok = bitList[i].(*parser.Bit_subexprContext)
		if !ok {
			return nil
		}
		right := c.convertBitSubexpr(bl)
		opText := ops[i-1].GetText()
		left = &ast.A_Expr{
			Name:  &ast.List{Items: []ast.Node{&ast.String{Str: opText}}},
			Lexpr: left,
			Rexpr: right,
		}
	}

	if n.Double_question() != nil {
		nextCtx := n.Neq_subexpr()
		if nextCtx != nil {
			neq, ok2 := nextCtx.(*parser.Neq_subexprContext)
			if !ok2 {
				return nil
			}
			right := c.convertNeqSubexpr(neq)
			left = &ast.A_Expr{
				Name:  &ast.List{Items: []ast.Node{&ast.String{Str: "??"}}},
				Lexpr: left,
				Rexpr: right,
			}
		}
	} else {
		// !! debug !!
		qCount := len(n.AllQUESTION())
		if qCount > 0 {
			questionOp := "?"
			if qCount > 1 {
				questionOp = strings.Repeat("?", qCount)
			}
			left = &ast.A_Expr{
				Name:  &ast.List{Items: []ast.Node{&ast.String{Str: questionOp}}},
				Lexpr: left,
			}
		}
	}

	return left
}

func (c *cc) collectBitwiseOps(ctx parser.INeq_subexprContext) []antlr.TerminalNode {
	var ops []antlr.TerminalNode
	children := ctx.GetChildren()
	for _, child := range children {
		if tn, ok := child.(antlr.TerminalNode); ok {
			txt := tn.GetText()
			switch txt {
			case "<<", ">>", "<<|", ">>|", "&", "|", "^":
				ops = append(ops, tn)
			}
		}
	}
	return ops
}

func (c *cc) convert(node node) ast.Node {
	switch n := node.(type) {

	case *parser.Sql_stmt_coreContext:
		return c.convertSqlStmtCore(n)

	case *parser.Create_table_stmtContext:
		return c.convertCreate_table_stmtContext(n)

	case *parser.Select_stmtContext:
		return c.convertSelectStmtContext(n)

	case *parser.Select_coreContext:
		return c.convertSelectCoreContext(n)

	default:
		return todo("convert(case=default)", n)
	}
}
