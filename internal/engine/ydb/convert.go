package ydb

import (
	"log"
	"strconv"
	"strings"

	"github.com/antlr4-go/antlr/v4"
	"github.com/sqlc-dev/sqlc/internal/debug"
	"github.com/sqlc-dev/sqlc/internal/sql/ast"
	parser "github.com/ydb-platform/yql-parsers/go"
)

type cc struct {
	parser.BaseYQLVisitor
	content string
}

func (c *cc) pos(token antlr.Token) int {
	if token == nil {
		return 0
	}
	runeIdx := token.GetStart()
	return byteOffsetFromRuneIndex(c.content, runeIdx)
}

type node interface {
	GetParser() antlr.Parser
}

func todo(funcname string, n node) *ast.TODO {
	if debug.Active {
		log.Printf("ydb.%s: Unknown node type %T\n", funcname, n)
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

func stripQuotes(s string) string {
	if len(s) >= 2 && (s[0] == '\'' || s[0] == '"') && s[0] == s[len(s)-1] {
		return s[1 : len(s)-1]
	}
	return s
}

func NewIdentifier(t string) *ast.String {
	return &ast.String{Str: identifier(t)}
}

func (c *cc) VisitDrop_role_stmt(ctx *parser.Drop_role_stmtContext) interface{} {
	if ctx.DROP() == nil || (ctx.USER() == nil && ctx.GROUP() == nil) || len(ctx.AllRole_name()) == 0 {
		return todo("VisitDrop_role_stmt", ctx)
	}

	stmt := &ast.DropRoleStmt{
		MissingOk: ctx.IF() != nil && ctx.EXISTS() != nil,
		Roles:     &ast.List{},
	}

	for _, role := range ctx.AllRole_name() {
		member, isParam, _ := c.extractRoleSpec(role, ast.RoleSpecType(1))
		if member == nil {
			return todo("VisitDrop_role_stmt", role)
		}

		if debug.Active && isParam {
			log.Printf("YDB does not currently support parameters in the DROP ROLE statement")
		}

		stmt.Roles.Items = append(stmt.Roles.Items, member)
	}

	return stmt
}

func (c *cc) VisitAlter_group_stmt(ctx *parser.Alter_group_stmtContext) interface{} {
	if ctx.ALTER() == nil || ctx.GROUP() == nil || len(ctx.AllRole_name()) == 0 {
		return todo("VisitAlter_group_stmt", ctx)
	}
	role, paramFlag, _ := c.extractRoleSpec(ctx.Role_name(0), ast.RoleSpecType(1))
	if role == nil {
		return todo("VisitAlter_group_stmt", ctx)
	}

	if debug.Active && paramFlag {
		log.Printf("YDB does not currently support parameters in the ALTER GROUP statement")
	}

	stmt := &ast.AlterRoleStmt{
		Role:    role,
		Action:  1,
		Options: &ast.List{},
	}

	switch {
	case ctx.RENAME() != nil && ctx.TO() != nil && len(ctx.AllRole_name()) > 1:
		newName, ok := ctx.Role_name(1).Accept(c).(ast.Node)
		if !ok {
			return todo("VisitAlter_group_stmt", ctx.Role_name(1))
		}
		action := "rename"

		defElem := &ast.DefElem{
			Defname:   &action,
			Defaction: ast.DefElemAction(1),
			Location:  c.pos(ctx.Role_name(1).GetStart()),
		}

		bindFlag := true
		switch v := newName.(type) {
		case *ast.A_Const:
			switch val := v.Val.(type) {
			case *ast.String:
				bindFlag = false
				defElem.Arg = val
			case *ast.Boolean:
				defElem.Arg = val
			default:
				return todo("VisitAlter_group_stmt", ctx.Role_name(1))
			}
		case *ast.ParamRef, *ast.A_Expr:
			defElem.Arg = newName
		default:
			return todo("VisitAlter_group_stmt", ctx.Role_name(1))
		}

		if debug.Active && !paramFlag && bindFlag {
			log.Printf("YDB does not currently support parameters in the ALTER GROUP statement")
		}

		stmt.Options.Items = append(stmt.Options.Items, defElem)

	case (ctx.ADD() != nil || ctx.DROP() != nil) && len(ctx.AllRole_name()) > 1:
		defname := "rolemembers"
		optionList := &ast.List{}
		for _, role := range ctx.AllRole_name()[1:] {
			member, isParam, _ := c.extractRoleSpec(role, ast.RoleSpecType(1))
			if member == nil {
				return todo("VisitAlter_group_stmt", role)
			}

			if debug.Active && isParam && !paramFlag {
				log.Printf("YDB does not currently support parameters in the ALTER GROUP statement")
			}

			optionList.Items = append(optionList.Items, member)
		}

		var action ast.DefElemAction
		if ctx.ADD() != nil {
			action = 3
		} else {
			action = 4
		}

		stmt.Options.Items = append(stmt.Options.Items, &ast.DefElem{
			Defname:   &defname,
			Arg:       optionList,
			Defaction: action,
			Location:  c.pos(ctx.Role_name(1).GetStart()),
		})
	}

	return stmt
}

func (c *cc) VisitAlter_user_stmt(ctx *parser.Alter_user_stmtContext) interface{} {
	if ctx.ALTER() == nil || ctx.USER() == nil || len(ctx.AllRole_name()) == 0 {
		return todo("VisitAlter_user_stmt", ctx)
	}

	role, paramFlag, _ := c.extractRoleSpec(ctx.Role_name(0), ast.RoleSpecType(1))
	if role == nil {
		return todo("VisitAlter_group_stmt", ctx)
	}

	if debug.Active && paramFlag {
		log.Printf("YDB does not currently support parameters in the ALTER USER statement")
	}

	stmt := &ast.AlterRoleStmt{
		Role:    role,
		Action:  1,
		Options: &ast.List{},
	}

	switch {
	case ctx.RENAME() != nil && ctx.TO() != nil && len(ctx.AllRole_name()) > 1:
		newName, ok := ctx.Role_name(1).Accept(c).(ast.Node)
		if !ok {
			return todo("VisitAlter_user_stmt", ctx.Role_name(1))
		}
		action := "rename"

		defElem := &ast.DefElem{
			Defname:   &action,
			Defaction: ast.DefElemAction(1),
			Location:  c.pos(ctx.Role_name(1).GetStart()),
		}

		bindFlag := true
		switch v := newName.(type) {
		case *ast.A_Const:
			switch val := v.Val.(type) {
			case *ast.String:
				bindFlag = false
				defElem.Arg = val
			case *ast.Boolean:
				defElem.Arg = val
			default:
				return todo("VisitAlter_user_stmt", ctx.Role_name(1))
			}
		case *ast.ParamRef, *ast.A_Expr:
			defElem.Arg = newName
		default:
			return todo("VisitAlter_user_stmt", ctx.Role_name(1))
		}

		if debug.Active && !paramFlag && bindFlag {
			log.Printf("YDB does not currently support parameters in the ALTER USER statement")
		}

		stmt.Options.Items = append(stmt.Options.Items, defElem)

	case len(ctx.AllUser_option()) > 0:
		for _, opt := range ctx.AllUser_option() {
			if temp := opt.Accept(c); temp != nil {
				var node, ok = temp.(ast.Node)
				if !ok {
					return todo("VisitAlter_user_stmt", opt)
				}
				stmt.Options.Items = append(stmt.Options.Items, node)
			}
		}
	}

	return stmt
}

func (c *cc) VisitCreate_group_stmt(ctx *parser.Create_group_stmtContext) interface{} {
	if ctx.CREATE() == nil || ctx.GROUP() == nil || len(ctx.AllRole_name()) == 0 {
		return todo("VisitCreate_group_stmt", ctx)
	}
	groupName, ok := ctx.Role_name(0).Accept(c).(ast.Node)
	if !ok {
		return todo("VisitCreate_group_stmt", ctx.Role_name(0))
	}

	stmt := &ast.CreateRoleStmt{
		StmtType: ast.RoleStmtType(3),
		Options:  &ast.List{},
	}

	paramFlag := true
	switch v := groupName.(type) {
	case *ast.A_Const:
		switch val := v.Val.(type) {
		case *ast.String:
			paramFlag = false
			stmt.Role = &val.Str
		case *ast.Boolean:
			stmt.BindRole = groupName
		default:
			return todo("VisitCreate_group_stmt", ctx.Role_name(0))
		}
	case *ast.ParamRef, *ast.A_Expr:
		stmt.BindRole = groupName
	default:
		return todo("VisitCreate_group_stmt", ctx.Role_name(0))
	}

	if debug.Active && paramFlag {
		log.Printf("YDB does not currently support parameters in the CREATE GROUP statement")
	}

	if ctx.WITH() != nil && ctx.USER() != nil && len(ctx.AllRole_name()) > 1 {
		defname := "rolemembers"
		optionList := &ast.List{}
		for _, role := range ctx.AllRole_name()[1:] {
			member, isParam, _ := c.extractRoleSpec(role, ast.RoleSpecType(1))
			if member == nil {
				return todo("VisitCreate_group_stmt", role)
			}

			if debug.Active && isParam && !paramFlag {
				log.Printf("YDB does not currently support parameters in the CREATE GROUP statement")
			}

			optionList.Items = append(optionList.Items, member)
		}

		stmt.Options.Items = append(stmt.Options.Items, &ast.DefElem{
			Defname:  &defname,
			Arg:      optionList,
			Location: c.pos(ctx.Role_name(1).GetStart()),
		})
	}

	return stmt
}

func (c *cc) VisitUse_stmt(ctx *parser.Use_stmtContext) interface{} {
	if ctx.USE() != nil && ctx.Cluster_expr() != nil {
		clusterExpr, ok := ctx.Cluster_expr().Accept(c).(ast.Node)
		if !ok {
			return todo("VisitUse_stmt", ctx.Cluster_expr())
		}
		stmt := &ast.UseStmt{
			Xpr:      clusterExpr,
			Location: c.pos(ctx.Cluster_expr().GetStart()),
		}
		return stmt
	}
	return todo("VisitUse_stmt", ctx)
}

func (c *cc) VisitCluster_expr(ctx *parser.Cluster_exprContext) interface{} {
	var node ast.Node

	switch {
	case ctx.Pure_column_or_named() != nil:
		pureCtx := ctx.Pure_column_or_named()
		if anID := pureCtx.An_id(); anID != nil {
			name := parseAnId(anID)
			node = &ast.ColumnRef{
				Fields:   &ast.List{Items: []ast.Node{NewIdentifier(name)}},
				Location: c.pos(anID.GetStart()),
			}
		} else if bp := pureCtx.Bind_parameter(); bp != nil {
			temp, ok := bp.Accept(c).(ast.Node)
			if !ok {
				return todo("VisitCluster_expr", bp)
			}
			node = temp
		}
	case ctx.ASTERISK() != nil:
		node = &ast.A_Star{}
	default:
		return todo("VisitCluster_expr", ctx)
	}

	if ctx.An_id() != nil && ctx.COLON() != nil {
		name := parseAnId(ctx.An_id())
		return &ast.A_Expr{
			Name:     &ast.List{Items: []ast.Node{&ast.String{Str: ":"}}},
			Lexpr:    &ast.String{Str: name},
			Rexpr:    node,
			Location: c.pos(ctx.GetStart()),
		}
	}

	return node
}

func (c *cc) VisitCreate_user_stmt(ctx *parser.Create_user_stmtContext) interface{} {
	if ctx.CREATE() == nil || ctx.USER() == nil || ctx.Role_name() == nil {
		return todo("VisitCreate_user_stmt", ctx)
	}
	roleNode, ok := ctx.Role_name().Accept(c).(ast.Node)
	if !ok {
		return todo("VisitCreate_user_stmt", ctx.Role_name())
	}

	stmt := &ast.CreateRoleStmt{
		StmtType: ast.RoleStmtType(2),
		Options:  &ast.List{},
	}

	paramFlag := true
	switch v := roleNode.(type) {
	case *ast.A_Const:
		switch val := v.Val.(type) {
		case *ast.String:
			paramFlag = false
			stmt.Role = &val.Str
		case *ast.Boolean:
			stmt.BindRole = roleNode
		default:
			return todo("VisitCreate_user_stmt", ctx.Role_name())
		}
	case *ast.ParamRef, *ast.A_Expr:
		stmt.BindRole = roleNode
	default:
		return todo("VisitCreate_user_stmt", ctx.Role_name())
	}

	if debug.Active && paramFlag {
		log.Printf("YDB does not currently support parameters in the CREATE USER statement")
	}

	if len(ctx.AllUser_option()) > 0 {
		options := []ast.Node{}
		for _, opt := range ctx.AllUser_option() {
			if temp := opt.Accept(c); temp != nil {
				node, ok := temp.(ast.Node)
				if !ok {
					return todo("VisitCreate_user_stmt", opt)
				}
				options = append(options, node)
			}
		}
		if len(options) > 0 {
			stmt.Options = &ast.List{Items: options}
		}
	}
	return stmt
}

func (c *cc) VisitUser_option(ctx *parser.User_optionContext) interface{} {
	switch {
	case ctx.Authentication_option() != nil:
		aOpt := ctx.Authentication_option()
		if pOpt := aOpt.Password_option(); pOpt != nil {
			if pOpt.PASSWORD() != nil {
				name := "password"
				pValue := pOpt.Password_value()
				var password ast.Node
				if pValue.STRING_VALUE() != nil {
					password = &ast.String{Str: stripQuotes(pValue.STRING_VALUE().GetText())}
				} else {
					password = &ast.Null{}
				}
				return &ast.DefElem{
					Defname:  &name,
					Arg:      password,
					Location: c.pos(pOpt.GetStart()),
				}
			}
		} else if hOpt := aOpt.Hash_option(); hOpt != nil {
			if debug.Active {
				log.Printf("YDB does not currently support HASH in CREATE USER statement")
			}
			var pass string
			if hOpt.HASH() != nil && hOpt.STRING_VALUE() != nil {
				pass = stripQuotes(hOpt.STRING_VALUE().GetText())
			}
			name := "hash"
			return &ast.DefElem{
				Defname:  &name,
				Arg:      &ast.String{Str: pass},
				Location: c.pos(hOpt.GetStart()),
			}
		}

	case ctx.Login_option() != nil:
		lOpt := ctx.Login_option()
		var name string
		if lOpt.LOGIN() != nil {
			name = "login"
		} else if lOpt.NOLOGIN() != nil {
			name = "nologin"
		}
		return &ast.DefElem{
			Defname:  &name,
			Arg:      &ast.Boolean{Boolval: lOpt.LOGIN() != nil},
			Location: c.pos(lOpt.GetStart()),
		}
	default:
		return todo("VisitUser_option", ctx)
	}
	return todo("VisitUser_option", ctx)
}

func (c *cc) VisitRole_name(ctx *parser.Role_nameContext) interface{} {
	switch {
	case ctx.An_id_or_type() != nil:
		name := parseAnIdOrType(ctx.An_id_or_type())
		return &ast.A_Const{Val: NewIdentifier(name), Location: c.pos(ctx.An_id_or_type().GetStart())}
	case ctx.Bind_parameter() != nil:
		bindPar, ok := ctx.Bind_parameter().Accept(c).(ast.Node)
		if !ok {
			return todo("VisitRole_name", ctx.Bind_parameter())
		}
		return bindPar
	}
	return todo("VisitRole_name", ctx)
}

func (c *cc) VisitCommit_stmt(ctx *parser.Commit_stmtContext) interface{} {
	if ctx.COMMIT() != nil {
		return &ast.TransactionStmt{Kind: ast.TransactionStmtKind(3)}
	}
	return todo("VisitCommit_stmt", ctx)
}

func (c *cc) VisitRollback_stmt(ctx *parser.Rollback_stmtContext) interface{} {
	if ctx.ROLLBACK() != nil {
		return &ast.TransactionStmt{Kind: ast.TransactionStmtKind(4)}
	}
	return todo("VisitRollback_stmt", ctx)
}

func (c *cc) VisitAlter_table_stmt(ctx *parser.Alter_table_stmtContext) interface{} {
	if ctx.ALTER() == nil || ctx.TABLE() == nil || ctx.Simple_table_ref() == nil || len(ctx.AllAlter_table_action()) == 0 {
		return todo("VisitAlter_table_stmt", ctx)
	}

	stmt := &ast.AlterTableStmt{
		Table: parseTableName(ctx.Simple_table_ref().Simple_table_ref_core()),
		Cmds:  &ast.List{},
	}

	for _, action := range ctx.AllAlter_table_action() {
		if action == nil {
			continue
		}

		switch {
		case action.Alter_table_add_column() != nil:
			ac := action.Alter_table_add_column()
			if ac.ADD() != nil && ac.Column_schema() != nil {
				temp, ok := ac.Column_schema().Accept(c).(ast.Node)
				if !ok {
					return todo("VisitAlter_table_stmt", ac.Column_schema())
				}
				columnDef, ok := temp.(*ast.ColumnDef)
				if !ok {
					return todo("VisitAlter_table_stmt", ac.Column_schema())
				}
				stmt.Cmds.Items = append(stmt.Cmds.Items, &ast.AlterTableCmd{
					Name:    &columnDef.Colname,
					Subtype: ast.AT_AddColumn,
					Def:     columnDef,
				})
			}
		case action.Alter_table_drop_column() != nil:
			ac := action.Alter_table_drop_column()
			if ac.DROP() != nil && ac.An_id() != nil {
				name := parseAnId(ac.An_id())
				stmt.Cmds.Items = append(stmt.Cmds.Items, &ast.AlterTableCmd{
					Name:    &name,
					Subtype: ast.AT_DropColumn,
				})
			}
		case action.Alter_table_alter_column_drop_not_null() != nil:
			ac := action.Alter_table_alter_column_drop_not_null()
			if ac.DROP() != nil && ac.NOT() != nil && ac.NULL() != nil && ac.An_id() != nil {
				name := parseAnId(ac.An_id())
				stmt.Cmds.Items = append(stmt.Cmds.Items, &ast.AlterTableCmd{
					Name:    &name,
					Subtype: ast.AT_DropNotNull,
				})
			}
		case action.Alter_table_rename_to() != nil:
			ac := action.Alter_table_rename_to()
			if ac.RENAME() != nil && ac.TO() != nil && ac.An_id_table() != nil {
				// FIXME: Returning here may be incorrect if there are multiple specs
				newName := parseAnIdTable(ac.An_id_table())
				return &ast.RenameTableStmt{
					Table:   stmt.Table,
					NewName: &newName,
				}
			}
		case action.Alter_table_add_index() != nil,
			action.Alter_table_drop_index() != nil,
			action.Alter_table_add_column_family() != nil,
			action.Alter_table_alter_column_family() != nil,
			action.Alter_table_set_table_setting_uncompat() != nil,
			action.Alter_table_set_table_setting_compat() != nil,
			action.Alter_table_reset_table_setting() != nil,
			action.Alter_table_add_changefeed() != nil,
			action.Alter_table_alter_changefeed() != nil,
			action.Alter_table_drop_changefeed() != nil,
			action.Alter_table_rename_index_to() != nil,
			action.Alter_table_alter_index() != nil:
			// All these actions do not change column schema relevant to sqlc; no-op.
			// Intentionally ignored.
		}
	}

	return stmt
}

func (c *cc) VisitDo_stmt(ctx *parser.Do_stmtContext) interface{} {
	if ctx.DO() == nil || (ctx.Call_action() == nil && ctx.Inline_action() == nil) {
		return todo("VisitDo_stmt", ctx)
	}

	switch {
	case ctx.Call_action() != nil:
		result, ok := ctx.Call_action().Accept(c).(ast.Node)
		if !ok {
			return todo("VisitDo_stmt", ctx.Call_action())
		}
		return result

	case ctx.Inline_action() != nil:
		result, ok := ctx.Inline_action().Accept(c).(ast.Node)
		if !ok {
			return todo("VisitDo_stmt", ctx.Inline_action())
		}
		return result
	}

	return todo("VisitDo_stmt", ctx)
}

func (c *cc) VisitCall_action(ctx *parser.Call_actionContext) interface{} {
	if ctx == nil {
		return todo("VisitCall_action", ctx)
	}
	if ctx.LPAREN() != nil && ctx.RPAREN() != nil {
		funcCall := &ast.FuncCall{
			Funcname: &ast.List{},
			Args:     &ast.List{},
			AggOrder: &ast.List{},
		}

		if ctx.Bind_parameter() != nil {
			bindPar, ok := ctx.Bind_parameter().Accept(c).(ast.Node)
			if !ok {
				return todo("VisitCall_action", ctx.Bind_parameter())
			}
			funcCall.Funcname.Items = append(funcCall.Funcname.Items, bindPar)
		} else if ctx.EMPTY_ACTION() != nil {
			funcCall.Funcname.Items = append(funcCall.Funcname.Items, &ast.String{Str: "EMPTY_ACTION"})
		}

		if ctx.Expr_list() != nil {
			for _, expr := range ctx.Expr_list().AllExpr() {
				exprNode, ok := expr.Accept(c).(ast.Node)
				if !ok {
					return todo("VisitCall_action", expr)
				}
				funcCall.Args.Items = append(funcCall.Args.Items, exprNode)
			}
		}

		return &ast.DoStmt{
			Args: &ast.List{Items: []ast.Node{funcCall}},
		}
	}
	return todo("VisitCall_action", ctx)
}

func (c *cc) VisitInline_action(ctx *parser.Inline_actionContext) interface{} {
	if ctx == nil {
		return todo("VisitInline_action", ctx)
	}
	if ctx.BEGIN() != nil && ctx.END() != nil && ctx.DO() != nil {
		args := &ast.List{}
		if defineBody := ctx.Define_action_or_subquery_body(); defineBody != nil {
			cores := defineBody.AllSql_stmt_core()
			for _, stmtCore := range cores {
				if converted := stmtCore.Accept(c); converted != nil {
					var convertedNode, ok = converted.(ast.Node)
					if !ok {
						return todo("VisitInline_action", stmtCore)
					}
					args.Items = append(args.Items, convertedNode)
				}
			}
		}
		return &ast.DoStmt{Args: args}
	}
	return todo("VisitInline_action", ctx)
}

func (c *cc) VisitDrop_table_stmt(ctx *parser.Drop_table_stmtContext) interface{} {
	if ctx.DROP() != nil && (ctx.TABLESTORE() != nil || (ctx.EXTERNAL() != nil && ctx.TABLE() != nil) || ctx.TABLE() != nil) {
		name := parseTableName(ctx.Simple_table_ref().Simple_table_ref_core())
		stmt := &ast.DropTableStmt{
			IfExists: ctx.IF() != nil && ctx.EXISTS() != nil,
			Tables:   []*ast.TableName{name},
		}
		return stmt
	}
	return todo("VisitDrop_table_stmt", ctx)
}

func (c *cc) VisitDelete_stmt(ctx *parser.Delete_stmtContext) interface{} {
	batch := ctx.BATCH() != nil

	tableName := identifier(ctx.Simple_table_ref().Simple_table_ref_core().GetText())
	rel := &ast.RangeVar{Relname: &tableName}

	var where ast.Node
	if ctx.WHERE() != nil && ctx.Expr() != nil {
		whereNode, ok := ctx.Expr().Accept(c).(ast.Node)
		if !ok {
			return todo("VisitDelete_stmt", ctx.Expr())
		}
		where = whereNode
	}
	var cols *ast.List
	var source ast.Node
	if ctx.ON() != nil && ctx.Into_values_source() != nil {
		nVal := ctx.Into_values_source()
		// todo: handle default values when implemented
		if pureCols := nVal.Pure_column_list(); pureCols != nil {
			cols = &ast.List{}
			for _, anID := range pureCols.AllAn_id() {
				name := identifier(parseAnId(anID))
				cols.Items = append(cols.Items, &ast.ResTarget{
					Name:     &name,
					Location: c.pos(anID.GetStart()),
				})
			}
		}

		valSource := nVal.Values_source()
		if valSource != nil {
			switch {
			case valSource.Values_stmt() != nil:
				stmt := emptySelectStmt()
				temp, ok := valSource.Values_stmt().Accept(c).(ast.Node)
				if !ok {
					return todo("VisitDelete_stmt", valSource.Values_stmt())
				}
				list, ok := temp.(*ast.List)
				if !ok {
					return todo("VisitDelete_stmt", valSource.Values_stmt())
				}
				stmt.ValuesLists = list
				source = stmt
			case valSource.Select_stmt() != nil:
				temp, ok := valSource.Select_stmt().Accept(c).(ast.Node)
				if !ok {
					return todo("VisitDelete_stmt", valSource.Select_stmt())
				}
				source = temp
			}
		}
	}

	returning := &ast.List{}
	if ret := ctx.Returning_columns_list(); ret != nil {
		temp, ok := ret.Accept(c).(ast.Node)
		if !ok {
			return todo("VisitDelete_stmt", ctx.Returning_columns_list())
		}
		returningNode, ok := temp.(*ast.List)
		if !ok {
			return todo("VisitDelete_stmt", ctx.Returning_columns_list())
		}
		returning = returningNode
	}

	stmts := &ast.DeleteStmt{
		Relations:     &ast.List{Items: []ast.Node{rel}},
		WhereClause:   where,
		ReturningList: returning,
		Batch:         batch,
		OnCols:        cols,
		OnSelectStmt:  source,
	}

	return stmts
}

func (c *cc) VisitPragma_stmt(ctx *parser.Pragma_stmtContext) interface{} {
	if ctx.PRAGMA() != nil && ctx.An_id() != nil {
		prefix := ""
		if p := ctx.Opt_id_prefix_or_type(); p != nil {
			prefix = parseAnIdOrType(p.An_id_or_type())
		}
		items := []ast.Node{}
		if prefix != "" {
			items = append(items, &ast.A_Const{Val: NewIdentifier(prefix)})
		}

		name := parseAnId(ctx.An_id())
		items = append(items, &ast.A_Const{Val: NewIdentifier(name)})

		stmt := &ast.Pragma_stmt{
			Name:     &ast.List{Items: items},
			Location: c.pos(ctx.An_id().GetStart()),
		}

		if ctx.EQUALS() != nil {
			stmt.Equals = true
			if val := ctx.Pragma_value(0); val != nil {
				valNode, ok := val.Accept(c).(ast.Node)
				if !ok {
					return todo("VisitPragma_stmt", ctx.Pragma_value(0))
				}
				stmt.Values = &ast.List{Items: []ast.Node{valNode}}
			}
		} else if lp := ctx.LPAREN(); lp != nil {
			values := []ast.Node{}
			for _, v := range ctx.AllPragma_value() {
				valNode, ok := v.Accept(c).(ast.Node)
				if !ok {
					return todo("VisitPragma_stmt", v)
				}
				values = append(values, valNode)
			}
			stmt.Values = &ast.List{Items: values}
		}

		return stmt
	}
	return todo("VisitPragma_stmt", ctx)
}

func (c *cc) VisitPragma_value(ctx *parser.Pragma_valueContext) interface{} {
	switch {
	case ctx.Signed_number() != nil:
		if ctx.Signed_number().Integer() != nil {
			text := ctx.Signed_number().GetText()
			val, err := parseIntegerValue(text)
			if err != nil {
				if debug.Active {
					log.Printf("Failed to parse integer value '%s': %v", text, err)
				}
				return &ast.TODO{}
			}
			return &ast.A_Const{Val: &ast.Integer{Ival: val}, Location: c.pos(ctx.GetStart())}
		}
		if ctx.Signed_number().Real_() != nil {
			text := ctx.Signed_number().GetText()
			return &ast.A_Const{Val: &ast.Float{Str: text}, Location: c.pos(ctx.GetStart())}
		}
	case ctx.STRING_VALUE() != nil:
		val := ctx.STRING_VALUE().GetText()
		if len(val) >= 2 {
			val = val[1 : len(val)-1]
		}
		return &ast.A_Const{Val: &ast.String{Str: val}, Location: c.pos(ctx.GetStart())}
	case ctx.Bool_value() != nil:
		var i bool
		if ctx.Bool_value().TRUE() != nil {
			i = true
		}
		return &ast.A_Const{Val: &ast.Boolean{Boolval: i}, Location: c.pos(ctx.GetStart())}
	case ctx.Bind_parameter() != nil:
		bindPar := ctx.Bind_parameter().Accept(c)
		var bindParNode, ok = bindPar.(ast.Node)
		if !ok {
			return todo("VisitPragma_value", ctx.Bind_parameter())
		}
		return bindParNode
	}

	return todo("VisitPragma_value", ctx)
}

func (c *cc) VisitUpdate_stmt(ctx *parser.Update_stmtContext) interface{} {
	if ctx == nil || ctx.UPDATE() == nil {
		return todo("VisitUpdate_stmt", ctx)
	}
	batch := ctx.BATCH() != nil

	tableName := identifier(ctx.Simple_table_ref().Simple_table_ref_core().GetText())
	rel := &ast.RangeVar{Relname: &tableName}

	var where ast.Node
	var setList *ast.List
	var cols *ast.List
	var source ast.Node

	if ctx.SET() != nil && ctx.Set_clause_choice() != nil {
		nSet := ctx.Set_clause_choice()
		setList = &ast.List{Items: []ast.Node{}}

		switch {
		case nSet.Set_clause_list() != nil:
			for _, clause := range nSet.Set_clause_list().AllSet_clause() {
				targetCtx := clause.Set_target()
				columnName := identifier(targetCtx.Column_name().GetText())
				expr, ok := clause.Expr().Accept(c).(ast.Node)
				if !ok {
					return todo("VisitUpdate_stmt", clause.Expr())
				}
				resTarget := &ast.ResTarget{
					Name:     &columnName,
					Val:      expr,
					Location: c.pos(clause.Expr().GetStart()),
				}
				setList.Items = append(setList.Items, resTarget)
			}

		case nSet.Multiple_column_assignment() != nil:
			multiAssign := nSet.Multiple_column_assignment()
			targetsCtx := multiAssign.Set_target_list()
			valuesCtx := multiAssign.Simple_values_source()

			var colNames []string
			for _, target := range targetsCtx.AllSet_target() {
				targetCtx := target.(*parser.Set_targetContext)
				colNames = append(colNames, targetCtx.Column_name().GetText())
			}

			var rowExpr *ast.RowExpr
			if exprList := valuesCtx.Expr_list(); exprList != nil {
				rowExpr = &ast.RowExpr{
					Args: &ast.List{},
				}
				for _, expr := range exprList.AllExpr() {
					exprNode, ok := expr.Accept(c).(ast.Node)
					if !ok {
						return todo("VisitUpdate_stmt", expr)
					}
					rowExpr.Args.Items = append(rowExpr.Args.Items, exprNode)
				}
			}

			for i, colName := range colNames {
				name := identifier(colName)
				setList.Items = append(setList.Items, &ast.ResTarget{
					Name: &name,
					Val: &ast.MultiAssignRef{
						Source:   rowExpr,
						Colno:    i + 1,
						Ncolumns: len(colNames),
					},
					Location: c.pos(targetsCtx.Set_target(i).GetStart()),
				})
			}
		}

		if ctx.WHERE() != nil && ctx.Expr() != nil {
			whereNode, ok := ctx.Expr().Accept(c).(ast.Node)
			if !ok {
				return todo("VisitUpdate_stmt", ctx.Expr())
			}
			where = whereNode
		}
	} else if ctx.ON() != nil && ctx.Into_values_source() != nil {

		// todo: handle default values when implemented

		nVal := ctx.Into_values_source()

		if pureCols := nVal.Pure_column_list(); pureCols != nil {
			cols = &ast.List{}
			for _, anID := range pureCols.AllAn_id() {
				name := identifier(parseAnId(anID))
				cols.Items = append(cols.Items, &ast.ResTarget{
					Name:     &name,
					Location: c.pos(anID.GetStart()),
				})
			}
		}

		valSource := nVal.Values_source()
		if valSource != nil {
			switch {
			case valSource.Values_stmt() != nil:
				stmt := emptySelectStmt()
				temp, ok := valSource.Values_stmt().Accept(c).(ast.Node)
				if !ok {
					return todo("VisitUpdate_stmt", valSource.Values_stmt())
				}
				list, ok := temp.(*ast.List)
				if !ok {
					return todo("VisitUpdate_stmt", valSource.Values_stmt())
				}
				stmt.ValuesLists = list
				source = stmt
			case valSource.Select_stmt() != nil:
				temp, ok := valSource.Select_stmt().Accept(c).(ast.Node)
				if !ok {
					return todo("VisitUpdate_stmt", valSource.Select_stmt())
				}
				source = temp
			}
		}
	}

	returning := &ast.List{}
	if ret := ctx.Returning_columns_list(); ret != nil {
		temp, ok := ret.Accept(c).(ast.Node)
		if !ok {
			return todo("VisitDelete_stmt", ctx.Returning_columns_list())
		}
		returningNode, ok := temp.(*ast.List)
		if !ok {
			return todo("VisitDelete_stmt", ctx.Returning_columns_list())
		}
		returning = returningNode
	}

	stmts := &ast.UpdateStmt{
		Relations:     &ast.List{Items: []ast.Node{rel}},
		TargetList:    setList,
		WhereClause:   where,
		ReturningList: returning,
		FromClause:    &ast.List{},
		WithClause:    nil,
		Batch:         batch,
		OnCols:        cols,
		OnSelectStmt:  source,
	}

	return stmts
}

func (c *cc) VisitInto_table_stmt(ctx *parser.Into_table_stmtContext) interface{} {
	tableName := identifier(ctx.Into_simple_table_ref().Simple_table_ref().Simple_table_ref_core().GetText())
	rel := &ast.RangeVar{
		Relname:  &tableName,
		Location: c.pos(ctx.Into_simple_table_ref().GetStart()),
	}

	onConflict := &ast.OnConflictClause{}
	switch {
	case ctx.INSERT() != nil && ctx.OR() != nil && ctx.ABORT() != nil:
		onConflict.Action = ast.OnConflictAction_INSERT_OR_ABORT
	case ctx.INSERT() != nil && ctx.OR() != nil && ctx.REVERT() != nil:
		onConflict.Action = ast.OnConflictAction_INSERT_OR_REVERT
	case ctx.INSERT() != nil && ctx.OR() != nil && ctx.IGNORE() != nil:
		onConflict.Action = ast.OnConflictAction_INSERT_OR_IGNORE
	case ctx.UPSERT() != nil:
		onConflict.Action = ast.OnConflictAction_UPSERT
	case ctx.REPLACE() != nil:
		onConflict.Action = ast.OnConflictAction_REPLACE
	}

	var cols *ast.List
	var source ast.Node
	if nVal := ctx.Into_values_source(); nVal != nil {
		// todo: handle default values when implemented

		if pureCols := nVal.Pure_column_list(); pureCols != nil {
			cols = &ast.List{}
			for _, anID := range pureCols.AllAn_id() {
				name := identifier(parseAnId(anID))
				cols.Items = append(cols.Items, &ast.ResTarget{
					Name:     &name,
					Location: c.pos(anID.GetStart()),
				})
			}
		}

		valSource := nVal.Values_source()
		if valSource != nil {
			switch {
			case valSource.Values_stmt() != nil:
				stmt := emptySelectStmt()
				temp, ok := valSource.Values_stmt().Accept(c).(ast.Node)
				if !ok {
					return todo("VisitInto_table_stmt", valSource.Values_stmt())
				}
				stmtNode, ok := temp.(*ast.List)
				if !ok {
					return todo("VisitInto_table_stmt", valSource.Values_stmt())
				}
				stmt.ValuesLists = stmtNode
				source = stmt
			case valSource.Select_stmt() != nil:
				sourceNode, ok := valSource.Select_stmt().Accept(c).(ast.Node)
				if !ok {
					return todo("VisitInto_table_stmt", valSource.Select_stmt())
				}
				source = sourceNode
			}
		}
	}

	returning := &ast.List{}
	if ret := ctx.Returning_columns_list(); ret != nil {
		temp, ok := ret.Accept(c).(ast.Node)
		if !ok {
			return todo("VisitInto_table_stmt", ctx.Returning_columns_list())
		}
		returningNode, ok := temp.(*ast.List)
		if !ok {
			return todo("VisitInto_table_stmt", ctx.Returning_columns_list())
		}
		returning = returningNode
	}

	stmts := &ast.InsertStmt{
		Relation:         rel,
		Cols:             cols,
		SelectStmt:       source,
		OnConflictClause: onConflict,
		ReturningList:    returning,
	}

	return stmts
}

func (c *cc) VisitValues_stmt(ctx *parser.Values_stmtContext) interface{} {
	mainList := &ast.List{}

	for _, rowCtx := range ctx.Values_source_row_list().AllValues_source_row() {
		rowList := &ast.List{}
		exprListCtx := rowCtx.Expr_list().(*parser.Expr_listContext)

		for _, exprCtx := range exprListCtx.AllExpr() {
			if converted := exprCtx.Accept(c); converted != nil {
				var convertedNode, ok = converted.(ast.Node)
				if !ok {
					return todo("VisitValues_stmt", exprCtx)
				}
				rowList.Items = append(rowList.Items, convertedNode)
			}
		}

		mainList.Items = append(mainList.Items, rowList)

	}

	return mainList
}

func (c *cc) VisitReturning_columns_list(ctx *parser.Returning_columns_listContext) interface{} {
	list := &ast.List{Items: []ast.Node{}}

	if ctx.ASTERISK() != nil {
		target := &ast.ResTarget{
			Indirection: &ast.List{},
			Val: &ast.ColumnRef{
				Fields:   &ast.List{Items: []ast.Node{&ast.A_Star{}}},
				Location: c.pos(ctx.ASTERISK().GetSymbol()),
			},
			Location: c.pos(ctx.ASTERISK().GetSymbol()),
		}
		list.Items = append(list.Items, target)
		return list
	}

	for _, idCtx := range ctx.AllAn_id() {
		target := &ast.ResTarget{
			Indirection: &ast.List{},
			Val: &ast.ColumnRef{
				Fields: &ast.List{
					Items: []ast.Node{NewIdentifier(parseAnId(idCtx))},
				},
				Location: c.pos(idCtx.GetStart()),
			},
			Location: c.pos(idCtx.GetStart()),
		}
		list.Items = append(list.Items, target)
	}

	return list
}

func (c *cc) VisitSelect_stmt(ctx *parser.Select_stmtContext) interface{} {
	if len(ctx.AllSelect_kind_parenthesis()) == 0 {
		return todo("VisitSelect_stmt", ctx)
	}

	skp := ctx.Select_kind_parenthesis(0)
	if skp == nil {
		return todo("VisitSelect_stmt", skp)
	}

	temp, ok := skp.Accept(c).(ast.Node)
	if !ok {
		return todo("VisitSelect_kind_parenthesis", skp)
	}
	left, ok := temp.(*ast.SelectStmt)
	if left == nil || !ok {
		return todo("VisitSelect_kind_parenthesis", skp)
	}

	kinds := ctx.AllSelect_kind_parenthesis()
	ops := ctx.AllSelect_op()

	for i := 1; i < len(kinds); i++ {
		temp, ok := kinds[i].Accept(c).(ast.Node)
		if !ok {
			return todo("VisitSelect_kind_parenthesis", kinds[i])
		}
		right, ok := temp.(*ast.SelectStmt)
		if right == nil || !ok {
			return todo("VisitSelect_kind_parenthesis", kinds[i])
		}

		var op ast.SetOperation
		var all bool
		if i-1 < len(ops) && ops[i-1] != nil {
			so := ops[i-1]
			switch {
			case so.UNION() != nil:
				op = ast.Union
			case so.INTERSECT() != nil:
				log.Fatalf("YDB: INTERSECT is not implemented yet")
			case so.EXCEPT() != nil:
				log.Fatalf("YDB: EXCEPT is not implemented yet")
			default:
				op = ast.None
			}
			all = so.ALL() != nil
		}
		larg := left
		left = emptySelectStmt()
		left.Op = op
		left.All = all
		left.Larg = larg
		left.Rarg = right
	}

	return left
}

func (c *cc) VisitSelect_kind_parenthesis(ctx *parser.Select_kind_parenthesisContext) interface{} {
	if ctx == nil || ctx.Select_kind_partial() == nil {
		return todo("VisitSelect_kind_parenthesis", ctx)
	}
	partial := ctx.Select_kind_partial()

	sk := partial.Select_kind()
	if sk == nil {
		return todo("VisitSelect_kind_parenthesis", sk)
	}

	var base ast.Node
	switch {
	case sk.Select_core() != nil:
		baseNode, ok := sk.Select_core().Accept(c).(ast.Node)
		if !ok {
			return todo("VisitSelect_kind_parenthesis", sk.Select_core())
		}
		base = baseNode
	case sk.Process_core() != nil:
		log.Fatalf("PROCESS is not supported in YDB engine")
	case sk.Reduce_core() != nil:
		log.Fatalf("REDUCE is not supported in YDB engine")
	}
	stmt, ok := base.(*ast.SelectStmt)
	if !ok || stmt == nil {
		return todo("VisitSelect_kind_parenthesis", sk.Select_core())
	}

	// TODO: handle INTO RESULT clause

	if partial.LIMIT() != nil {
		exprs := partial.AllExpr()
		if len(exprs) >= 1 {
			temp, ok := exprs[0].Accept(c).(ast.Node)
			if !ok {
				return todo("VisitSelect_kind_parenthesis", exprs[0])
			}
			stmt.LimitCount = temp
		}
		if partial.OFFSET() != nil {
			if len(exprs) >= 2 {
				temp, ok := exprs[1].Accept(c).(ast.Node)
				if !ok {
					return todo("VisitSelect_kind_parenthesis", exprs[1])
				}
				stmt.LimitOffset = temp
			}
		}
	}

	return stmt
}

func (c *cc) VisitSelect_core(ctx *parser.Select_coreContext) interface{} {
	stmt := emptySelectStmt()
	if ctx.Opt_set_quantifier() != nil {
		oq := ctx.Opt_set_quantifier()
		if oq.DISTINCT() != nil {
			stmt.DistinctClause.Items = append(stmt.DistinctClause.Items, &ast.TODO{}) // trick to handle distinct
		}
	}
	resultCols := ctx.AllResult_column()
	if len(resultCols) > 0 {
		var items []ast.Node
		for _, rc := range resultCols {
			convNode, ok := rc.Accept(c).(ast.Node)
			if !ok {
				return todo("VisitSelect_core", rc)
			}
			items = append(items, convNode)
		}
		stmt.TargetList = &ast.List{
			Items: items,
		}
	}

	// TODO: handle WITHOUT clause

	jsList := ctx.AllJoin_source()
	if len(ctx.AllFROM()) > 1 {
		log.Fatalf("YDB: Only one FROM clause is allowed")
	}
	if len(jsList) > 0 {
		var fromItems []ast.Node
		for _, js := range jsList {
			joinNode, ok := js.Accept(c).(ast.Node)
			if !ok {
				return todo("VisitSelect_core", js)
			}
			fromItems = append(fromItems, joinNode)
		}
		stmt.FromClause = &ast.List{
			Items: fromItems,
		}
	}

	exprIdx := 0
	if ctx.WHERE() != nil {
		if whereCtx := ctx.Expr(exprIdx); whereCtx != nil {
			where, ok := whereCtx.Accept(c).(ast.Node)
			if !ok {
				return todo("VisitSelect_core", whereCtx)
			}
			stmt.WhereClause = where
		}
		exprIdx++
	}
	if ctx.HAVING() != nil {
		if havingCtx := ctx.Expr(exprIdx); havingCtx != nil {
			having, ok := havingCtx.Accept(c).(ast.Node)
			if !ok || having == nil {
				return todo("VisitSelect_core", havingCtx)
			}
			stmt.HavingClause = having
		}
		exprIdx++
	}

	if gbc := ctx.Group_by_clause(); gbc != nil {
		if gel := gbc.Grouping_element_list(); gel != nil {
			var groups []ast.Node
			for _, ne := range gel.AllGrouping_element() {
				groupBy, ok := ne.Accept(c).(ast.Node)
				if !ok {
					return todo("VisitSelect_core", ne)
				}
				groups = append(groups, groupBy)
			}
			if len(groups) > 0 {
				stmt.GroupClause = &ast.List{Items: groups}
			}
		}
	}

	if ext := ctx.Ext_order_by_clause(); ext != nil {
		if ob := ext.Order_by_clause(); ob != nil && ob.ORDER() != nil && ob.BY() != nil {
			// TODO: ASSUME ORDER BY
			if sl := ob.Sort_specification_list(); sl != nil {
				var orderItems []ast.Node
				for _, sp := range sl.AllSort_specification() {
					expr, ok := sp.Expr().Accept(c).(ast.Node)
					if !ok {
						return todo("VisitSelect_core", sp.Expr())
					}
					dir := ast.SortByDirDefault
					if sp.ASC() != nil {
						dir = ast.SortByDirAsc
					} else if sp.DESC() != nil {
						dir = ast.SortByDirDesc
					}
					orderItems = append(orderItems, &ast.SortBy{
						Node:        expr,
						SortbyDir:   dir,
						SortbyNulls: ast.SortByNullsUndefined,
						UseOp:       &ast.List{},
						Location:    c.pos(sp.GetStart()),
					})
				}
				if len(orderItems) > 0 {
					stmt.SortClause = &ast.List{Items: orderItems}
				}
			}
		}
	}
	return stmt
}

func (c *cc) VisitGrouping_element(ctx *parser.Grouping_elementContext) interface{} {
	if ctx == nil {
		return todo("VisitGrouping_element", ctx)
	}
	if ogs := ctx.Ordinary_grouping_set(); ogs != nil {
		groupingSet, ok := ogs.Accept(c).(ast.Node)
		if !ok {
			return todo("VisitGrouping_element", ogs)
		}
		return groupingSet
	}
	if rl := ctx.Rollup_list(); rl != nil {
		rollupList, ok := rl.Accept(c).(ast.Node)
		if !ok {
			return todo("VisitGrouping_element", rl)
		}
		return rollupList
	}
	if cl := ctx.Cube_list(); cl != nil {
		cubeList, ok := cl.Accept(c).(ast.Node)
		if !ok {
			return todo("VisitGrouping_element", cl)
		}
		return cubeList
	}
	if gss := ctx.Grouping_sets_specification(); gss != nil {
		groupingSets, ok := gss.Accept(c).(ast.Node)
		if !ok {
			return todo("VisitGrouping_element", gss)
		}
		return groupingSets
	}
	return todo("VisitGrouping_element", ctx)
}

func (c *cc) VisitOrdinary_grouping_set(ctx *parser.Ordinary_grouping_setContext) interface{} {
	if ctx == nil || ctx.Named_expr() == nil {
		return todo("VisitOrdinary_grouping_set", ctx)
	}

	namedExpr, ok := ctx.Named_expr().Accept(c).(ast.Node)
	if !ok {
		return todo("VisitOrdinary_grouping_set", ctx.Named_expr())
	}
	return namedExpr
}

func (c *cc) VisitRollup_list(ctx *parser.Rollup_listContext) interface{} {
	if ctx == nil || ctx.ROLLUP() == nil || ctx.LPAREN() == nil || ctx.RPAREN() == nil {
		return todo("VisitRollup_list", ctx)
	}

	var items []ast.Node
	if list := ctx.Ordinary_grouping_set_list(); list != nil {
		for _, ogs := range list.AllOrdinary_grouping_set() {
			og, ok := ogs.Accept(c).(ast.Node)
			if !ok {
				return todo("VisitRollup_list", ogs)
			}
			items = append(items, og)
		}
	}
	return &ast.GroupingSet{Kind: 1, Content: &ast.List{Items: items}}
}

func (c *cc) VisitCube_list(ctx *parser.Cube_listContext) interface{} {
	if ctx == nil || ctx.CUBE() == nil || ctx.LPAREN() == nil || ctx.RPAREN() == nil {
		return todo("VisitCube_list", ctx)
	}

	var items []ast.Node
	if list := ctx.Ordinary_grouping_set_list(); list != nil {
		for _, ogs := range list.AllOrdinary_grouping_set() {
			og, ok := ogs.Accept(c).(ast.Node)
			if !ok {
				return todo("VisitCube_list", ogs)
			}
			items = append(items, og)
		}
	}

	return &ast.GroupingSet{Kind: 2, Content: &ast.List{Items: items}}
}

func (c *cc) VisitGrouping_sets_specification(ctx *parser.Grouping_sets_specificationContext) interface{} {
	if ctx == nil || ctx.GROUPING() == nil || ctx.SETS() == nil || ctx.LPAREN() == nil || ctx.RPAREN() == nil {
		return todo("VisitGrouping_sets_specification", ctx)
	}

	var items []ast.Node
	if gel := ctx.Grouping_element_list(); gel != nil {
		for _, ge := range gel.AllGrouping_element() {
			g, ok := ge.Accept(c).(ast.Node)
			if !ok {
				return todo("VisitGrouping_sets_specification", ge)
			}
			items = append(items, g)
		}
	}
	return &ast.GroupingSet{Kind: 3, Content: &ast.List{Items: items}}
}

func (c *cc) VisitResult_column(ctx *parser.Result_columnContext) interface{} {
	// todo: support opt_id_prefix
	target := &ast.ResTarget{
		Location: c.pos(ctx.GetStart()),
	}
	var val ast.Node
	iexpr := ctx.Expr()
	switch {
	case ctx.ASTERISK() != nil:
		val = c.convertWildCardField(ctx)
	case iexpr != nil:
		temp, ok := iexpr.Accept(c).(ast.Node)
		if !ok {
			return todo("VisitResult_column", iexpr)
		}
		val = temp
	}

	if val == nil {
		return todo("VisitResult_column", ctx)
	}
	switch {
	case ctx.AS() != nil && ctx.An_id_or_type() != nil:
		name := parseAnIdOrType(ctx.An_id_or_type())
		target.Name = &name
	case ctx.An_id_as_compat() != nil: //nolint
		// todo: parse as_compat
	}
	target.Val = val
	return target
}

func (c *cc) VisitJoin_source(ctx *parser.Join_sourceContext) interface{} {
	if ctx == nil || len(ctx.AllFlatten_source()) == 0 {
		return todo("VisitJoin_source", ctx)
	}
	fsList := ctx.AllFlatten_source()
	joinOps := ctx.AllJoin_op()
	joinConstraints := ctx.AllJoin_constraint()

	// todo: add ANY support

	leftNode, ok := fsList[0].Accept(c).(ast.Node)
	if !ok {
		return todo("VisitJoin_source", fsList[0])
	}
	for i, jopCtx := range joinOps {
		if i+1 >= len(fsList) {
			break
		}
		rightNode, ok := fsList[i+1].Accept(c).(ast.Node)
		if !ok {
			return todo("VisitJoin_source", fsList[i+1])
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
			if jc := joinConstraints[i]; jc != nil {
				switch {
				case jc.ON() != nil:
					if exprCtx := jc.Expr(); exprCtx != nil {
						expr, ok := exprCtx.Accept(c).(ast.Node)
						if !ok {
							return todo("VisitJoin_source", exprCtx)
						}
						jexpr.Quals = expr
					}
				case jc.USING() != nil:
					if pureListCtx := jc.Pure_column_or_named_list(); pureListCtx != nil {
						var using ast.List
						pureItems := pureListCtx.AllPure_column_or_named()
						for _, pureCtx := range pureItems {
							if anID := pureCtx.An_id(); anID != nil {
								using.Items = append(using.Items, NewIdentifier(parseAnId(anID)))
							} else if bp := pureCtx.Bind_parameter(); bp != nil {
								bindPar, ok := bp.Accept(c).(ast.Node)
								if !ok {
									return todo("VisitJoin_source", bp)
								}
								using.Items = append(using.Items, bindPar)
							}
						}
						jexpr.UsingClause = &using
					}
				default:
					return todo("VisitJoin_source", jc)
				}
			}
		}
		leftNode = jexpr
	}
	return leftNode
}

func (c *cc) VisitFlatten_source(ctx *parser.Flatten_sourceContext) interface{} {
	if ctx == nil || ctx.Named_single_source() == nil {
		return todo("VisitFlatten_source", ctx)
	}
	namedSingleSource, ok := ctx.Named_single_source().Accept(c).(ast.Node)
	if !ok {
		return todo("VisitFlatten_source", ctx.Named_single_source())
	}
	return namedSingleSource
}

func (c *cc) VisitNamed_single_source(ctx *parser.Named_single_sourceContext) interface{} {
	if ctx == nil || ctx.Single_source() == nil {
		return todo("VisitNamed_single_source", ctx)
	}
	base, ok := ctx.Single_source().Accept(c).(ast.Node)
	if !ok {
		return todo("VisitNamed_single_source", ctx.Single_source())
	}

	if ctx.AS() != nil && ctx.An_id() != nil {
		aliasText := parseAnId(ctx.An_id())
		switch source := base.(type) {
		case *ast.RangeVar:
			source.Alias = &ast.Alias{Aliasname: &aliasText}
		case *ast.RangeSubselect:
			source.Alias = &ast.Alias{Aliasname: &aliasText}
		}
	} else if ctx.An_id_as_compat() != nil { //nolint
		// todo: parse as_compat
	}
	return base
}

func (c *cc) VisitSingle_source(ctx *parser.Single_sourceContext) interface{} {
	if ctx == nil {
		return todo("VisitSingle_source", ctx)
	}

	if ctx.Table_ref() != nil {
		tableName := ctx.Table_ref().GetText() // !! debug !!
		return &ast.RangeVar{
			Relname:  &tableName,
			Location: c.pos(ctx.GetStart()),
		}
	}

	if ctx.Select_stmt() != nil {
		subquery, ok := ctx.Select_stmt().Accept(c).(ast.Node)
		if !ok {
			return todo("VisitSingle_source", ctx.Select_stmt())
		}
		return &ast.RangeSubselect{
			Subquery: subquery,
		}

	}
	// todo: Values stmt

	return todo("VisitSingle_source", ctx)
}

func (c *cc) VisitBind_parameter(ctx *parser.Bind_parameterContext) interface{} {
	if ctx == nil || ctx.DOLLAR() == nil {
		return todo("VisitBind_parameter", ctx)
	}

	if ctx.TRUE() != nil {
		return &ast.A_Const{Val: &ast.Boolean{Boolval: true}, Location: c.pos(ctx.GetStart())}
	}
	if ctx.FALSE() != nil {
		return &ast.A_Const{Val: &ast.Boolean{Boolval: false}, Location: c.pos(ctx.GetStart())}
	}

	if an := ctx.An_id_or_type(); an != nil {
		idText := parseAnIdOrType(an)
		return &ast.A_Expr{
			Name:     &ast.List{Items: []ast.Node{&ast.String{Str: "@"}}},
			Rexpr:    &ast.String{Str: idText},
			Location: c.pos(ctx.GetStart()),
		}
	}
	return todo("VisitBind_parameter", ctx)
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
		Location: c.pos(n.GetStart()),
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

func (c *cc) VisitCreate_table_stmt(ctx *parser.Create_table_stmtContext) interface{} {
	stmt := &ast.CreateTableStmt{
		Name:        parseTableName(ctx.Simple_table_ref().Simple_table_ref_core()),
		IfNotExists: ctx.EXISTS() != nil,
	}
	for _, def := range ctx.AllCreate_table_entry() {
		switch {
		case def.Column_schema() != nil:
			temp, ok := def.Column_schema().Accept(c).(ast.Node)
			if !ok {
				return todo("VisitCreate_table_stmt", def.Column_schema())
			}
			colCtx, ok := temp.(*ast.ColumnDef)
			if !ok {
				return todo("VisitCreate_table_stmt", def.Column_schema())
			}
			stmt.Cols = append(stmt.Cols, colCtx)
		case def.Table_constraint() != nil:
			conCtx := def.Table_constraint()
			switch {
			case conCtx.PRIMARY() != nil && conCtx.KEY() != nil:
				for _, cname := range conCtx.AllAn_id() {
					for _, col := range stmt.Cols {
						if col.Colname == parseAnId(cname) {
							col.IsNotNull = true
						}
					}
				}
			case conCtx.PARTITION() != nil && conCtx.BY() != nil:
				return todo("VisitCreate_table_stmt", conCtx)
			case conCtx.ORDER() != nil && conCtx.BY() != nil:
				return todo("VisitCreate_table_stmt", conCtx)
			}

		case def.Table_index() != nil:
			return todo("VisitCreate_table_stmt", def.Table_index())
		case def.Family_entry() != nil:
			return todo("VisitCreate_table_stmt", def.Family_entry())
		case def.Changefeed() != nil: // table-oriented
			return todo("VisitCreate_table_stmt", def.Changefeed())
		}
	}
	return stmt
}

func (c *cc) VisitColumn_schema(ctx *parser.Column_schemaContext) interface{} {
	if ctx == nil {
		return todo("VisitColumn_schema", ctx)
	}
	col := &ast.ColumnDef{}

	if anId := ctx.An_id_schema(); anId != nil {
		col.Colname = identifier(parseAnIdSchema(anId))
	}
	if tnb := ctx.Type_name_or_bind(); tnb != nil {
		temp, ok := tnb.Accept(c).(ast.Node)
		if !ok {
			return todo("VisitColumn_schema", tnb)
		}
		typeName, ok := temp.(*ast.TypeName)
		if !ok {
			return todo("VisitColumn_schema", tnb)
		}
		col.TypeName = typeName
	}
	if colCons := ctx.Opt_column_constraints(); colCons != nil {
		col.IsNotNull = colCons.NOT() != nil && colCons.NULL() != nil

		if colCons.DEFAULT() != nil && colCons.Expr() != nil {
			defaultExpr, ok := colCons.Expr().Accept(c).(ast.Node)
			if !ok {
				return todo("VisitColumn_schema", colCons.Expr())
			}
			col.RawDefault = defaultExpr
		}
	}
	// todo: family

	return col
}

func (c *cc) VisitType_name_or_bind(ctx *parser.Type_name_or_bindContext) interface{} {
	if ctx == nil {
		return todo("VisitType_name_or_bind", ctx)
	}

	if t := ctx.Type_name(); t != nil {
		temp, ok := t.Accept(c).(ast.Node)
		if !ok {
			return todo("VisitType_name_or_bind", t)
		}
		typeName, ok := temp.(*ast.TypeName)
		if !ok {
			return todo("VisitType_name_or_bind", t)
		}
		return typeName
	} else if b := ctx.Bind_parameter(); b != nil {
		return &ast.TypeName{Name: "BIND:" + identifier(parseAnIdOrType(b.An_id_or_type()))}
	}
	return todo("VisitType_name_or_bind", ctx)
}

func (c *cc) VisitType_name(ctx *parser.Type_nameContext) interface{} {
	if ctx == nil {
		return todo("VisitType_name", ctx)
	}

	if composite := ctx.Type_name_composite(); composite != nil {
		typeName, ok := composite.Accept(c).(ast.Node)
		if !ok {
			return todo("VisitType_name_or_bind", composite)
		}
		return typeName
	}

	if decimal := ctx.Type_name_decimal(); decimal != nil {
		if integerOrBinds := decimal.AllInteger_or_bind(); len(integerOrBinds) >= 2 {
			first, ok := integerOrBinds[0].Accept(c).(ast.Node)
			if !ok {
				return todo("VisitType_name", decimal.Integer_or_bind(0))
			}
			second, ok := integerOrBinds[1].Accept(c).(ast.Node)
			if !ok {
				return todo("VisitType_name", decimal.Integer_or_bind(1))
			}
			return &ast.TypeName{
				Name:    "decimal",
				TypeOid: 0,
				Names: &ast.List{
					Items: []ast.Node{
						first,
						second,
					},
				},
			}
		}
	}

	// Handle simple types
	if simple := ctx.Type_name_simple(); simple != nil {
		return &ast.TypeName{
			Name:    simple.GetText(),
			TypeOid: 0,
		}
	}

	return todo("VisitType_name", ctx)
}

func (c *cc) VisitInteger_or_bind(ctx *parser.Integer_or_bindContext) interface{} {
	if ctx == nil {
		return todo("VisitInteger_or_bind", ctx)
	}

	if integer := ctx.Integer(); integer != nil {
		val, err := parseIntegerValue(integer.GetText())
		if err != nil {
			return todo("VisitInteger_or_bind", ctx.Integer())
		}
		return &ast.Integer{Ival: val}
	}

	if bind := ctx.Bind_parameter(); bind != nil {
		temp, ok := bind.Accept(c).(ast.Node)
		if !ok {
			return todo("VisitInteger_or_bind", bind)
		}
		return temp
	}

	return todo("VisitInteger_or_bind", ctx)
}

func (c *cc) VisitType_name_composite(ctx *parser.Type_name_compositeContext) interface{} {
	if ctx == nil {
		return todo("VisitType_name_composite", ctx)
	}

	if opt := ctx.Type_name_optional(); opt != nil {
		if typeName := opt.Type_name_or_bind(); typeName != nil {
			tn, ok := typeName.Accept(c).(ast.Node)
			if !ok {
				return todo("VisitType_name_composite", typeName)
			}
			return &ast.TypeName{
				Name:    "Optional",
				TypeOid: 0,
				Names: &ast.List{
					Items: []ast.Node{tn},
				},
			}
		}
	}

	if tuple := ctx.Type_name_tuple(); tuple != nil {
		if typeNames := tuple.AllType_name_or_bind(); len(typeNames) > 0 {
			var items []ast.Node
			for _, tn := range typeNames {
				tnNode, ok := tn.Accept(c).(ast.Node)
				if !ok {
					return todo("VisitType_name_composite", tn)
				}
				items = append(items, tnNode)
			}
			return &ast.TypeName{
				Name:    "Tuple",
				TypeOid: 0,
				Names:   &ast.List{Items: items},
			}
		}
	}

	if struct_ := ctx.Type_name_struct(); struct_ != nil {
		if structArgs := struct_.AllStruct_arg(); len(structArgs) > 0 {
			var items []ast.Node
			for range structArgs {
				// TODO: Handle struct field names and types
				items = append(items, &ast.TODO{})
			}
			return &ast.TypeName{
				Name:    "Struct",
				TypeOid: 0,
				Names:   &ast.List{Items: items},
			}
		}
	}

	if variant := ctx.Type_name_variant(); variant != nil {
		if variantArgs := variant.AllVariant_arg(); len(variantArgs) > 0 {
			var items []ast.Node
			for range variantArgs {
				// TODO: Handle variant arguments
				items = append(items, &ast.TODO{})
			}
			return &ast.TypeName{
				Name:    "Variant",
				TypeOid: 0,
				Names:   &ast.List{Items: items},
			}
		}
	}

	if list := ctx.Type_name_list(); list != nil {
		if typeName := list.Type_name_or_bind(); typeName != nil {
			tn, ok := typeName.Accept(c).(ast.Node)
			if !ok {
				return todo("VisitType_name_composite", typeName)
			}
			return &ast.TypeName{
				Name:    "List",
				TypeOid: 0,
				Names: &ast.List{
					Items: []ast.Node{tn},
				},
			}
		}
	}

	if stream := ctx.Type_name_stream(); stream != nil {
		if typeName := stream.Type_name_or_bind(); typeName != nil {
			tn, ok := typeName.Accept(c).(ast.Node)
			if !ok {
				return todo("VisitType_name_composite", typeName)
			}
			return &ast.TypeName{
				Name:    "Stream",
				TypeOid: 0,
				Names: &ast.List{
					Items: []ast.Node{tn},
				},
			}
		}
	}

	if flow := ctx.Type_name_flow(); flow != nil {
		return todo("VisitType_name_composite", flow)
	}

	if dict := ctx.Type_name_dict(); dict != nil {
		if typeNames := dict.AllType_name_or_bind(); len(typeNames) >= 2 {
			first, ok := typeNames[0].Accept(c).(ast.Node)
			if !ok {
				return todo("VisitType_name_composite", typeNames[0])
			}
			second, ok := typeNames[1].Accept(c).(ast.Node)
			if !ok {
				return todo("VisitType_name_composite", typeNames[1])
			}
			return &ast.TypeName{
				Name:    "Dict",
				TypeOid: 0,
				Names: &ast.List{
					Items: []ast.Node{
						first,
						second,
					},
				},
			}
		}
	}

	if set := ctx.Type_name_set(); set != nil {
		if typeName := set.Type_name_or_bind(); typeName != nil {
			tn, ok := typeName.Accept(c).(ast.Node)
			if !ok {
				return todo("VisitType_name_composite", typeName)
			}
			return &ast.TypeName{
				Name:    "Set",
				TypeOid: 0,
				Names: &ast.List{
					Items: []ast.Node{tn},
				},
			}
		}
	}

	if enum := ctx.Type_name_enum(); enum != nil { // todo: handle enum
		todo("VisitType_name_composite", enum)
	}

	if resource := ctx.Type_name_resource(); resource != nil { // todo: handle resource
		todo("VisitType_name_composite", resource)
	}

	if tagged := ctx.Type_name_tagged(); tagged != nil { // todo: handle tagged
		todo("VisitType_name_composite", tagged)
	}

	if callable := ctx.Type_name_callable(); callable != nil { // todo: handle callable
		todo("VisitType_name_composite", callable)
	}

	return todo("VisitType_name_composite", ctx)
}

func (c *cc) VisitSql_stmt_core(ctx *parser.Sql_stmt_coreContext) interface{} {
	if ctx == nil {
		return todo("VisitSql_stmt_core", ctx)
	}

	if stmt := ctx.Pragma_stmt(); stmt != nil {
		return stmt.Accept(c)
	}
	if stmt := ctx.Select_stmt(); stmt != nil {
		return stmt.Accept(c)
	}
	if stmt := ctx.Named_nodes_stmt(); stmt != nil {
		return stmt.Accept(c)
	}
	if stmt := ctx.Create_table_stmt(); stmt != nil {
		return stmt.Accept(c)
	}
	if stmt := ctx.Named_nodes_stmt(); stmt != nil {
		return stmt.Accept(c)
	}
	if stmt := ctx.Create_table_stmt(); stmt != nil {
		return stmt.Accept(c)
	}
	if stmt := ctx.Drop_table_stmt(); stmt != nil {
		return stmt.Accept(c)
	}
	if stmt := ctx.Use_stmt(); stmt != nil {
		return stmt.Accept(c)
	}
	if stmt := ctx.Into_table_stmt(); stmt != nil {
		return stmt.Accept(c)
	}
	if stmt := ctx.Commit_stmt(); stmt != nil {
		return stmt.Accept(c)
	}
	if stmt := ctx.Update_stmt(); stmt != nil {
		return stmt.Accept(c)
	}
	if stmt := ctx.Delete_stmt(); stmt != nil {
		return stmt.Accept(c)
	}
	if stmt := ctx.Rollback_stmt(); stmt != nil {
		return stmt.Accept(c)
	}
	if stmt := ctx.Declare_stmt(); stmt != nil {
		return stmt.Accept(c)
	}
	if stmt := ctx.Import_stmt(); stmt != nil {
		return stmt.Accept(c)
	}
	if stmt := ctx.Export_stmt(); stmt != nil {
		return stmt.Accept(c)
	}
	if stmt := ctx.Alter_table_stmt(); stmt != nil {
		return stmt.Accept(c)
	}
	if stmt := ctx.Alter_external_table_stmt(); stmt != nil {
		return stmt.Accept(c)
	}
	if stmt := ctx.Do_stmt(); stmt != nil {
		return stmt.Accept(c)
	}
	if stmt := ctx.Define_action_or_subquery_stmt(); stmt != nil {
		return stmt.Accept(c)
	}
	if stmt := ctx.If_stmt(); stmt != nil {
		return stmt.Accept(c)
	}
	if stmt := ctx.For_stmt(); stmt != nil {
		return stmt.Accept(c)
	}
	if stmt := ctx.Values_stmt(); stmt != nil {
		return stmt.Accept(c)
	}
	if stmt := ctx.Create_user_stmt(); stmt != nil {
		return stmt.Accept(c)
	}
	if stmt := ctx.Alter_user_stmt(); stmt != nil {
		return stmt.Accept(c)
	}
	if stmt := ctx.Create_group_stmt(); stmt != nil {
		return stmt.Accept(c)
	}
	if stmt := ctx.Alter_group_stmt(); stmt != nil {
		return stmt.Accept(c)
	}
	if stmt := ctx.Drop_role_stmt(); stmt != nil {
		return stmt.Accept(c)
	}
	if stmt := ctx.Create_object_stmt(); stmt != nil {
		return stmt.Accept(c)
	}
	if stmt := ctx.Alter_object_stmt(); stmt != nil {
		return stmt.Accept(c)
	}
	if stmt := ctx.Drop_object_stmt(); stmt != nil {
		return stmt.Accept(c)
	}
	if stmt := ctx.Create_external_data_source_stmt(); stmt != nil {
		return stmt.Accept(c)
	}
	if stmt := ctx.Alter_external_data_source_stmt(); stmt != nil {
		return stmt.Accept(c)
	}
	if stmt := ctx.Drop_external_data_source_stmt(); stmt != nil {
		return stmt.Accept(c)
	}
	if stmt := ctx.Create_replication_stmt(); stmt != nil {
		return stmt.Accept(c)
	}
	if stmt := ctx.Drop_replication_stmt(); stmt != nil {
		return stmt.Accept(c)
	}
	if stmt := ctx.Create_topic_stmt(); stmt != nil {
		return stmt.Accept(c)
	}
	if stmt := ctx.Alter_topic_stmt(); stmt != nil {
		return stmt.Accept(c)
	}
	if stmt := ctx.Drop_topic_stmt(); stmt != nil {
		return stmt.Accept(c)
	}
	if stmt := ctx.Grant_permissions_stmt(); stmt != nil {
		return stmt.Accept(c)
	}
	if stmt := ctx.Revoke_permissions_stmt(); stmt != nil {
		return stmt.Accept(c)
	}
	if stmt := ctx.Alter_table_store_stmt(); stmt != nil {
		return stmt.Accept(c)
	}
	if stmt := ctx.Upsert_object_stmt(); stmt != nil {
		return stmt.Accept(c)
	}
	if stmt := ctx.Create_view_stmt(); stmt != nil {
		return stmt.Accept(c)
	}
	if stmt := ctx.Drop_view_stmt(); stmt != nil {
		return stmt.Accept(c)
	}
	if stmt := ctx.Alter_replication_stmt(); stmt != nil {
		return stmt.Accept(c)
	}
	if stmt := ctx.Create_resource_pool_stmt(); stmt != nil {
		return stmt.Accept(c)
	}
	if stmt := ctx.Alter_resource_pool_stmt(); stmt != nil {
		return stmt.Accept(c)
	}
	if stmt := ctx.Drop_resource_pool_stmt(); stmt != nil {
		return stmt.Accept(c)
	}
	if stmt := ctx.Create_backup_collection_stmt(); stmt != nil {
		return stmt.Accept(c)
	}
	if stmt := ctx.Alter_backup_collection_stmt(); stmt != nil {
		return stmt.Accept(c)
	}
	if stmt := ctx.Drop_backup_collection_stmt(); stmt != nil {
		return stmt.Accept(c)
	}
	if stmt := ctx.Analyze_stmt(); stmt != nil {
		return stmt.Accept(c)
	}
	if stmt := ctx.Create_resource_pool_classifier_stmt(); stmt != nil {
		return stmt.Accept(c)
	}
	if stmt := ctx.Alter_resource_pool_classifier_stmt(); stmt != nil {
		return stmt.Accept(c)
	}
	if stmt := ctx.Drop_resource_pool_classifier_stmt(); stmt != nil {
		return stmt.Accept(c)
	}
	if stmt := ctx.Backup_stmt(); stmt != nil {
		return stmt.Accept(c)
	}
	if stmt := ctx.Restore_stmt(); stmt != nil {
		return stmt.Accept(c)
	}
	if stmt := ctx.Alter_sequence_stmt(); stmt != nil {
		return stmt.Accept(c)
	}
	return todo("VisitSql_stmt_core", ctx)
}

func (c *cc) VisitNamed_expr(ctx *parser.Named_exprContext) interface{} {
	if ctx == nil || ctx.Expr() == nil {
		return todo("VisitNamed_expr", ctx)
	}

	expr, ok := ctx.Expr().Accept(c).(ast.Node)
	if !ok {
		return todo("VisitNamed_expr", ctx)
	}

	if ctx.AS() != nil && ctx.An_id_or_type() != nil {
		name := parseAnIdOrType(ctx.An_id_or_type())
		return &ast.ResTarget{
			Name:     &name,
			Val:      expr,
			Location: c.pos(ctx.Expr().GetStart()),
		}
	}
	return expr
}

func (c *cc) VisitExpr(ctx *parser.ExprContext) interface{} {
	if ctx == nil {
		return todo("VisitExpr", ctx)
	}

	if tn := ctx.Type_name_composite(); tn != nil {
		return tn.Accept(c)
	}

	orSubs := ctx.AllOr_subexpr()
	if len(orSubs) == 0 {
		return todo("VisitExpr", ctx)
	}

	left, ok := ctx.Or_subexpr(0).Accept(c).(ast.Node)
	if !ok {
		return todo("VisitExpr", ctx)
	}

	for i := 1; i < len(orSubs); i++ {

		right, ok := orSubs[i].Accept(c).(ast.Node)
		if !ok {
			return todo("VisitExpr", ctx)
		}

		left = &ast.BoolExpr{
			Boolop:   ast.BoolExprTypeOr,
			Args:     &ast.List{Items: []ast.Node{left, right}},
			Location: c.pos(ctx.GetStart()),
		}
	}
	return left
}

func (c *cc) VisitOr_subexpr(ctx *parser.Or_subexprContext) interface{} {
	if ctx == nil || len(ctx.AllAnd_subexpr()) == 0 {
		return todo("VisitOr_subexpr", ctx)
	}

	left, ok := ctx.And_subexpr(0).Accept(c).(ast.Node)
	if !ok {
		return todo("VisitOr_subexpr", ctx)
	}

	for i := 1; i < len(ctx.AllAnd_subexpr()); i++ {

		right, ok := ctx.And_subexpr(i).Accept(c).(ast.Node)
		if !ok {
			return todo("VisitOr_subexpr", ctx)
		}

		left = &ast.BoolExpr{
			Boolop:   ast.BoolExprTypeAnd,
			Args:     &ast.List{Items: []ast.Node{left, right}},
			Location: c.pos(ctx.GetStart()),
		}
	}
	return left
}

func (c *cc) VisitAnd_subexpr(ctx *parser.And_subexprContext) interface{} {
	if ctx == nil || len(ctx.AllXor_subexpr()) == 0 {
		return todo("VisitAnd_subexpr", ctx)
	}

	left, ok := ctx.Xor_subexpr(0).Accept(c).(ast.Node)
	if !ok {
		return todo("VisitAnd_subexpr", ctx)
	}

	for i := 1; i < len(ctx.AllXor_subexpr()); i++ {

		right, ok := ctx.Xor_subexpr(i).Accept(c).(ast.Node)
		if !ok {
			return todo("VisitAnd_subexpr", ctx)
		}

		left = &ast.A_Expr{
			Name:     &ast.List{Items: []ast.Node{&ast.String{Str: "XOR"}}},
			Lexpr:    left,
			Rexpr:    right,
			Location: c.pos(ctx.GetStart()),
		}
	}
	return left
}

func (c *cc) VisitXor_subexpr(ctx *parser.Xor_subexprContext) interface{} {
	if ctx == nil || ctx.Eq_subexpr() == nil {
		return todo("VisitXor_subexpr", ctx)
	}

	base, ok := ctx.Eq_subexpr().Accept(c).(ast.Node)
	if !ok {
		return todo("VisitXor_subexpr", ctx)
	}

	if condCtx := ctx.Cond_expr(); condCtx != nil {

		switch {
		case condCtx.IN() != nil:
			if inExpr := condCtx.In_expr(); inExpr != nil {
				temp, ok := inExpr.Accept(c).(ast.Node)
				if !ok {
					return todo("VisitXor_subexpr", inExpr)
				}
				list, ok := temp.(*ast.List)
				if !ok {
					return todo("VisitXor_subexpr", inExpr)
				}
				return &ast.In{
					Expr:     base,
					List:     list.Items,
					Not:      condCtx.NOT() != nil,
					Location: c.pos(ctx.GetStart()),
				}
			}
		case condCtx.BETWEEN() != nil:
			if eqSubs := condCtx.AllEq_subexpr(); len(eqSubs) >= 2 {

				first, ok := eqSubs[0].Accept(c).(ast.Node)
				if !ok {
					return todo("VisitXor_subexpr", ctx)
				}

				second, ok := eqSubs[1].Accept(c).(ast.Node)
				if !ok {
					return todo("VisitXor_subexpr", ctx)
				}

				return &ast.BetweenExpr{
					Expr:     base,
					Left:     first,
					Right:    second,
					Not:      condCtx.NOT() != nil,
					Location: c.pos(ctx.GetStart()),
				}
			}
		case condCtx.ISNULL() != nil:
			return &ast.NullTest{
				Arg:          base,
				Nulltesttype: 1, // IS NULL
				Location:     c.pos(ctx.GetStart()),
			}
		case condCtx.NOTNULL() != nil:
			return &ast.NullTest{
				Arg:          base,
				Nulltesttype: 2, // IS NOT NULL
				Location:     c.pos(ctx.GetStart()),
			}
		case condCtx.IS() != nil && condCtx.NULL() != nil:
			return &ast.NullTest{
				Arg:          base,
				Nulltesttype: 1, // IS NULL
				Location:     c.pos(ctx.GetStart()),
			}
		case condCtx.NOT() != nil && condCtx.NULL() != nil:
			return &ast.NullTest{
				Arg:          base,
				Nulltesttype: 2, // IS NOT NULL
				Location:     c.pos(ctx.GetStart()),
			}
		case condCtx.Match_op() != nil:
			// debug!!!
			matchOp := condCtx.Match_op().GetText()
			if eqSubs := condCtx.AllEq_subexpr(); len(eqSubs) >= 1 {

				xpr, ok := eqSubs[0].Accept(c).(ast.Node)
				if !ok {
					return todo("VisitXor_subexpr", ctx)
				}

				expr := &ast.A_Expr{
					Name:  &ast.List{Items: []ast.Node{&ast.String{Str: matchOp}}},
					Lexpr: base,
					Rexpr: xpr,
				}
				if condCtx.ESCAPE() != nil && len(eqSubs) >= 2 { //nolint
					// todo: Add ESCAPE support
				}
				return expr
			}
		case len(condCtx.AllEQUALS()) > 0 || len(condCtx.AllEQUALS2()) > 0 ||
			len(condCtx.AllNOT_EQUALS()) > 0 || len(condCtx.AllNOT_EQUALS2()) > 0:
			eqSubs := condCtx.AllEq_subexpr()
			if len(eqSubs) >= 1 {
				left := base

				ops := c.collectEqualityOps(condCtx)

				for i, eqSub := range eqSubs {
					right, ok := eqSub.Accept(c).(ast.Node)
					if !ok {
						return todo("VisitXor_subexpr", condCtx)
					}

					var op string
					if i < len(ops) {
						op = ops[i].GetText()
					} else {
						if len(condCtx.AllEQUALS()) > 0 {
							op = "="
						} else if len(condCtx.AllEQUALS2()) > 0 {
							op = "=="
						} else if len(condCtx.AllNOT_EQUALS()) > 0 {
							op = "!="
						} else if len(condCtx.AllNOT_EQUALS2()) > 0 {
							op = "<>"
						}
					}

					left = &ast.A_Expr{
						Name:     &ast.List{Items: []ast.Node{&ast.String{Str: op}}},
						Lexpr:    left,
						Rexpr:    right,
						Location: c.pos(condCtx.GetStart()),
					}
				}
				return left
			}
			return todo("VisitXor_subexpr", condCtx)
		case len(condCtx.AllDistinct_from_op()) > 0:
			// debug!!!
			distinctOps := condCtx.AllDistinct_from_op()
			for _, distinctOp := range distinctOps {
				if eqSubs := condCtx.AllEq_subexpr(); len(eqSubs) >= 1 {
					not := distinctOp.NOT() != nil
					op := "IS DISTINCT FROM"
					if not {
						op = "IS NOT DISTINCT FROM"
					}

					xpr, ok := eqSubs[0].Accept(c).(ast.Node)
					if !ok {
						return todo("VisitXor_subexpr", ctx)
					}

					return &ast.A_Expr{
						Name:  &ast.List{Items: []ast.Node{&ast.String{Str: op}}},
						Lexpr: base,
						Rexpr: xpr,
					}
				}
			}
		}
	}
	return base
}

func (c *cc) VisitEq_subexpr(ctx *parser.Eq_subexprContext) interface{} {
	if ctx == nil || len(ctx.AllNeq_subexpr()) == 0 {
		return todo("VisitEq_subexpr", ctx)
	}

	left, ok := ctx.Neq_subexpr(0).Accept(c).(ast.Node)
	if !ok {
		return todo("VisitEq_subexpr", ctx)
	}

	ops := c.collectComparisonOps(ctx)
	for i := 1; i < len(ctx.AllNeq_subexpr()); i++ {

		right, ok := ctx.Neq_subexpr(i).Accept(c).(ast.Node)
		if !ok {
			return todo("VisitEq_subexpr", ctx)
		}

		opText := ops[i-1].GetText()
		left = &ast.A_Expr{
			Name:     &ast.List{Items: []ast.Node{&ast.String{Str: opText}}},
			Lexpr:    left,
			Rexpr:    right,
			Location: c.pos(ctx.GetStart()),
		}
	}
	return left
}

func (c *cc) VisitNeq_subexpr(ctx *parser.Neq_subexprContext) interface{} {
	if ctx == nil || len(ctx.AllBit_subexpr()) == 0 {
		return todo("VisitNeq_subexpr", ctx)
	}

	left, ok := ctx.Bit_subexpr(0).Accept(c).(ast.Node)
	if !ok {
		return todo("VisitNeq_subexpr", ctx)
	}

	ops := c.collectBitwiseOps(ctx)
	for i := 1; i < len(ctx.AllBit_subexpr()); i++ {
		right, ok := ctx.Bit_subexpr(i).Accept(c).(ast.Node)
		if !ok {
			return todo("VisitNeq_subexpr", ctx)
		}
		opText := ops[i-1].GetText()
		left = &ast.A_Expr{
			Name:     &ast.List{Items: []ast.Node{&ast.String{Str: opText}}},
			Lexpr:    left,
			Rexpr:    right,
			Location: c.pos(ctx.GetStart()),
		}
	}

	if ctx.Double_question() != nil {
		if nextCtx := ctx.Neq_subexpr(); nextCtx != nil {
			right, ok2 := nextCtx.Accept(c).(ast.Node)
			if !ok2 {
				return todo("VisitNeq_subexpr", ctx)
			}

			left = &ast.A_Expr{
				Name:     &ast.List{Items: []ast.Node{&ast.String{Str: "??"}}},
				Lexpr:    left,
				Rexpr:    right,
				Location: c.pos(ctx.GetStart()),
			}
		}
	} else {
		// !! debug !!
		qCount := len(ctx.AllQUESTION())
		if qCount > 0 {
			questionOp := "?"
			if qCount > 1 {
				questionOp = strings.Repeat("?", qCount)
			}
			left = &ast.A_Expr{
				Name:     &ast.List{Items: []ast.Node{&ast.String{Str: questionOp}}},
				Lexpr:    left,
				Location: c.pos(ctx.GetStart()),
			}
		}
	}

	return left
}

func (c *cc) VisitBit_subexpr(ctx *parser.Bit_subexprContext) interface{} {
	if ctx == nil || len(ctx.AllAdd_subexpr()) == 0 {
		return todo("VisitBit_subexpr", ctx)
	}

	left, ok := ctx.Add_subexpr(0).Accept(c).(ast.Node)
	if !ok {
		return todo("VisitBit_subexpr", ctx)
	}

	ops := c.collectBitOps(ctx)
	for i := 1; i < len(ctx.AllAdd_subexpr()); i++ {

		right, ok := ctx.Add_subexpr(i).Accept(c).(ast.Node)
		if !ok {
			return todo("VisitBit_subexpr", ctx)
		}

		opText := ops[i-1].GetText()
		left = &ast.A_Expr{
			Name:     &ast.List{Items: []ast.Node{&ast.String{Str: opText}}},
			Lexpr:    left,
			Rexpr:    right,
			Location: c.pos(ctx.GetStart()),
		}
	}
	return left
}

func (c *cc) VisitAdd_subexpr(ctx *parser.Add_subexprContext) interface{} {
	if ctx == nil || len(ctx.AllMul_subexpr()) == 0 {
		return todo("VisitAdd_subexpr", ctx)
	}

	left, ok := ctx.Mul_subexpr(0).Accept(c).(ast.Node)
	if !ok {
		return todo("VisitAdd_subexpr", ctx)
	}

	ops := c.collectAddOps(ctx)
	for i := 1; i < len(ctx.AllMul_subexpr()); i++ {

		right, ok := ctx.Mul_subexpr(i).Accept(c).(ast.Node)
		if !ok {
			return todo("VisitAdd_subexpr", ctx)
		}

		opText := ops[i-1].GetText()
		left = &ast.A_Expr{
			Name:     &ast.List{Items: []ast.Node{&ast.String{Str: opText}}},
			Lexpr:    left,
			Rexpr:    right,
			Location: c.pos(ctx.GetStart()),
		}
	}
	return left
}

func (c *cc) VisitMul_subexpr(ctx *parser.Mul_subexprContext) interface{} {
	if ctx == nil || len(ctx.AllCon_subexpr()) == 0 {
		return todo("VisitMul_subexpr", ctx)
	}

	left, ok := ctx.Con_subexpr(0).Accept(c).(ast.Node)
	if !ok {
		return todo("VisitMul_subexpr", ctx)
	}

	for i := 1; i < len(ctx.AllCon_subexpr()); i++ {

		right, ok := ctx.Con_subexpr(i).Accept(c).(ast.Node)
		if !ok {
			return todo("VisitMul_subexpr", ctx)
		}

		left = &ast.A_Expr{
			Name:     &ast.List{Items: []ast.Node{&ast.String{Str: "||"}}},
			Lexpr:    left,
			Rexpr:    right,
			Location: c.pos(ctx.GetStart()),
		}
	}
	return left
}

func (c *cc) VisitCon_subexpr(ctx *parser.Con_subexprContext) interface{} {
	if ctx == nil || (ctx.Unary_op() == nil && ctx.Unary_subexpr() == nil) {
		return todo("VisitCon_subexpr", ctx)
	}

	if opCtx := ctx.Unary_op(); opCtx != nil {
		op := opCtx.GetText()
		operand, ok := ctx.Unary_subexpr().Accept(c).(ast.Node)
		if !ok {
			return todo("VisitCon_subexpr", opCtx)
		}
		return &ast.A_Expr{
			Name:     &ast.List{Items: []ast.Node{&ast.String{Str: op}}},
			Rexpr:    operand,
			Location: c.pos(ctx.GetStart()),
		}
	}

	operand, ok := ctx.Unary_subexpr().Accept(c).(ast.Node)
	if !ok {
		return todo("VisitCon_subexpr", ctx.Unary_subexpr())
	}
	return operand

}

func (c *cc) VisitUnary_subexpr(ctx *parser.Unary_subexprContext) interface{} {
	if ctx == nil || (ctx.Unary_casual_subexpr() == nil && ctx.Json_api_expr() == nil) {
		return todo("VisitUnary_subexpr", ctx)
	}

	if casual := ctx.Unary_casual_subexpr(); casual != nil {
		expr, ok := casual.Accept(c).(ast.Node)
		if !ok {
			return todo("VisitUnary_subexpr", casual)
		}
		return expr
	}
	if jsonExpr := ctx.Json_api_expr(); jsonExpr != nil {
		expr, ok := jsonExpr.Accept(c).(ast.Node)
		if !ok {
			return todo("VisitUnary_subexpr", jsonExpr)
		}
		return expr
	}

	return todo("VisitUnary_subexpr", ctx)
}

func (c *cc) VisitJson_api_expr(ctx *parser.Json_api_exprContext) interface{} {
	return todo("VisitJson_api_expr", ctx)
}

func (c *cc) VisitUnary_casual_subexpr(ctx *parser.Unary_casual_subexprContext) interface{} {
	var current ast.Node
	switch {
	case ctx.Id_expr() != nil:
		expr, ok := ctx.Id_expr().Accept(c).(ast.Node)
		if !ok {
			return todo("VisitUnary_casual_subexpr", ctx.Id_expr())
		}
		current = expr
	case ctx.Atom_expr() != nil:
		expr, ok := ctx.Atom_expr().Accept(c).(ast.Node)
		if !ok {
			return todo("VisitUnary_casual_subexpr", ctx.Atom_expr())
		}
		current = expr
	default:
		return todo("VisitUnary_casual_subexpr", ctx)
	}

	if suffix := ctx.Unary_subexpr_suffix(); suffix != nil {
		current = c.processSuffixChain(current, suffix.(*parser.Unary_subexpr_suffixContext))
	}

	return current
}

func (c *cc) processSuffixChain(base ast.Node, suffix *parser.Unary_subexpr_suffixContext) ast.Node {
	current := base
	for i := 0; i < suffix.GetChildCount(); i++ {
		child := suffix.GetChild(i)
		switch elem := child.(type) {
		case *parser.Key_exprContext:
			current = c.handleKeySuffix(current, elem)
		case *parser.Invoke_exprContext:
			current = c.handleInvokeSuffix(current, elem, i)
		case antlr.TerminalNode:
			if elem.GetText() == "." {
				current = c.handleDotSuffix(current, suffix, &i)
			} else {
				return todo("Unary_subexpr_suffixContext", suffix)
			}
		default:
			return todo("Unary_subexpr_suffixContext", suffix)
		}
	}
	return current
}

func (c *cc) handleKeySuffix(base ast.Node, keyCtx *parser.Key_exprContext) ast.Node {
	keyNode, ok := keyCtx.Accept(c).(ast.Node)
	if !ok {
		return todo("VisitKey_expr", keyCtx)
	}
	ind, ok := keyNode.(*ast.A_Indirection)
	if !ok {
		return todo("VisitKey_expr", keyCtx)
	}

	if indirection, ok := base.(*ast.A_Indirection); ok {
		indirection.Indirection.Items = append(indirection.Indirection.Items, ind.Indirection.Items...)
		return indirection
	}

	return &ast.A_Indirection{
		Arg: base,
		Indirection: &ast.List{
			Items: []ast.Node{keyNode},
		},
	}
}

func (c *cc) handleInvokeSuffix(base ast.Node, invokeCtx *parser.Invoke_exprContext, idx int) ast.Node {
	temp, ok := invokeCtx.Accept(c).(ast.Node)
	if !ok {
		return todo("VisitInvoke_expr", invokeCtx)
	}
	funcCall, ok := temp.(*ast.FuncCall)
	if !ok {
		return todo("VisitInvoke_expr", invokeCtx)
	}

	if idx == 0 {
		switch baseNode := base.(type) {
		case *ast.ColumnRef:
			if len(baseNode.Fields.Items) > 0 {
				var nameParts []string
				for _, item := range baseNode.Fields.Items {
					if s, ok := item.(*ast.String); ok {
						nameParts = append(nameParts, s.Str)
					}
				}
				funcName := strings.Join(nameParts, ".")

				if funcName == "coalesce" {
					return &ast.CoalesceExpr{
						Args:     funcCall.Args,
						Location: baseNode.Location,
					}
				}

				funcCall.Func = &ast.FuncName{Name: funcName}
				funcCall.Funcname.Items = append(funcCall.Funcname.Items, &ast.String{Str: funcName})

				return funcCall
			}
		default:
			return todo("VisitInvoke_expr", invokeCtx)
		}
	}

	stmt := &ast.RecursiveFuncCall{
		Func:        base,
		Funcname:    funcCall.Funcname,
		AggStar:     funcCall.AggStar,
		Location:    funcCall.Location,
		Args:        funcCall.Args,
		AggDistinct: funcCall.AggDistinct,
	}
	stmt.Funcname.Items = append(stmt.Funcname.Items, base)
	return stmt
}

func (c *cc) handleDotSuffix(base ast.Node, suffix *parser.Unary_subexpr_suffixContext, idx *int) ast.Node {
	if *idx+1 >= suffix.GetChildCount() {
		return base
	}

	next := suffix.GetChild(*idx + 1)
	*idx++

	var field ast.Node
	switch v := next.(type) {
	case *parser.Bind_parameterContext:
		temp, ok := v.Accept(c).(ast.Node)
		if !ok {
			return todo("VisitBind_parameter", v)
		}
		field = temp
	case *parser.An_id_or_typeContext:
		field = &ast.String{Str: parseAnIdOrType(v)}
	case antlr.TerminalNode:
		if val, err := parseIntegerValue(v.GetText()); err == nil {
			field = &ast.A_Const{Val: &ast.Integer{Ival: val}}
		} else {
			return todo("Unary_subexpr_suffixContext", suffix)
		}
	}

	if field == nil {
		return base
	}

	if cr, ok := base.(*ast.ColumnRef); ok {
		cr.Fields.Items = append(cr.Fields.Items, field)
		return cr
	}
	return &ast.ColumnRef{
		Fields: &ast.List{Items: []ast.Node{base, field}},
	}
}

func (c *cc) VisitKey_expr(ctx *parser.Key_exprContext) interface{} {
	if ctx.LBRACE_SQUARE() == nil || ctx.RBRACE_SQUARE() == nil || ctx.Expr() == nil {
		return todo("VisitKey_expr", ctx)
	}

	stmt := &ast.A_Indirection{
		Indirection: &ast.List{},
	}

	expr, ok := ctx.Expr().Accept(c).(ast.Node)
	if !ok {
		return todo("VisitKey_expr", ctx.Expr())
	}

	stmt.Indirection.Items = append(stmt.Indirection.Items, &ast.A_Indices{
		Uidx: expr,
	})

	return stmt
}

func (c *cc) VisitInvoke_expr(ctx *parser.Invoke_exprContext) interface{} {
	if ctx.LPAREN() == nil || ctx.RPAREN() == nil {
		return todo("VisitInvoke_expr", ctx)
	}

	distinct := false
	if ctx.Opt_set_quantifier() != nil {
		distinct = ctx.Opt_set_quantifier().DISTINCT() != nil
	}

	stmt := &ast.FuncCall{
		AggDistinct: distinct,
		Funcname:    &ast.List{},
		AggOrder:    &ast.List{},
		Args:        &ast.List{},
		Location:    c.pos(ctx.GetStart()),
	}

	if nList := ctx.Named_expr_list(); nList != nil {
		for _, namedExpr := range nList.AllNamed_expr() {
			name := parseAnIdOrType(namedExpr.An_id_or_type())
			expr, ok := namedExpr.Expr().Accept(c).(ast.Node)
			if !ok {
				return todo("VisitInvoke_expr", namedExpr.Expr())
			}

			var res ast.Node
			if rt, ok := expr.(*ast.ResTarget); ok {
				if name != "" {
					rt.Name = &name
				}
				res = rt
			} else if name != "" {
				res = &ast.ResTarget{
					Name:     &name,
					Val:      expr,
					Location: c.pos(namedExpr.Expr().GetStart()),
				}
			} else {
				res = expr
			}

			stmt.Args.Items = append(stmt.Args.Items, res)
		}
	} else if ctx.ASTERISK() != nil {
		stmt.AggStar = true
	}

	return stmt
}

func (c *cc) VisitId_expr(ctx *parser.Id_exprContext) interface{} {
	if ctx == nil {
		return todo("VisitId_expr", ctx)
	}
	if id := ctx.Identifier(); id != nil {
		return &ast.ColumnRef{
			Fields: &ast.List{
				Items: []ast.Node{
					NewIdentifier(id.GetText()),
				},
			},
			Location: c.pos(id.GetStart()),
		}
	}
	return todo("VisitId_expr", ctx)
}

func (c *cc) VisitAtom_expr(ctx *parser.Atom_exprContext) interface{} {
	if ctx == nil {
		return todo("VisitAtom_expr", ctx)
	}

	switch {
	case ctx.An_id_or_type() != nil:
		if ctx.NAMESPACE() != nil {
			return NewIdentifier(parseAnIdOrType(ctx.An_id_or_type()) + "::" + parseIdOrType(ctx.Id_or_type()))
		}
		return NewIdentifier(parseAnIdOrType(ctx.An_id_or_type()))
	case ctx.Literal_value() != nil:
		expr, ok := ctx.Literal_value().Accept(c).(ast.Node)
		if !ok {
			return todo("VisitAtom_expr", ctx.Literal_value())
		}
		return expr
	case ctx.Bind_parameter() != nil:
		expr, ok := ctx.Bind_parameter().Accept(c).(ast.Node)
		if !ok {
			return todo("VisitAtom_expr", ctx.Bind_parameter())
		}
		return expr
	// TODO: check other cases
	default:
		return todo("VisitAtom_expr", ctx)
	}
}

func (c *cc) VisitLiteral_value(ctx *parser.Literal_valueContext) interface{} {
	if ctx == nil {
		return todo("VisitLiteral_value", ctx)
	}

	switch {
	case ctx.Integer() != nil:
		text := ctx.Integer().GetText()
		val, err := parseIntegerValue(text)
		if err != nil {
			if debug.Active {
				log.Printf("Failed to parse integer value '%s': %v", text, err)
			}
			return todo("VisitLiteral_value", ctx.Integer())
		}
		return &ast.A_Const{Val: &ast.Integer{Ival: val}, Location: c.pos(ctx.GetStart())}

	case ctx.Real_() != nil:
		text := ctx.Real_().GetText()
		return &ast.A_Const{Val: &ast.Float{Str: text}, Location: c.pos(ctx.GetStart())}

	case ctx.STRING_VALUE() != nil: // !!! debug !!! (problem with quoted strings)
		val := ctx.STRING_VALUE().GetText()
		if len(val) >= 2 {
			val = val[1 : len(val)-1]
		}
		return &ast.A_Const{Val: &ast.String{Str: val}, Location: c.pos(ctx.GetStart())}

	case ctx.Bool_value() != nil:
		var i bool
		if ctx.Bool_value().TRUE() != nil {
			i = true
		}
		return &ast.A_Const{Val: &ast.Boolean{Boolval: i}, Location: c.pos(ctx.GetStart())}

	case ctx.NULL() != nil:
		return &ast.Null{}

	case ctx.CURRENT_TIME() != nil:
		log.Fatalf("CURRENT_TIME is not supported yet")
		return todo("VisitLiteral_value", ctx)

	case ctx.CURRENT_DATE() != nil:
		log.Fatalf("CURRENT_DATE is not supported yet")
		return todo("VisitLiteral_value", ctx)

	case ctx.CURRENT_TIMESTAMP() != nil:
		log.Fatalf("CURRENT_TIMESTAMP is not supported yet")
		return todo("VisitLiteral_value", ctx)

	case ctx.BLOB() != nil:
		blobText := ctx.BLOB().GetText()
		return &ast.A_Const{Val: &ast.String{Str: blobText}, Location: c.pos(ctx.GetStart())}

	case ctx.EMPTY_ACTION() != nil:
		if debug.Active {
			log.Printf("TODO: Implement EMPTY_ACTION")
		}
		return &ast.TODO{}

	default:
		return todo("VisitLiteral_value", ctx)
	}
}

func (c *cc) VisitSql_stmt(ctx *parser.Sql_stmtContext) interface{} {
	if ctx == nil || ctx.Sql_stmt_core() == nil {
		return todo("VisitSql_stmt", ctx)
	}

	expr, ok := ctx.Sql_stmt_core().Accept(c).(ast.Node)
	if !ok {
		return todo("VisitSql_stmt", ctx.Sql_stmt_core())
	}

	if ctx.EXPLAIN() != nil {
		options := &ast.List{Items: []ast.Node{}}

		if ctx.QUERY() != nil && ctx.PLAN() != nil {
			queryPlan := "QUERY PLAN"
			options.Items = append(options.Items, &ast.DefElem{
				Defname: &queryPlan,
				Arg:     &ast.TODO{},
			})
		}

		return &ast.ExplainStmt{
			Query:   expr,
			Options: options,
		}
	}

	return expr
}
