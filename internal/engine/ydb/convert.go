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
	paramCount int
	content    string
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
		return todo("Drop_role_stmtContext", ctx)
	}

	stmt := &ast.DropRoleStmt{
		MissingOk: ctx.IF() != nil && ctx.EXISTS() != nil,
		Roles:     &ast.List{},
	}

	for _, role := range ctx.AllRole_name() {
		member, isParam, _ := c.extractRoleSpec(role, ast.RoleSpecType(1))
		if member == nil {
			return todo("Drop_role_stmtContext", ctx)
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
		return todo("convertAlter_group_stmtContext", ctx)
	}
	role, paramFlag, _ := c.extractRoleSpec(ctx.Role_name(0), ast.RoleSpecType(1))
	if role == nil {
		return todo("convertAlter_group_stmtContext", ctx)
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
		newName := ctx.Role_name(1).Accept(c)
		action := "rename"

		defElem := &ast.DefElem{
			Defname:   &action,
			Defaction: ast.DefElemAction(1),
			Location:  c.pos(ctx.Role_name(1).GetStart()),
		}

		bindFlag := true
		if newNameNode, ok := newName.(ast.Node); ok {
			switch v := newNameNode.(type) {
			case *ast.A_Const:
				switch val := v.Val.(type) {
				case *ast.String:
					bindFlag = false
					defElem.Arg = val
				case *ast.Boolean:
					defElem.Arg = val
				default:
					return todo("convertAlter_group_stmtContext", ctx)
				}
			case *ast.ParamRef, *ast.A_Expr:
				defElem.Arg = newNameNode
			default:
				return todo("convertAlter_group_stmtContext", ctx)
			}
		} else {
			return todo("convertAlter_group_stmtContext", ctx)
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
				return todo("convertAlter_group_stmtContext", ctx)
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
			Location:  c.pos(ctx.GetStart()),
		})
	}

	return stmt
}

func (c *cc) VisitAlter_user_stmt(ctx *parser.Alter_user_stmtContext) interface{} {
	if ctx.ALTER() == nil || ctx.USER() == nil || len(ctx.AllRole_name()) == 0 {
		return todo("Alter_user_stmtContext", ctx)
	}

	role, paramFlag, _ := c.extractRoleSpec(ctx.Role_name(0), ast.RoleSpecType(1))
	if role == nil {
		return todo("convertAlter_group_stmtContext", ctx)
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
		newName := ctx.Role_name(1).Accept(c)
		action := "rename"

		defElem := &ast.DefElem{
			Defname:   &action,
			Defaction: ast.DefElemAction(1),
			Location:  c.pos(ctx.Role_name(1).GetStart()),
		}

		bindFlag := true
		if newNameNode, ok := newName.(ast.Node); ok {
			switch v := newNameNode.(type) {
			case *ast.A_Const:
				switch val := v.Val.(type) {
				case *ast.String:
					bindFlag = false
					defElem.Arg = val
				case *ast.Boolean:
					defElem.Arg = val
				default:
					return todo("Alter_user_stmtContext", ctx)
				}
			case *ast.ParamRef, *ast.A_Expr:
				defElem.Arg = newNameNode
			default:
				return todo("Alter_user_stmtContext", ctx)
			}
		} else {
			return todo("Alter_user_stmtContext", ctx)
		}

		if debug.Active && !paramFlag && bindFlag {
			log.Printf("YDB does not currently support parameters in the ALTER USER statement")
		}

		stmt.Options.Items = append(stmt.Options.Items, defElem)

	case len(ctx.AllUser_option()) > 0:
		for _, opt := range ctx.AllUser_option() {
			if result := opt.Accept(c); result != nil {
				if node, ok := result.(ast.Node); ok {
					stmt.Options.Items = append(stmt.Options.Items, node)
				}
			}
		}
	}

	return stmt
}

func (c *cc) VisitCreate_group_stmt(ctx *parser.Create_group_stmtContext) interface{} {
	if ctx.CREATE() == nil || ctx.GROUP() == nil || len(ctx.AllRole_name()) == 0 {
		return todo("Create_group_stmtContext", ctx)
	}
	groupName := ctx.Role_name(0).Accept(c)

	stmt := &ast.CreateRoleStmt{
		StmtType: ast.RoleStmtType(3),
		Options:  &ast.List{},
	}

	paramFlag := true
	if groupNameNode, ok := groupName.(ast.Node); ok {
		switch v := groupNameNode.(type) {
		case *ast.A_Const:
			switch val := v.Val.(type) {
			case *ast.String:
				paramFlag = false
				stmt.Role = &val.Str
			case *ast.Boolean:
				stmt.BindRole = groupNameNode
			default:
				return todo("convertCreate_group_stmtContext", ctx)
			}
		case *ast.ParamRef, *ast.A_Expr:
			stmt.BindRole = groupNameNode
		default:
			return todo("convertCreate_group_stmtContext", ctx)
		}
	} else {
		return todo("convertCreate_group_stmtContext", ctx)
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
				return todo("convertCreate_group_stmtContext", ctx)
			}

			if debug.Active && isParam && !paramFlag {
				log.Printf("YDB does not currently support parameters in the CREATE GROUP statement")
			}

			optionList.Items = append(optionList.Items, member)
		}

		stmt.Options.Items = append(stmt.Options.Items, &ast.DefElem{
			Defname:  &defname,
			Arg:      optionList,
			Location: c.pos(ctx.GetStart()),
		})
	}

	return stmt
}

func (c *cc) VisitUse_stmt(ctx *parser.Use_stmtContext) interface{} {
	if ctx.USE() != nil && ctx.Cluster_expr() != nil {
		clusterExpr := ctx.Cluster_expr().Accept(c)
		if clusterExprNode, ok := clusterExpr.(ast.Node); ok {
			stmt := &ast.UseStmt{
				Xpr:      clusterExprNode,
				Location: c.pos(ctx.GetStart()),
			}
			return stmt
		}
	}
	return todo("convertUse_stmtContext", ctx)
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
			if result := bp.Accept(c); result != nil {
				if nodeResult, ok := result.(ast.Node); ok {
					node = nodeResult
				}
			}
		}
	case ctx.ASTERISK() != nil:
		node = &ast.A_Star{}
	default:
		return todo("convertCluster_exprContext", ctx)
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
		return todo("convertCreate_user_stmtContext", ctx)
	}
	roleNode := ctx.Role_name().Accept(c)

	stmt := &ast.CreateRoleStmt{
		StmtType: ast.RoleStmtType(2),
		Options:  &ast.List{},
	}

	paramFlag := true
	if roleNodeResult, ok := roleNode.(ast.Node); ok {
		switch v := roleNodeResult.(type) {
		case *ast.A_Const:
			switch val := v.Val.(type) {
			case *ast.String:
				paramFlag = false
				stmt.Role = &val.Str
			case *ast.Boolean:
				stmt.BindRole = roleNodeResult
			default:
				return todo("convertCreate_user_stmtContext", ctx)
			}
		case *ast.ParamRef, *ast.A_Expr:
			stmt.BindRole = roleNodeResult
		default:
			return todo("convertCreate_user_stmtContext", ctx)
		}
	} else {
		return todo("convertCreate_user_stmtContext", ctx)
	}

	if debug.Active && paramFlag {
		log.Printf("YDB does not currently support parameters in the CREATE USER statement")
	}

	if len(ctx.AllUser_option()) > 0 {
		options := []ast.Node{}
		for _, opt := range ctx.AllUser_option() {
			if result := opt.Accept(c); result != nil {
				if node, ok := result.(ast.Node); ok {
					options = append(options, node)
				}
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
		return todo("convertUser_optionContext", ctx)
	}
	return nil
}

func (c *cc) VisitRole_name(ctx *parser.Role_nameContext) interface{} {
	switch {
	case ctx.An_id_or_type() != nil:
		name := parseAnIdOrType(ctx.An_id_or_type())
		return &ast.A_Const{Val: NewIdentifier(name), Location: c.pos(ctx.GetStart())}
	case ctx.Bind_parameter() != nil:
		bindPar := ctx.Bind_parameter().Accept(c)
		if bindParNode, ok := bindPar.(ast.Node); ok {
			return bindParNode
		}
		return &ast.TODO{}
	}
	return todo("convertRole_nameContext", ctx)
}

func (c *cc) VisitCommit_stmt(ctx *parser.Commit_stmtContext) interface{} {
	if ctx.COMMIT() != nil {
		return &ast.TransactionStmt{Kind: ast.TransactionStmtKind(3)}
	}
	return todo("convertCommit_stmtContext", ctx)
}

func (c *cc) VisitRollback_stmt(ctx *parser.Rollback_stmtContext) interface{} {
	if ctx.ROLLBACK() != nil {
		return &ast.TransactionStmt{Kind: ast.TransactionStmtKind(4)}
	}
	return todo("convertRollback_stmtContext", ctx)
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
	return todo("convertDrop_Table_stmtContxt", ctx)
}

func (c *cc) VisitDelete_stmt(ctx *parser.Delete_stmtContext) interface{} {
	batch := ctx.BATCH() != nil

	tableName := identifier(ctx.Simple_table_ref().Simple_table_ref_core().GetText())
	rel := &ast.RangeVar{Relname: &tableName}

	var where ast.Node
	if ctx.WHERE() != nil && ctx.Expr() != nil {
		if whereResult := ctx.Expr().Accept(c); whereResult != nil {
			if whereNode, ok := whereResult.(ast.Node); ok {
				where = whereNode
			}
		}
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
				source = &ast.SelectStmt{
					ValuesLists: func() *ast.List {
						if valuesResult := valSource.Values_stmt().Accept(c); valuesResult != nil {
							if valuesList, ok := valuesResult.(*ast.List); ok {
								return valuesList
							}
						}
						return &ast.List{}
					}(),
					FromClause: &ast.List{},
					TargetList: &ast.List{},
				}

			case valSource.Select_stmt() != nil:
				if selectResult := valSource.Select_stmt().Accept(c); selectResult != nil {
					if selectNode, ok := selectResult.(ast.Node); ok {
						source = selectNode
					}
				}
			}
		}
	}

	returning := &ast.List{}
	if ret := ctx.Returning_columns_list(); ret != nil {
		if retResult := ret.Accept(c); retResult != nil {
			if retList, ok := retResult.(*ast.List); ok {
				returning = retList
			}
		}
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
				if valResult := val.Accept(c); valResult != nil {
					if valNode, ok := valResult.(ast.Node); ok {
						stmt.Values = &ast.List{Items: []ast.Node{valNode}}
					}
				}
			}
		} else if lp := ctx.LPAREN(); lp != nil {
			values := []ast.Node{}
			for _, v := range ctx.AllPragma_value() {
				if vResult := v.Accept(c); vResult != nil {
					if vNode, ok := vResult.(ast.Node); ok {
						values = append(values, vNode)
					}
				}
			}
			stmt.Values = &ast.List{Items: values}
		}

		return stmt
	}
	return todo("convertPragma_stmtContext", ctx)
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
		if bindParNode, ok := bindPar.(ast.Node); ok {
			return bindParNode
		}
		return &ast.TODO{}
	}

	return todo("convertPragma_valueContext", ctx)
}

func (c *cc) VisitUpdate_stmt(ctx *parser.Update_stmtContext) interface{} {
	if ctx.UPDATE() == nil {
		return nil
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
				if exprResult := clause.Expr().Accept(c); exprResult != nil {
					if exprNode, ok := exprResult.(ast.Node); ok {
						resTarget := &ast.ResTarget{
							Name:     &columnName,
							Val:      exprNode,
							Location: c.pos(clause.Expr().GetStart()),
						}
						setList.Items = append(setList.Items, resTarget)
					}
				}
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
					if exprResult := expr.Accept(c); exprResult != nil {
						if exprNode, ok := exprResult.(ast.Node); ok {
							rowExpr.Args.Items = append(rowExpr.Args.Items, exprNode)
						}
					}
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
			if whereResult := ctx.Expr().Accept(c); whereResult != nil {
				if whereNode, ok := whereResult.(ast.Node); ok {
					where = whereNode
				}
			}
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
				source = &ast.SelectStmt{
					ValuesLists: func() *ast.List {
						if valuesResult := valSource.Values_stmt().Accept(c); valuesResult != nil {
							if valuesList, ok := valuesResult.(*ast.List); ok {
								return valuesList
							}
						}
						return &ast.List{}
					}(),
					FromClause: &ast.List{},
					TargetList: &ast.List{},
				}

			case valSource.Select_stmt() != nil:
				if selectResult := valSource.Select_stmt().Accept(c); selectResult != nil {
					if selectNode, ok := selectResult.(ast.Node); ok {
						source = selectNode
					}
				}
			}
		}
	}

	returning := &ast.List{}
	if ret := ctx.Returning_columns_list(); ret != nil {
		if retResult := ret.Accept(c); retResult != nil {
			if retList, ok := retResult.(*ast.List); ok {
				returning = retList
			}
		}
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
				source = &ast.SelectStmt{
					ValuesLists: func() *ast.List {
						if valuesResult := valSource.Values_stmt().Accept(c); valuesResult != nil {
							if valuesList, ok := valuesResult.(*ast.List); ok {
								return valuesList
							}
						}
						return &ast.List{}
					}(),
					FromClause: &ast.List{},
					TargetList: &ast.List{},
				}

			case valSource.Select_stmt() != nil:
				if selectResult := valSource.Select_stmt().Accept(c); selectResult != nil {
					if selectNode, ok := selectResult.(ast.Node); ok {
						source = selectNode
					}
				}
			}
		}
	}

	returning := &ast.List{}
	if ret := ctx.Returning_columns_list(); ret != nil {
		if retResult := ret.Accept(c); retResult != nil {
			if retList, ok := retResult.(*ast.List); ok {
				returning = retList
			}
		}
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
				if convertedNode, ok := converted.(ast.Node); ok {
					rowList.Items = append(rowList.Items, convertedNode)
				}
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
	skp := ctx.Select_kind_parenthesis(0)
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
		if cnode := sk.Process_core().Accept(c); cnode != nil {
			if stmt, ok := cnode.(*ast.SelectStmt); ok {
				selectStmt = stmt
			}
		}
	case sk.Select_core() != nil:
		if cnode := sk.Select_core().Accept(c); cnode != nil {
			if stmt, ok := cnode.(*ast.SelectStmt); ok {
				selectStmt = stmt
			}
		}
	case sk.Reduce_core() != nil:
		if cnode := sk.Reduce_core().Accept(c); cnode != nil {
			if stmt, ok := cnode.(*ast.SelectStmt); ok {
				selectStmt = stmt
			}
		}
	}

	// Handle LIMIT and OFFSET
	if partial.LIMIT() != nil {
		// Get LIMIT expression
		if limitExprs := partial.AllExpr(); len(limitExprs) > 0 {
			if limitResult := limitExprs[0].Accept(c); limitResult != nil {
				if limitNode, ok := limitResult.(ast.Node); ok {
					selectStmt.LimitCount = limitNode
				}
			}
		}

		// Get OFFSET expression (second expr if present)
		if limitExprs := partial.AllExpr(); len(limitExprs) > 1 {
			if offsetResult := limitExprs[1].Accept(c); offsetResult != nil {
				if offsetNode, ok := offsetResult.(ast.Node); ok {
					selectStmt.LimitOffset = offsetNode
				}
			}
		}
	}

	return selectStmt
}

func (c *cc) VisitSelect_core(ctx *parser.Select_coreContext) interface{} {
	stmt := &ast.SelectStmt{
		TargetList: &ast.List{},
		FromClause: &ast.List{},
	}
	if ctx.Opt_set_quantifier() != nil {
		oq := ctx.Opt_set_quantifier()
		if oq.DISTINCT() != nil {
			// todo: add distinct support
			stmt.DistinctClause = &ast.List{}
		}
	}
	resultCols := ctx.AllResult_column()
	if len(resultCols) > 0 {
		var items []ast.Node
		for _, rc := range resultCols {
			convNode := rc.Accept(c)
			if convNode != nil {
				items = append(items, convNode.(ast.Node))
			}
		}
		stmt.TargetList = &ast.List{
			Items: items,
		}
	}
	jsList := ctx.AllJoin_source()
	if len(ctx.AllFROM()) > 0 && len(jsList) > 0 {
		var fromItems []ast.Node
		for _, js := range jsList {
			jsCon, ok := js.(*parser.Join_sourceContext)
			if !ok {
				continue
			}

			joinNode := jsCon.Accept(c)
			if joinNode != nil {
				fromItems = append(fromItems, joinNode.(ast.Node))
			}
		}
		stmt.FromClause = &ast.List{
			Items: fromItems,
		}
	}
	if ctx.WHERE() != nil {
		whereCtx := ctx.Expr(0)
		if whereCtx != nil {
			if whereResult := whereCtx.Accept(c); whereResult != nil {
				if whereNode, ok := whereResult.(ast.Node); ok {
					stmt.WhereClause = whereNode
				}
			}
		}
	}
	return stmt
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
		if iexprResult := iexpr.Accept(c); iexprResult != nil {
			if iexprNode, ok := iexprResult.(ast.Node); ok {
				val = iexprNode
			}
		}
	}

	if val == nil {
		return nil
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
	if ctx == nil {
		return nil
	}
	fsList := ctx.AllFlatten_source()
	if len(fsList) == 0 {
		return nil
	}
	joinOps := ctx.AllJoin_op()
	joinConstraints := ctx.AllJoin_constraint()

	// todo: add ANY support

	leftNode := fsList[0].Accept(c)
	if leftNode == nil {
		return nil
	}
	for i, jopCtx := range joinOps {
		if i+1 >= len(fsList) {
			break
		}
		rightNode := fsList[i+1].Accept(c)
		if rightNode == nil {
			return leftNode
		}
		jexpr := &ast.JoinExpr{
			Larg: leftNode.(ast.Node),
			Rarg: rightNode.(ast.Node),
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
						if exprResult := exprCtx.Accept(c); exprResult != nil {
							if exprNode, ok := exprResult.(ast.Node); ok {
								jexpr.Quals = exprNode
							}
						}
					}
				case jc.USING() != nil:
					if pureListCtx := jc.Pure_column_or_named_list(); pureListCtx != nil {
						var using ast.List
						pureItems := pureListCtx.AllPure_column_or_named()
						for _, pureCtx := range pureItems {
							if anID := pureCtx.An_id(); anID != nil {
								using.Items = append(using.Items, NewIdentifier(parseAnId(anID)))
							} else if bp := pureCtx.Bind_parameter(); bp != nil {
								if bindParResult := bp.Accept(c); bindParResult != nil {
									if bindParNode, ok := bindParResult.(ast.Node); ok {
										using.Items = append(using.Items, bindParNode)
									}
								}
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

func (c *cc) VisitFlatten_source(ctx *parser.Flatten_sourceContext) interface{} {
	if ctx == nil {
		return nil
	}
	nss := ctx.Named_single_source()
	if nss == nil {
		return nil
	}
	namedSingleSource, ok := nss.(*parser.Named_single_sourceContext)
	if !ok {
		return nil
	}
	return namedSingleSource.Accept(c)
}

func (c *cc) VisitNamed_single_source(ctx *parser.Named_single_sourceContext) interface{} {
	ss := ctx.Single_source()
	if ss == nil {
		return nil
	}
	SingleSource, ok := ss.(*parser.Single_sourceContext)
	if !ok {
		return nil
	}
	base := SingleSource.Accept(c)

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
	if ctx.Table_ref() != nil {
		tableName := ctx.Table_ref().GetText() // !! debug !!
		return &ast.RangeVar{
			Relname:  &tableName,
			Location: c.pos(ctx.GetStart()),
		}
	}

	if ctx.Select_stmt() != nil {
		if subqueryResult := ctx.Select_stmt().Accept(c); subqueryResult != nil {
			if subqueryNode, ok := subqueryResult.(ast.Node); ok {
				return &ast.RangeSubselect{
					Subquery: subqueryNode,
				}
			}
		}
	}
	// todo: Values stmt

	return nil
}

func (c *cc) VisitBind_parameter(ctx *parser.Bind_parameterContext) interface{} {
	if ctx.DOLLAR() != nil {
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
		c.paramCount++
		return &ast.ParamRef{
			Number:   c.paramCount,
			Location: c.pos(ctx.GetStart()),
			Dollar:   true,
			Plike:    true,
		}
	}
	return &ast.TODO{}
}

func (c *cc) convertWildCardField(ctx *parser.Result_columnContext) *ast.ColumnRef {
	prefixCtx := ctx.Opt_id_prefix()
	prefix := prefixCtx.Accept(c)

	items := []ast.Node{}
	if prefix != nil {
		items = append(items, NewIdentifier(prefix.(string)))
	}

	items = append(items, &ast.A_Star{})
	return &ast.ColumnRef{
		Fields:   &ast.List{Items: items},
		Location: c.pos(ctx.GetStart()),
	}
}

func (c *cc) VisitOptIdPrefix(ctx *parser.Opt_id_prefixContext) interface{} {
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
	for _, idef := range ctx.AllCreate_table_entry() {
		if def, ok := idef.(*parser.Create_table_entryContext); ok {
			switch {
			case def.Column_schema() != nil:
				if colCtx, ok := def.Column_schema().(*parser.Column_schemaContext); ok {
					colDef := colCtx.Accept(c)
					if colDef != nil {
						stmt.Cols = append(stmt.Cols, colDef.(*ast.ColumnDef))
					}
				}
			case def.Table_constraint() != nil:
				if conCtx, ok := def.Table_constraint().(*parser.Table_constraintContext); ok {
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

func (c *cc) VisitColumn_schema(ctx *parser.Column_schemaContext) interface{} {
	col := &ast.ColumnDef{}

	if anId := ctx.An_id_schema(); anId != nil {
		col.Colname = identifier(parseAnIdSchema(anId))
	}
	if tnb := ctx.Type_name_or_bind(); tnb != nil {
		col.TypeName = tnb.Accept(c).(*ast.TypeName)
	}
	if colCons := ctx.Opt_column_constraints(); colCons != nil {
		col.IsNotNull = colCons.NOT() != nil && colCons.NULL() != nil
		//todo: cover exprs if needed
	}
	// todo: family

	return col
}

func (c *cc) VisitType_name_or_bind(ctx *parser.Type_name_or_bindContext) interface{} {
	if t := ctx.Type_name(); t != nil {
		return t.Accept(c)
	} else if b := ctx.Bind_parameter(); b != nil {
		return &ast.TypeName{Name: "BIND:" + identifier(parseAnIdOrType(b.An_id_or_type()))}
	}
	return nil
}

func (c *cc) VisitType_name(ctx *parser.Type_nameContext) interface{} {
	if ctx == nil {
		return nil
	}

	if composite := ctx.Type_name_composite(); composite != nil {
		if node := composite.Accept(c); node != nil {
			if typeName, ok := node.(*ast.TypeName); ok {
				return typeName
			}
		}
	}

	if decimal := ctx.Type_name_decimal(); decimal != nil {
		if integerOrBinds := decimal.AllInteger_or_bind(); len(integerOrBinds) >= 2 {
			return &ast.TypeName{
				Name:    "Decimal",
				TypeOid: 0,
				Names: &ast.List{
					Items: []ast.Node{
						integerOrBinds[0].Accept(c).(ast.Node),
						integerOrBinds[1].Accept(c).(ast.Node),
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

	return nil
}

func (c *cc) VisitInteger_or_bind(ctx *parser.Integer_or_bindContext) interface{} {
	if ctx == nil {
		return nil
	}

	if integer := ctx.Integer(); integer != nil {
		val, err := parseIntegerValue(integer.GetText())
		if err != nil {
			return &ast.TODO{}
		}
		return &ast.Integer{Ival: val}
	}

	if bind := ctx.Bind_parameter(); bind != nil {
		return bind.Accept(c)
	}

	return nil
}

func (c *cc) VisitType_name_composite(ctx *parser.Type_name_compositeContext) interface{} {
	if ctx == nil {
		return nil
	}

	if opt := ctx.Type_name_optional(); opt != nil {
		if typeName := opt.Type_name_or_bind(); typeName != nil {
			return &ast.TypeName{
				Name:    "Optional",
				TypeOid: 0,
				Names: &ast.List{
					Items: []ast.Node{typeName.Accept(c).(ast.Node)},
				},
			}
		}
	}

	if tuple := ctx.Type_name_tuple(); tuple != nil {
		if typeNames := tuple.AllType_name_or_bind(); len(typeNames) > 0 {
			var items []ast.Node
			for _, tn := range typeNames {
				items = append(items, tn.Accept(c).(ast.Node))
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
			return &ast.TypeName{
				Name:    "List",
				TypeOid: 0,
				Names: &ast.List{
					Items: []ast.Node{typeName.Accept(c).(ast.Node)},
				},
			}
		}
	}

	if stream := ctx.Type_name_stream(); stream != nil {
		if typeName := stream.Type_name_or_bind(); typeName != nil {
			return &ast.TypeName{
				Name:    "Stream",
				TypeOid: 0,
				Names: &ast.List{
					Items: []ast.Node{typeName.Accept(c).(ast.Node)},
				},
			}
		}
	}

	if flow := ctx.Type_name_flow(); flow != nil {
		if typeName := flow.Type_name_or_bind(); typeName != nil {
			return &ast.TypeName{
				Name:    "Flow",
				TypeOid: 0,
				Names: &ast.List{
					Items: []ast.Node{typeName.Accept(c).(ast.Node)},
				},
			}
		}
	}

	if dict := ctx.Type_name_dict(); dict != nil {
		if typeNames := dict.AllType_name_or_bind(); len(typeNames) >= 2 {
			return &ast.TypeName{
				Name:    "Dict",
				TypeOid: 0,
				Names: &ast.List{
					Items: []ast.Node{
						typeNames[0].Accept(c).(ast.Node),
						typeNames[1].Accept(c).(ast.Node),
					},
				},
			}
		}
	}

	if set := ctx.Type_name_set(); set != nil {
		if typeName := set.Type_name_or_bind(); typeName != nil {
			return &ast.TypeName{
				Name:    "Set",
				TypeOid: 0,
				Names: &ast.List{
					Items: []ast.Node{typeName.Accept(c).(ast.Node)},
				},
			}
		}
	}

	if enum := ctx.Type_name_enum(); enum != nil {
		if typeTags := enum.AllType_name_tag(); len(typeTags) > 0 {
			var items []ast.Node
			for range typeTags { // todo: Handle enum tags
				items = append(items, &ast.TODO{})
			}
			return &ast.TypeName{
				Name:    "Enum",
				TypeOid: 0,
				Names:   &ast.List{Items: items},
			}
		}
	}

	if resource := ctx.Type_name_resource(); resource != nil {
		if typeTag := resource.Type_name_tag(); typeTag != nil {
			// TODO: Handle resource tag
			return &ast.TypeName{
				Name:    "Resource",
				TypeOid: 0,
				Names: &ast.List{
					Items: []ast.Node{&ast.TODO{}},
				},
			}
		}
	}

	if tagged := ctx.Type_name_tagged(); tagged != nil {
		if typeName := tagged.Type_name_or_bind(); typeName != nil {
			if typeTag := tagged.Type_name_tag(); typeTag != nil {
				// TODO: Handle tagged type and tag
				return &ast.TypeName{
					Name:    "Tagged",
					TypeOid: 0,
					Names: &ast.List{
						Items: []ast.Node{
							typeName.Accept(c).(ast.Node),
							&ast.TODO{},
						},
					},
				}
			}
		}
	}

	if callable := ctx.Type_name_callable(); callable != nil {
		// TODO: Handle callable argument list and return type
		return &ast.TypeName{
			Name:    "Callable",
			TypeOid: 0,
			Names: &ast.List{
				Items: []ast.Node{&ast.TODO{}},
			},
		}
	}

	return nil
}

func (c *cc) VisitSql_stmt_core(ctx *parser.Sql_stmt_coreContext) interface{} {
	if ctx == nil {
		return nil
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
	return nil
}

func (c *cc) VisitExpr(ctx *parser.ExprContext) interface{} {
	if ctx == nil {
		return nil
	}

	if tn := ctx.Type_name_composite(); tn != nil {
		return tn.Accept(c)
	}

	orSubs := ctx.AllOr_subexpr()
	if len(orSubs) == 0 {
		return nil
	}

	left := orSubs[0].Accept(c)
	for i := 1; i < len(orSubs); i++ {
		right := orSubs[i].Accept(c)

		left = &ast.BoolExpr{
			Boolop:   ast.BoolExprTypeOr,
			Args:     &ast.List{Items: []ast.Node{left.(ast.Node), right.(ast.Node)}},
			Location: c.pos(ctx.GetStart()),
		}
	}
	return left
}

func (c *cc) VisitOr_subexpr(ctx *parser.Or_subexprContext) interface{} {
	if ctx == nil {
		return nil
	}
	andSubs := ctx.AllAnd_subexpr()
	if len(andSubs) == 0 {
		return nil
	}

	left := andSubs[0].Accept(c)
	for i := 1; i < len(andSubs); i++ {
		right := andSubs[i].Accept(c)
		left = &ast.BoolExpr{
			Boolop:   ast.BoolExprTypeAnd,
			Args:     &ast.List{Items: []ast.Node{left.(ast.Node), right.(ast.Node)}},
			Location: c.pos(ctx.GetStart()),
		}
	}
	return left
}

func (c *cc) VisitAnd_subexpr(ctx *parser.And_subexprContext) interface{} {
	if ctx == nil {
		return nil
	}

	xors := ctx.AllXor_subexpr()
	if len(xors) == 0 {
		return nil
	}

	left := xors[0].Accept(c)
	for i := 1; i < len(xors); i++ {
		right := xors[i].Accept(c)
		left = &ast.A_Expr{
			Name:     &ast.List{Items: []ast.Node{&ast.String{Str: "XOR"}}},
			Lexpr:    left.(ast.Node),
			Rexpr:    right.(ast.Node),
			Location: c.pos(ctx.GetStart()),
		}
	}
	return left
}

func (c *cc) VisitXor_subexpr(ctx *parser.Xor_subexprContext) interface{} {
	if ctx == nil {
		return nil
	}
	es := ctx.Eq_subexpr()
	if es == nil {
		return nil
	}
	base := es.Accept(c)
	var baseNode ast.Node
	if baseResult, ok := base.(ast.Node); ok {
		baseNode = baseResult
	} else {
		return todo("VisitXor_subexpr", ctx)
	}
	if cond := ctx.Cond_expr(); cond != nil {
		switch {
		case cond.IN() != nil:
			if inExpr := cond.In_expr(); inExpr != nil {
				if inExprResult := inExpr.Accept(c); inExprResult != nil {
					if inExprNode, ok := inExprResult.(ast.Node); ok {
						return &ast.A_Expr{
							Name:  &ast.List{Items: []ast.Node{&ast.String{Str: "IN"}}},
							Lexpr: baseNode,
							Rexpr: inExprNode,
						}
					}
				}
			}
		case cond.BETWEEN() != nil:
			if eqSubs := cond.AllEq_subexpr(); len(eqSubs) >= 2 {
				if leftResult := eqSubs[0].Accept(c); leftResult != nil {
					if rightResult := eqSubs[1].Accept(c); rightResult != nil {
						if leftNode, ok := leftResult.(ast.Node); ok {
							if rightNode, ok := rightResult.(ast.Node); ok {
								return &ast.BetweenExpr{
									Expr:     baseNode,
									Left:     leftNode,
									Right:    rightNode,
									Not:      cond.NOT() != nil,
									Location: c.pos(ctx.GetStart()),
								}
							}
						}
					}
				}
			}
		case cond.ISNULL() != nil:
			return &ast.NullTest{
				Arg:          baseNode,
				Nulltesttype: 1, // IS NULL
				Location:     c.pos(ctx.GetStart()),
			}
		case cond.NOTNULL() != nil:
			return &ast.NullTest{
				Arg:          baseNode,
				Nulltesttype: 2, // IS NOT NULL
				Location:     c.pos(ctx.GetStart()),
			}
		case cond.IS() != nil && cond.NULL() != nil:
			return &ast.NullTest{
				Arg:          baseNode,
				Nulltesttype: 1, // IS NULL
				Location:     c.pos(ctx.GetStart()),
			}
		case cond.IS() != nil && cond.NOT() != nil && cond.NULL() != nil:
			return &ast.NullTest{
				Arg:          baseNode,
				Nulltesttype: 2, // IS NOT NULL
				Location:     c.pos(ctx.GetStart()),
			}
		case cond.Match_op() != nil:
			// debug!!!
			matchOp := cond.Match_op().GetText()
			if eqSubs := cond.AllEq_subexpr(); len(eqSubs) >= 1 {
				if rexprResult := eqSubs[0].Accept(c); rexprResult != nil {
					if rexprNode, ok := rexprResult.(ast.Node); ok {
						expr := &ast.A_Expr{
							Name:  &ast.List{Items: []ast.Node{&ast.String{Str: matchOp}}},
							Lexpr: baseNode,
							Rexpr: rexprNode,
						}
						if cond.ESCAPE() != nil && len(eqSubs) >= 2 { //nolint
							// todo: Add ESCAPE support
						}
						return expr
					}
				}
			}
		case len(cond.AllEQUALS()) > 0 || len(cond.AllEQUALS2()) > 0 ||
			len(cond.AllNOT_EQUALS()) > 0 || len(cond.AllNOT_EQUALS2()) > 0:
			// debug!!!
			var op string
			switch {
			case len(cond.AllEQUALS()) > 0:
				op = "="
			case len(cond.AllEQUALS2()) > 0:
				op = "=="
			case len(cond.AllNOT_EQUALS()) > 0:
				op = "!="
			case len(cond.AllNOT_EQUALS2()) > 0:
				op = "<>"
			}
			if eqSubs := cond.AllEq_subexpr(); len(eqSubs) >= 1 {
				if rexprResult := eqSubs[0].Accept(c); rexprResult != nil {
					if rexprNode, ok := rexprResult.(ast.Node); ok {
						return &ast.A_Expr{
							Name:  &ast.List{Items: []ast.Node{&ast.String{Str: op}}},
							Lexpr: baseNode,
							Rexpr: rexprNode,
						}
					}
				}
			}
		case len(cond.AllDistinct_from_op()) > 0:
			// debug!!!
			distinctOps := cond.AllDistinct_from_op()
			for _, distinctOp := range distinctOps {
				if eqSubs := cond.AllEq_subexpr(); len(eqSubs) >= 1 {
					not := distinctOp.NOT() != nil
					op := "IS DISTINCT FROM"
					if not {
						op = "IS NOT DISTINCT FROM"
					}
					if rexprResult := eqSubs[0].Accept(c); rexprResult != nil {
						if rexprNode, ok := rexprResult.(ast.Node); ok {
							return &ast.A_Expr{
								Name:  &ast.List{Items: []ast.Node{&ast.String{Str: op}}},
								Lexpr: baseNode,
								Rexpr: rexprNode,
							}
						}
					}
				}
			}
		}
	}
	return baseNode
}

func (c *cc) VisitEq_subexpr(ctx *parser.Eq_subexprContext) interface{} {
	if ctx == nil {
		return nil
	}
	neqList := ctx.AllNeq_subexpr()
	if len(neqList) == 0 {
		return nil
	}
	left := neqList[0].Accept(c)
	ops := c.collectComparisonOps(ctx)
	for i := 1; i < len(neqList); i++ {
		right := neqList[i].Accept(c)

		opText := ops[i-1].GetText()
		left = &ast.A_Expr{
			Name:     &ast.List{Items: []ast.Node{&ast.String{Str: opText}}},
			Lexpr:    left.(ast.Node),
			Rexpr:    right.(ast.Node),
			Location: c.pos(ctx.GetStart()),
		}
	}
	return left
}

func (c *cc) collectComparisonOps(ctx parser.IEq_subexprContext) []antlr.TerminalNode {
	var ops []antlr.TerminalNode
	for _, child := range ctx.GetChildren() {
		if tn, ok := child.(antlr.TerminalNode); ok {
			switch tn.GetText() {
			case "<", "<=", ">", ">=":
				ops = append(ops, tn)
			}
		}
	}
	return ops
}

func (c *cc) VisitNeq_subexpr(ctx *parser.Neq_subexprContext) interface{} {
	if ctx == nil {
		return nil
	}
	bitList := ctx.AllBit_subexpr()
	if len(bitList) == 0 {
		return nil
	}

	left := bitList[0].Accept(c)
	ops := c.collectBitwiseOps(ctx)
	for i := 1; i < len(bitList); i++ {
		right := bitList[i].Accept(c)
		opText := ops[i-1].GetText()
		left = &ast.A_Expr{
			Name:     &ast.List{Items: []ast.Node{&ast.String{Str: opText}}},
			Lexpr:    left.(ast.Node),
			Rexpr:    right.(ast.Node),
			Location: c.pos(ctx.GetStart()),
		}
	}

	if ctx.Double_question() != nil {
		nextCtx := ctx.Neq_subexpr()
		if nextCtx != nil {
			right := nextCtx.Accept(c)
			left = &ast.A_Expr{
				Name:     &ast.List{Items: []ast.Node{&ast.String{Str: "??"}}},
				Lexpr:    left.(ast.Node),
				Rexpr:    right.(ast.Node),
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
				Lexpr:    left.(ast.Node),
				Location: c.pos(ctx.GetStart()),
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

func (c *cc) VisitBit_subexpr(ctx *parser.Bit_subexprContext) interface{} {
	if ctx == nil {
		return nil
	}
	addList := ctx.AllAdd_subexpr()
	left := addList[0].Accept(c)

	ops := c.collectBitOps(ctx)
	for i := 1; i < len(addList); i++ {
		right := addList[i].Accept(c)
		opText := ops[i-1].GetText()
		left = &ast.A_Expr{
			Name:     &ast.List{Items: []ast.Node{&ast.String{Str: opText}}},
			Lexpr:    left.(ast.Node),
			Rexpr:    right.(ast.Node),
			Location: c.pos(ctx.GetStart()),
		}
	}
	return left
}

func (c *cc) collectBitOps(ctx parser.IBit_subexprContext) []antlr.TerminalNode {
	var ops []antlr.TerminalNode
	children := ctx.GetChildren()
	for _, child := range children {
		if tn, ok := child.(antlr.TerminalNode); ok {
			txt := tn.GetText()
			switch txt {
			case "+", "-":
				ops = append(ops, tn)
			}
		}
	}
	return ops
}

func (c *cc) VisitAdd_subexpr(ctx *parser.Add_subexprContext) interface{} {
	if ctx == nil {
		return nil
	}
	mulList := ctx.AllMul_subexpr()
	left := mulList[0].Accept(c)

	ops := c.collectAddOps(ctx)
	for i := 1; i < len(mulList); i++ {
		right := mulList[i].Accept(c)
		opText := ops[i-1].GetText()
		left = &ast.A_Expr{
			Name:     &ast.List{Items: []ast.Node{&ast.String{Str: opText}}},
			Lexpr:    left.(ast.Node),
			Rexpr:    right.(ast.Node),
			Location: c.pos(ctx.GetStart()),
		}
	}
	return left
}

func (c *cc) collectAddOps(ctx parser.IAdd_subexprContext) []antlr.TerminalNode {
	var ops []antlr.TerminalNode
	for _, child := range ctx.GetChildren() {
		if tn, ok := child.(antlr.TerminalNode); ok {
			switch tn.GetText() {
			case "*", "/", "%":
				ops = append(ops, tn)
			}
		}
	}
	return ops
}

func (c *cc) VisitMul_subexpr(ctx *parser.Mul_subexprContext) interface{} {
	if ctx == nil {
		return nil
	}
	conList := ctx.AllCon_subexpr()
	left := conList[0].Accept(c)

	for i := 1; i < len(conList); i++ {
		right := conList[i].Accept(c)
		left = &ast.A_Expr{
			Name:     &ast.List{Items: []ast.Node{&ast.String{Str: "||"}}},
			Lexpr:    left.(ast.Node),
			Rexpr:    right.(ast.Node),
			Location: c.pos(ctx.GetStart()),
		}
	}
	return left
}

func (c *cc) VisitCon_subexpr(ctx *parser.Con_subexprContext) interface{} {
	if ctx == nil {
		return nil
	}
	if opCtx := ctx.Unary_op(); opCtx != nil {
		op := opCtx.GetText()
		operand := ctx.Unary_subexpr().Accept(c)
		return &ast.A_Expr{
			Name:     &ast.List{Items: []ast.Node{&ast.String{Str: op}}},
			Rexpr:    operand.(ast.Node),
			Location: c.pos(ctx.GetStart()),
		}
	}
	return ctx.Unary_subexpr().Accept(c)
}

func (c *cc) VisitUnary_subexpr(ctx *parser.Unary_subexprContext) interface{} {
	if casual := ctx.Unary_casual_subexpr(); casual != nil {
		return casual.Accept(c)
	}
	if jsonExpr := ctx.Json_api_expr(); jsonExpr != nil {
		return jsonExpr.Accept(c)
	}
	return nil
}

func (c *cc) VisitJson_api_expr(ctx *parser.Json_api_exprContext) interface{} {
	return todo("Json_api_exprContext", ctx)
}

func (c *cc) VisitUnary_casual_subexpr(ctx *parser.Unary_casual_subexprContext) interface{} {
	if ctx == nil {
		return nil
	}
	var current ast.Node
	switch {
	case ctx.Id_expr() != nil:
		current = ctx.Id_expr().Accept(c).(ast.Node)
	case ctx.Atom_expr() != nil:
		current = ctx.Atom_expr().Accept(c).(ast.Node)
	default:
		return todo("Unary_casual_subexprContext", ctx)
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
			}
		}
	}
	return current
}

func (c *cc) handleKeySuffix(base ast.Node, keyCtx *parser.Key_exprContext) ast.Node {
	keyNode := keyCtx.Accept(c).(ast.Node)
	ind, ok := keyNode.(*ast.A_Indirection)
	if !ok {
		return todo("Key_exprContext", keyCtx)
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
	funcCall, ok := invokeCtx.Accept(c).(*ast.FuncCall)
	if !ok {
		return todo("Invoke_exprContext", invokeCtx)
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
			return todo("Invoke_exprContext", invokeCtx)
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
		field = v.Accept(c).(ast.Node)
	case *parser.An_id_or_typeContext:
		field = &ast.String{Str: parseAnIdOrType(v)}
	case antlr.TerminalNode:
		if val, err := parseIntegerValue(v.GetText()); err == nil {
			field = &ast.A_Const{Val: &ast.Integer{Ival: val}}
		} else {
			return &ast.TODO{}
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
		return todo("Key_exprContext", ctx)
	}

	stmt := &ast.A_Indirection{
		Indirection: &ast.List{},
	}

	expr := ctx.Expr().Accept(c).(ast.Node)

	stmt.Indirection.Items = append(stmt.Indirection.Items, &ast.A_Indices{
		Uidx: expr,
	})

	return stmt
}

func (c *cc) VisitInvoke_expr(ctx *parser.Invoke_exprContext) interface{} {
	if ctx.LPAREN() == nil || ctx.RPAREN() == nil {
		return todo("Invoke_exprContext", ctx)
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
			expr := namedExpr.Expr().Accept(c).(ast.Node)

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
	return &ast.TODO{}
}

func (c *cc) VisitAtom_expr(ctx *parser.Atom_exprContext) interface{} {
	switch {
	case ctx.An_id_or_type() != nil && ctx.NAMESPACE() != nil:
		return NewIdentifier(parseAnIdOrType(ctx.An_id_or_type()) + "::" + parseIdOrType(ctx.Id_or_type()))
	case ctx.An_id_or_type() != nil:
		return NewIdentifier(parseAnIdOrType(ctx.An_id_or_type()))
	case ctx.Literal_value() != nil:
		return ctx.Literal_value().Accept(c)
	case ctx.Bind_parameter() != nil:
		return ctx.Bind_parameter().Accept(c).(ast.Node)
	default:
		return &ast.TODO{}
	}
}

func (c *cc) VisitLiteral_value(ctx *parser.Literal_valueContext) interface{} {
	switch {
	case ctx.Integer() != nil:
		text := ctx.Integer().GetText()
		val, err := parseIntegerValue(text)
		if err != nil {
			if debug.Active {
				log.Printf("Failed to parse integer value '%s': %v", text, err)
			}
			return &ast.TODO{}
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
		if debug.Active {
			log.Printf("TODO: Implement CURRENT_TIME")
		}
		return &ast.TODO{}

	case ctx.CURRENT_DATE() != nil:
		if debug.Active {
			log.Printf("TODO: Implement CURRENT_DATE")
		}
		return &ast.TODO{}

	case ctx.CURRENT_TIMESTAMP() != nil:
		if debug.Active {
			log.Printf("TODO: Implement CURRENT_TIMESTAMP")
		}
		return &ast.TODO{}

	case ctx.BLOB() != nil:
		blobText := ctx.BLOB().GetText()
		return &ast.A_Const{Val: &ast.String{Str: blobText}, Location: c.pos(ctx.GetStart())}

	case ctx.EMPTY_ACTION() != nil:
		if debug.Active {
			log.Printf("TODO: Implement EMPTY_ACTION")
		}
		return &ast.TODO{}

	default:
		if debug.Active {
			log.Printf("Unknown literal value type: %T", ctx)
		}
		return &ast.TODO{}
	}
}

func (c *cc) VisitSql_stmt(ctx *parser.Sql_stmtContext) interface{} {
	if ctx == nil {
		return nil
	}
	// todo: handle explain
	if core := ctx.Sql_stmt_core(); core != nil {
		return core.Accept(c)
	}

	return nil
}
