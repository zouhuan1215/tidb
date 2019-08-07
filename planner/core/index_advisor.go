package core

import (
	"reflect"

	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/table"
)

// TableInfoSets includes in the table's sets for forming virtual indices.
type TableInfoSets struct {
	TblInfo *model.TableInfo
	Eq      []model.CIStr
	O       [][]model.CIStr
	Rg      []model.CIStr
	Ref     []model.CIStr
}

// List set names.
const (
	EQ    = "eq"
	RANGE = "rg"
	REF   = "ref"
)

type vi struct {
	tblInfo *model.TableInfo

	candidateIndices []*model.IndexInfo
}

// QueryExprInfo includes in a query's Column and ScalarFunction.
type QueryExprInfo struct {
	ScalarFuncExpr []*expression.ScalarFunction
	ColumnExpr     [][]*expression.Column
	ProjExpr       []expression.Expression
	Ds             []*dataSource
}

type dataSource struct {
	Table       table.Table
	TableAsName *model.CIStr
	ColCnt      int
}

// NewQueryExprInfo constructs the expression information of the query.
func NewQueryExprInfo(plan LogicalPlan) (queryInfo QueryExprInfo) {
	aggItems := []*expression.Column{}
	travelLogicalPlan(plan, &queryInfo, &aggItems)
	excess := 0
	for _, ds := range queryInfo.Ds {
		excess += ds.ColCnt
	}
	if len(aggItems) >= excess {
		aggItems = aggItems[:len(aggItems)-excess]
	}
	if len(aggItems) > 0 {
		queryInfo.ColumnExpr = append(queryInfo.ColumnExpr, aggItems)
	}
	queryInfo.ScalarFuncExpr = getAllScalarFunc(queryInfo.ScalarFuncExpr)
	return queryInfo
}

func travelLogicalPlan(plan LogicalPlan, q *QueryExprInfo, aggItems *[]*expression.Column) {
	if plan == nil {
		return
	}

	switch logic := plan.(type) {
	case *LogicalProjection:
		q.ProjExpr = append(q.ProjExpr, logic.Exprs...)
	case *LogicalSelection:
		for _, e := range logic.Conditions {
			switch x := e.(type) {
			case *expression.ScalarFunction:
				q.ScalarFuncExpr = append(q.ScalarFuncExpr, x)
			}
		}
	case *LogicalAggregation:
		for _, af := range logic.AggFuncs {
			e := af.Args[0]
			switch x := e.(type) {
			case *expression.Column:
				*aggItems = append(*aggItems, x)
			}
		}
		q.ColumnExpr = append(q.ColumnExpr, logic.groupByCols)
	case *LogicalJoin:
		q.ScalarFuncExpr = append(q.ScalarFuncExpr, logic.EqualConditions...)
	case *LogicalSort:
		sortItems := []*expression.Column{}
		for _, e := range logic.ByItems {
			switch x := e.Expr.(type) {
			case *expression.Column:
				sortItems = append(sortItems, x)
			}
		}
		q.ColumnExpr = append(q.ColumnExpr, sortItems)
	case *DataSource:
		ds := &dataSource{Table: logic.table,
			TableAsName: logic.TableAsName,
			ColCnt:      len(logic.Columns),
		}
		q.Ds = append(q.Ds, ds)
	}

	for _, p := range plan.Children() {
		travelLogicalPlan(p, q, aggItems)
	}
}

func getAllScalarFunc(functions []*expression.ScalarFunction) []*expression.ScalarFunction {
	allScalarFunc := []*expression.ScalarFunction{}
	for _, f := range functions {
		scalarFunc := []*expression.ScalarFunction{}
		recursiveGetScalarFunc(f, &scalarFunc)
		allScalarFunc = append(allScalarFunc, scalarFunc...)
	}
	return allScalarFunc
}

func recursiveGetScalarFunc(f *expression.ScalarFunction, functions *[]*expression.ScalarFunction) {
	switch f.FuncName.L {
	case "or", "and":
		args := f.GetArgs()
		switch e := args[0].(type) {
		case *expression.ScalarFunction:
			recursiveGetScalarFunc(e, functions)
		}
		switch e := args[1].(type) {
		case *expression.ScalarFunction:
			recursiveGetScalarFunc(e, functions)
		}
	default:
		*functions = append(*functions, f)
	}
}

// NewTableInfoSets constructs the table and its sets for forming virtual indices with queryInfo.
func NewTableInfoSets(queryInfo QueryExprInfo) map[int64]*TableInfoSets {
	tblInfoMap := make(map[int64]*TableInfoSets)
	for _, ds := range queryInfo.Ds {
		meta := ds.Table.Meta()
		tblInfoMap[meta.ID] = &TableInfoSets{TblInfo: meta}
	}

	// form eq or rg
	for _, expr := range queryInfo.ScalarFuncExpr {
		var flag string
		switch expr.FuncName.L {
		case "eq", "in":
			flag = EQ
		case "gt", "lt", "ne":
			flag = RANGE
		}

		args := expr.GetArgs()
		for _, arg := range args {
			switch e := arg.(type) {
			case *expression.Column:
				addToSet(e, &tblInfoMap, flag)
			}
		}
	}

	// form o
	for _, expr := range queryInfo.ColumnExpr {
		tblNameSet := splitColumns(expr)
		for name, set := range tblNameSet {
			addToOSet(name, *set, &tblInfoMap)
		}
	}

	for _, tblInfoSets := range tblInfoMap {
		tblInfoSets.O = removeRepeatedColumnSet(tblInfoSets.O)
	}

	// form ref
	for _, expr := range queryInfo.ProjExpr {
		switch e := expr.(type) {
		case *expression.Column:
			flag := REF
			addToSet(e, &tblInfoMap, flag)
		}
	}

	return tblInfoMap
}

func addToSet(e *expression.Column, tblInfoMap *map[int64]*TableInfoSets, flag string) {
	if e.OrigColName.O == "" {
		return
	}

	colName := e.OrigColName
	tblName := e.OrigTblName
	for _, tblInfoSets := range *tblInfoMap {
		if tblInfoSets.TblInfo.Name.L == tblName.L {
			switch flag {
			case EQ:
				tblInfoSets.Eq = append(tblInfoSets.Eq, colName)
			case RANGE:
				tblInfoSets.Rg = append(tblInfoSets.Rg, colName)
			case REF:
				tblInfoSets.Ref = append(tblInfoSets.Ref, colName)
			}
		}
	}
}

func addToOSet(name string, set []model.CIStr, tblInfoMap *map[int64]*TableInfoSets) {
	for _, tblInfoSets := range *tblInfoMap {
		if tblInfoSets.TblInfo.Name.L == name {
			tblInfoSets.O = append(tblInfoSets.O, set)
		}
	}
}

func splitColumns(columnExpr []*expression.Column) map[string]*[]model.CIStr {
	tblNameSet := make(map[string]*[]model.CIStr)
	for _, expr := range columnExpr {
		if _, is := tblNameSet[expr.OrigTblName.L]; !is {
			tblNameSet[expr.OrigTblName.L] = &[]model.CIStr{expr.OrigColName}
		}
		*tblNameSet[expr.OrigTblName.L] = append(*tblNameSet[expr.OrigTblName.L], expr.OrigColName)
	}

	for _, columns := range tblNameSet {
		*columns = removeRepeatedColumn(*columns)
	}

	return tblNameSet
}

func removeRepeatedColumn(columns []model.CIStr) (ret []model.CIStr) {
	ret = make([]model.CIStr, 0)
	for _, s := range columns {
		if len(ret) == 0 {
			ret = append(ret, s)
		} else {
			for i, v := range ret {
				if reflect.DeepEqual(s, v) {
					break
				}
				if i == len(ret)-1 {
					ret = append(ret, s)
				}
			}
		}
	}
	return
}

func removeRepeatedColumnSet(columnSet [][]model.CIStr) (ret [][]model.CIStr) {
	ret = make([][]model.CIStr, 0)
	for _, s := range columnSet {
		if len(ret) == 0 {
			ret = append(ret, s)
		} else {
			for i, v := range ret {
				if reflect.DeepEqual(s, v) {
					break
				}
				if i == len(ret)-1 {
					ret = append(ret, s)
				}
			}
		}
	}
	return
}

