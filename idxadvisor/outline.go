package idxadvisor

import (
	"database/sql"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/ddl"

	"github.com/pingcap/tidb/infoschema"
)

// idxAdvPool stores initialized but not set up IdxAdvisor
// initialized IdxAdvisor: initialized dbClient member
// set up IdxAdvisor: both dbClient and QueryCtx member are initialized
type idxAdvPool []*IdxAdvisor

var registeredIdxAdv = make(map[uint64]*IdxAdvisor)
var idxadvPool idxAdvPool = make(idxAdvPool, 0)

func (iap *idxAdvPool) push(ia *IdxAdvisor) {
	*iap = append(*iap, ia)
}

func (iap *idxAdvPool) pop() (*IdxAdvisor, error) {
	if iap.empty() {
		return nil, errors.New("idxAdvPool is empty!")
	}
	ia := (*iap)[len(*iap)-1]
	(*iap) = (*iap)[:len(*iap)-1]
	return ia, nil
}

func (iap *idxAdvPool) empty() bool {
	return len(*iap) == 0
}

//type IndexAdvisor interface {
//	ExtractClause(sel *ast.SelectStmt)
//	BuildVirtualTblInfo()                                          // input: original tblInfo, output newtablInfo, combine GenVirtualIdx and GenVirtualIdxStats?
//	CompareCost() (original_cost, virtual_cost, selected_idx, err) // input: need a session? virtualTblInfo, build two different logical_plan_builder
//}

// indexAdviosr implements ExtractClause and CompareCost() method, BuildVirtualTblInfo() is implemented by Single or Multiple
type IdxAdvisor struct {
	dbClient *sql.DB
	//	IdxAdvCtx *IdxAdvContext

	ready atomic.Value // initialized(1) or not(0)
	//	Candidate_idx  []*CandidateIdx
}

//type IdxAdvContext struct {
//	VirtualIS    infoschema.InfoSchema
//	TableName    model.CIStr
//	OriginalCost float64
//	VirtualCost  float64
//	SelectedIdx  []*model.IndexInfo
//}

//type idxColSet struct {
//	O     [][]*ast.IndexColName
//	Order [][]*ast.IndexColName
//	Range [][]*ast.IndexColName
//	Ref   [][]*ast.IndexColName
//}
//
//type CandidateIdx struct {
//	Cidx    *IndexInfo
//	Benefit float64
//}
//
//type MultipleColIdx struct {
//}
//
//type SingleColIdx struct {
//}
//
//type candidateIndice struct {
//	benefits float64
//}
//
//func (ia *IdxAdvisor) addCandidateIndice(res *IdxAdvResult) {
//	c := buildCandidateIndice(res)
//}
//
//func buildCandidateIndice(idxInfo *model.IndexInfo) *candidateIndice {
//	idxLen := 0
//	for _, col := range idxInfo.Columns {
//		idxLen += col.Length
//
//	}
//}
//
//func (ia *IdxAdvisor) ExtractClause(sel *ast.SelectStmt) {
//}

func (ia *IdxAdvisor) GetReady() {
	ia.ready.Store(true)
}

func (ia *IdxAdvisor) IsReady() bool {
	if v, ok := ia.ready.Load().(bool); ok {
		return v
	}
	panic("IdxAdvisor.ready is not bool")
}

func NewIdxAdv(db *sql.DB) *IdxAdvisor {
	ia := &IdxAdvisor{dbClient: db}
	ia.ready.Store(false)

	idxadvPool.push(ia)
	return ia
}

// Init set session variable tidb_enable_index_advisor = true
// called by sqlClent
func (ia *IdxAdvisor) Init() error {
	_, err := ia.dbClient.Exec("SET tidb_enable_index_advisor = 1")
	if err == nil {
		ia.GetReady()
		return nil
	}
	return err
}

// called by TiDBServer
func RegisterIdxAdv(oldIS infoschema.InfoSchema, sessionID uint64) {
	ia, err := idxadvPool.pop()
	if err != nil {
		panic(err)
	}

	if _, ok := registeredIdxAdv[sessionID]; ok {
		panic("idxAdv session has already registered!")
	} else {
		registeredIdxAdv[sessionID] = ia
	}
}

func BuildAndGetVirtualInfoschema(stmtNode ast.StmtNode, is infoschema.InfoSchema, dbName string) infoschema.InfoSchema {
	// construct new DBInfos
	dbInfos := is.Clone()
	ISCopy := infoschema.MockInfoSchemaWithDBInfos(dbInfos, is.SchemaMetaVersion())
	dbname := model.NewCIStr(dbName)
	tblname := GetTableNameFromStmtNode(stmtNode)
	tblCopy, err := ISCopy.TableByName(dbname, tblname)
	tblInfoCopy := tblCopy.Meta()
	tbl, err := is.TableByName(dbname, tblname)
	tblInfo := tbl.Meta()
	fmt.Printf("**********tblname: %v  tbl.Indices: %v\n", tblname.String(), len(tblInfo.Indices))
	if err != nil {
		fmt.Printf("***BuildAndGetVirtualInfoschema, get tblInfoCopy from ISCopy error\n")
		panic(err)
	}
	virtualIndexs := BuildVirtualIndexes(stmtNode, tblInfoCopy, dbname, tblname)
	tblInfoCopy.Indices = append(tblInfoCopy.Indices, virtualIndexs...)
	fmt.Printf("******BuildAndGetVirtualInfoSchema: len(virtualIndexes): %v\n", len(virtualIndexs))
	fmt.Printf("******BuildAndGetVirtualInfoSchema: len(tblInfoCopy.Indices): %v\n", len(tblInfoCopy.Indices))
	fmt.Printf("******BuildAndGetVirtualInfoSchema: len(tblCopy.Meta().Indices): %v\n", len(tblCopy.Meta().Indices))

	return ISCopy
}

func BuildVirtualIndexes(stmtNode ast.StmtNode, tblInfo *model.TableInfo, dbname, tblname model.CIStr) []*model.IndexInfo {
	indexes := GenVirtualIndexCols(stmtNode, tblInfo, dbname, tblname)
	fmt.Printf("********BuildVirtualIndexes: len(GenVirtualIndexCols): %v\n", len(indexes))
	result := make([]*model.IndexInfo, 0)
	for i, idxColNames := range indexes {
		indexName := model.NewCIStr("vIndex" + string(i))
		indexinfo, err := ddl.BuildIndexInfo(tblInfo, indexName, idxColNames, model.StatePublic)
		if err != nil {
			fmt.Printf("***************BuildVirtualIndexes error: %v!\n", err)
			panic(err)
		}
		result = append(result, indexinfo)
	}
	return result
}

func GenVirtualIndexCols(stmtNode ast.StmtNode, tblInfo *model.TableInfo, dbname, tblname model.CIStr) [][]*ast.IndexColName {
	columnInfos := tblInfo.Columns
	var result [][]*ast.IndexColName
	for _, columnInfo := range columnInfos {
		idxCols := make([]*ast.IndexColName, 1, 1)
		idxCols[0] = BuildIdxColNameFromColInfo(columnInfo, dbname, tblname)
		if !IndexesHasAlreadyExist(idxCols, tblInfo.Indices) {
			result = append(result, idxCols)
		}
	}
	return result
}

func BuildIdxColNameFromColInfo(colInfo *model.ColumnInfo, dbname, tblname model.CIStr) *ast.IndexColName {
	idxColName := &ast.IndexColName{}
	idxColName.Column = &ast.ColumnName{Schema: dbname, Table: tblname, Name: colInfo.Name}
	idxColName.Length = -1
	return idxColName
}

// TODO: better way to extract tableName from stmtNode.
// current method requires target table is on the left side of join
func GetTableNameFromStmtNode(stmtNode ast.StmtNode) model.CIStr {
	return stmtNode.(*ast.SelectStmt).From.TableRefs.Left.(*ast.TableSource).Source.(*ast.TableName).Name
}

// TODO: This is only single col index recomendation
func IndexesHasAlreadyExist(idxCols []*ast.IndexColName, indices []*model.IndexInfo) bool {
	primaryKey := findPrimaryKey(indices)
	if primaryKey == nil {
		return false
	}
	return primaryKey.Columns[0].Name.String() == idxCols[0].Column.Name.String()
}

func findPrimaryKey(indices []*model.IndexInfo) *model.IndexInfo {
	if len(indices) == 0 {
		return nil
	}
	for _, indexInfo := range indices {
		if indexInfo.Primary {
			return indexInfo
		}
	}
	return nil
}

//func BuildAndGetIdxAdvCtx(sessionID uint64, stmtNode ast.StmtNode, is infoschema.InfoSchema) *IdxAdvContext {
//	if ia, ok := registeredIdxAdv[sessionID]; ok {
//		fmt.Printf("********BuildAndGetIdxAdvCtx: has already idxadv has registered\n")
//		ia.BuildIdxAdvCtx(stmtNode)
//		return ia.IdxAdvCtx
//	} else {
//		fmt.Printf("*********BuildAndGetIdxAdvCtx: not registered, register\n")
//		RegisterIdxAdv(is, sessionID)
//		if ia, ok := registeredIdxAdv[sessionID]; ok {
//			ia.BuildIdxAdvCtx()
//		} else {
//			panic("RegisterIdxAdv doesn't work!")
//		}
//	}
//}
//
//func (ia *IdxAdvisor) BuildIdxAdvCtx(stmtNode ast.StmtNode) {
//	if _, ok := stmtNode.(*ast.SelectStmt); ok {
//		ia.IdxAdvCtx.OriginalCost = 2.00
//	}
//}

// called by dbClient
func (ia *IdxAdvisor) StartTask(query string) {
	if ia.IsReady() {
		fmt.Printf("********idxadvisor/outline.go: Set variable has done, StartTask starts query\n")
		var err error
		_, err = ia.dbClient.Exec(query)
		for i := 0; i < 10; i++ {
			if err == nil {
				_, err = ia.dbClient.Exec(query)
				fmt.Printf("[%v]", i)
			} else {
				fmt.Printf("**********query execution error: %v\n", err)
				panic(err)
			}
		}
	}
}

func newInfoSchemaCopy(oldIS infoschema.InfoSchema) infoschema.InfoSchema {
	isVersion := oldIS.SchemaMetaVersion()
	oldDBInfos := oldIS.Clone()

	ISCopy := infoschema.MockInfoSchemaWithDBInfos(oldDBInfos, isVersion)

	return ISCopy
}
