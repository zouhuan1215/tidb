package idxadvisor

import (
	"database/sql"
	"errors"
	"log"
	"sync/atomic"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/domain"
	s "github.com/pingcap/tidb/session"

	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/store/mockstore"
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
	dbClient  *sql.DB
	IdxAdvCtx *IdxAdvContext

	ready atomic.Value // initialized(1) or not(0)
	//	Candidate_idx  []*CandidateIdx
}

type IdxAdvContext struct {
	VirtualIS    infoschema.InfoSchema
	TableName    model.CIStr
	OriginalCost float64
	VirtualCost  float64
	SelectedIdx  []*model.IndexInfo
}

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
//	idxCols  []*model.IndexColumn
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
	return err
}

// called by TiDBServer
func RegisterIdxAdv(session s.Session) {
	ia, err := idxadvPool.pop()
	if err != nil {
		panic(err)
	}
	dom := domain.GetDomain(session)
	oldIS := dom.InfoSchema()
	ia.IdxAdvCtx = &IdxAdvContext{VirtualIS: newInfoSchemaCopy(oldIS)}

	sessionID := session.GetSessionVars().ConnectionID
	if _, ok := registeredIdxAdv[sessionID]; ok {
		panic("idxAdv session has already registered!")
	} else {
		registeredIdxAdv[sessionID] = ia
	}
	ia.ready.Store(true)
}

func BuildAndGetIdxAdvCtx(sessionID uint64, stmtNode ast.StmtNode) *IdxAdvContext {
	if ia, ok := registeredIdxAdv[sessionID]; ok {
		ia.BuildIdxAdvCtx(stmtNode)
		return ia.IdxAdvCtx
	}
	panic("IdxAdv with this id hasn't been registered yet")
}

// called by dbClient
func (ia *IdxAdvisor) StartTask(query string) {
	if ia.IsReady() {
		_, err := ia.dbClient.Exec(query)
		for i := 0; i < 10; i++ {
			if err != nil && ia.IsReady() {
				ia.dbClient.Exec(query)
			}
		}
	}

}

func newInfoSchemaCopy(oldIS infoschema.InfoSchema) infoschema.InfoSchema {
	isVersion := oldIS.SchemaMetaVersion()
	oldDBInfos := oldIS.Clone()

	store, err := mockstore.NewMockTikvStore()
	if err != nil {
		log.Fatalf("mockstore error: %v", err)
		panic("idxAdvisor newInfoSchemaCopy Error!")
	}
	handle := infoschema.NewHandle(store)
	builder, err := infoschema.NewBuilder(handle).InitWithDBInfos(oldDBInfos, isVersion)
	builder.Build()

	return handle.Get()
}
