package idxadvisor

import (
	"database/sql"
	"errors"
	"fmt"
	"io/ioutil"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/infoschema"
	plannercore "github.com/pingcap/tidb/planner/core"
)

type idxAdvPool []*IdxAdvisor

const queryChanSize int = 10000

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

type IdxAdvisor struct {
	dbClient  *sql.DB
	queryChan chan string // queryChan transfer query read from file
	queryCnt  uint64      // record how many queries have been evaluated in current session

	ready         atomic.Value
	Candidate_idx CandidateIdxes
}

// CandidateIdx includes in index and its benefit.
type CandidateIdx struct {
	Index   *IdxAndTblInfo
	Benefit float64
}

// CandidateIdxes implements sort.Sort() interface
type CandidateIdxes []*CandidateIdx

func (ci CandidateIdxes) Len() int { return len(ci) }

func (ci CandidateIdxes) Less(i, j int) bool { return ci[i].Benefit > ci[j].Benefit }

func (ci CandidateIdxes) Swap(i, j int) { ci[i], ci[j] = ci[j], ci[i] }

// NewIdxAdv create a new IdxAdvisor.
func NewIdxAdv(db *sql.DB) *IdxAdvisor {
	ia := &IdxAdvisor{dbClient: db}
	ia.ready.Store(false)

	idxadvPool.push(ia)
	return ia
}

func GetIdxAdv(connID uint64) *IdxAdvisor {
	if ia, ok := registeredIdxAdv[connID]; ok {
		return ia
	}

	if ia, err := idxadvPool.pop(); err != nil {
		panic(err)
	} else {
		registeredIdxAdv[connID] = ia
		return ia
	}
}

// Init set session variable tidb_enable_index_advisor = true
func (ia *IdxAdvisor) Init() error {
	ia.queryChan = make(chan string, queryChanSize)
	_, err := ia.dbClient.Exec("SET tidb_enable_index_advisor = 1")
	if err == nil {
		ia.GetReady()
		return nil
	}
	return err
}

func (ia *IdxAdvisor) GetReady() {
	ia.ready.Store(true)
}

func (ia *IdxAdvisor) IsReady() bool {
	if v, ok := ia.ready.Load().(bool); ok {
		return v
	}
	panic("IdxAdvisor.ready is not bool")
}

// StartTask start handling queries in idxadv mode after session variable tidb_enable_index_advisor has been set
func (ia *IdxAdvisor) StartTask(query string) {
	if ia.IsReady() {
		sqlFile := "/tmp/queries/"
		go readQuery(&sqlFile, ia.queryChan)

		cnt := 0
		for {
			cnt++
			query, ok := <-ia.queryChan
			if !ok {
				// No more query
				WriteFinaleResult()
				return
			}
			fmt.Printf("**************************************[%v]******************************************\n", cnt)
			ia.dbClient.Exec(query)
		}
	}
}

//func readQuery(sqlFile *string, queryChan chan string) {
//	fd, _ := os.Open(*sqlFile)
//	defer func() {
//		fd.Close()
//		close(queryChan)
//	}()
//
//	scanner := bufio.NewScanner(fd)
//
//	// TODO: more efficient way to extract select statement from file
//	maxCap := bufio.MaxScanTokenSize
//	buf := make([]byte, maxCap)
//	scanner.Buffer(buf, maxCap)
//	split := func(data []byte, atEOF bool) (advance int, token []byte, err error) {
//		// Define a split function that separates on "--"
//		for i := 0; i < len(data)-1; i++ {
//			if data[i] == 0x2d && data[i+1] == 0x2d {
//				return i + 2, data[:i], nil
//
//			}
//
//		}
//		return 0, data, bufio.ErrFinalToken
//	}
//	scanner.Split(split)
//
//	// Scan
//	cnt := 1
//	for scanner.Scan() {
//		contents := scanner.Text()
//		//	fmt.Printf("================================[%v]==================================\n", cnt)
//		//	fmt.Printf("%v\n", contents)
//		sqlBegin := strings.Index(string(contents), "select")
//		query := contents[sqlBegin : len(contents)-1]
//		queryChan <- query
//		cnt++
//	}
//
//	if err := scanner.Err(); err != nil {
//		fmt.Fprintln(os.Stderr, "reading input:", err)
//	}
//}

func readQuery(sqlFile *string, queryChan chan string) {
	defer func() {
		close(queryChan)
	}()
	for i := 1; i <= 7; i++ {
		sqlfile := *sqlFile + strconv.Itoa(i) + ".sql"
		contents, err := ioutil.ReadFile(sqlfile)
		if err != nil {
			panic(err)

		}
		sqlBegin := strings.Index(string(contents), "select")
		query := contents[sqlBegin:]
		queryChan <- string(query)
	}
}

/*
// StartTask start handling queries in idxadv mode after session variable tidb_enable_index_advisor has been set
func (ia *IdxAdvisor) StartTask(query string) {
	if ia.IsReady() {
		//		var err error
		sqlFile := "/tmp/queries"
		queries := readQuery(&sqlFile)
		for i, query := range queries {
			fmt.Printf("$$$$$$$$$$$$$$$$$$$$$$[%v]$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$\n", i+1)
			ia.dbClient.Exec(query)
		}
	}
}
*/
func GetVirtualInfoschema(is infoschema.InfoSchema, dbName string, tableInfoSets map[string]*plannercore.TableInfoSets) infoschema.InfoSchema {
	// Get a copy of InfoSchema
	dbInfos := is.Clone()
	ISCopy := infoschema.MockInfoSchemaWithDBInfos(dbInfos, is.SchemaMetaVersion())

	dbname := model.NewCIStr(dbName)
	for tblname, tblInfoSets := range tableInfoSets {
		tblname := model.NewCIStr(tblname)
		tblCopy, err := ISCopy.TableByName(dbname, tblname)
		if err != nil {
			panic(err)
		}
		tblInfoCopy := tblCopy.Meta()
		idxInfo := tblCopy.Meta().Indices

		// add virtual indexes to InfoSchemaCopy.TblInfo
		virtualIndexes := BuildVirtualIndexes(tblInfoCopy, dbname, tblname, tblInfoSets)
		for _, virtualIndex := range virtualIndexes {
			if !isExistedInTable(virtualIndex, idxInfo) {
				tblInfoCopy.Indices = append(tblInfoCopy.Indices, virtualIndex)
			}
		}
	}
	return ISCopy
}

func BuildVirtualIndexes(tblInfo *model.TableInfo, dbname, tblname model.CIStr, tblInfoSets *plannercore.TableInfoSets) []*model.IndexInfo {
	indexes := GenVirtualIndexCols(tblInfo, dbname, tblname, tblInfoSets)
	result := make([]*model.IndexInfo, 0)
	for i, idxColNames := range indexes {
		indexName := model.NewCIStr("vIndex" + string(i))
		indexinfo, err := ddl.BuildIndexInfo(tblInfo, indexName, idxColNames, model.StatePublic)
		if err != nil {
			fmt.Printf("BuildVirtualIndexes error: %v!\n", err)
			var idxColNameStr string
			for _, idxCol := range idxColNames {
				idxColNameStr = fmt.Sprintf("%v  %v", idxColNameStr, idxCol)
			}
			fmt.Printf("++++++++++++++++++idxColNames: %v, indexName: %v++++++++++++++++++++++\n", idxColNames, idxColNameStr)
			panic(err)
		}
		result = append(result, indexinfo)
	}
	return result
}

func GenVirtualIndexCols(tblInfo *model.TableInfo, dbname, tblname model.CIStr, tblInfoSets *plannercore.TableInfoSets) [][]*ast.IndexColName {
	columnInfos := tblInfo.Columns
	var result [][]*ast.IndexColName

	// one column
	for _, columnInfo := range columnInfos {
		idxCols := make([]*ast.IndexColName, 1, 1)
		idxCols[0] = BuildIdxColNameFromColInfo(columnInfo, dbname, tblname)
		result = append(result, idxCols)
	}

	// two columns
	nCols := len(columnInfos)
	for i := 0; i < nCols; i++ {
		for j := 0; j < nCols; j++ {
			if i != j {
				idxTwoCols := make([]*ast.IndexColName, 2, 2)
				idxTwoCols[0] = BuildIdxColNameFromColInfo(columnInfos[i], dbname, tblname)
				idxTwoCols[1] = BuildIdxColNameFromColInfo(columnInfos[j], dbname, tblname)
				result = append(result, idxTwoCols)
			}
		}
	}

	// multi columns
	candidateCols := [][]model.CIStr{}
	eq := tblInfoSets.Eq
	o := tblInfoSets.O
	rg := tblInfoSets.Rg
	ref := tblInfoSets.Ref

	// EQ + O + RANGE + REF
	cols := [][]model.CIStr{}
	for i, oCols := range o {
		cols = append(cols, []model.CIStr{})
		addToCandidateCols(eq, &cols[i], &candidateCols)
		addToCandidateCols(oCols, &cols[i], &candidateCols)
		addToCandidateCols(rg, &cols[i], &candidateCols)
		addToCandidateCols(ref, &cols[i], &candidateCols)
	}
	if len(cols) == 0 {
		cols = append(cols, []model.CIStr{})
		addToCandidateCols(eq, &cols[0], &candidateCols)
		addToCandidateCols(rg, &cols[0], &candidateCols)
		addToCandidateCols(ref, &cols[0], &candidateCols)
	}

	// O + EQ + RANGE + REF
	cols = cols[:0]
	for i, oCols := range o {
		cols = append(cols, []model.CIStr{})
		addToCandidateCols(oCols, &cols[i], &candidateCols)
		addToCandidateCols(eq, &cols[i], &candidateCols)
		addToCandidateCols(rg, &cols[i], &candidateCols)
		addToCandidateCols(ref, &cols[i], &candidateCols)
	}
	if len(cols) == 0 {
		cols = append(cols, []model.CIStr{})
		addToCandidateCols(eq, &cols[0], &candidateCols)
		addToCandidateCols(rg, &cols[0], &candidateCols)
		addToCandidateCols(ref, &cols[0], &candidateCols)
	}

	candidateCols = plannercore.RemoveRepeatedColumnSet(candidateCols)
	if len(candidateCols) > 0 {
		fmt.Printf("table %s multi candidate index: ", tblname)
		fmt.Println(candidateCols)
	}
	for _, candidateColumns := range candidateCols {
		idxCols := []*ast.IndexColName{}
		for _, column := range candidateColumns {
			columnInfo := new(model.ColumnInfo)
			isExisted := false
			for _, tmpColumn := range columnInfos {
				if tmpColumn.Name.L == column.L {
					columnInfo = tmpColumn
					isExisted = true
					break
				}
			}
			if isExisted {
				idxCols = append(idxCols, BuildIdxColNameFromColInfo(columnInfo, dbname, tblname))
			}
		}
		result = append(result, idxCols)
	}

	return result
}

func addToCandidateCols(readyCols []model.CIStr, cols *[]model.CIStr, candidateCols *[][]model.CIStr) {
	if len(readyCols) == 0 {
		return
	}

	*cols = append(*cols, readyCols...)
	*cols = plannercore.RemoveRepeatedColumn(*cols)
	if len(*cols) > 2 {
		*candidateCols = append(*candidateCols, *cols)
	}
}

func BuildIdxColNameFromColInfo(colInfo *model.ColumnInfo, dbname, tblname model.CIStr) *ast.IndexColName {
	idxColName := &ast.IndexColName{}
	idxColName.Column = &ast.ColumnName{Schema: dbname, Table: tblname, Name: colInfo.Name}
	idxColName.Length = -1
	return idxColName
}

func GenIndexCols(index *model.IndexInfo) []model.CIStr {
	cols := []model.CIStr{}
	for _, idxColumn := range index.Columns {
		cols = append(cols, idxColumn.Name)
	}
	return cols
}

func isExistedInTable(virtualIndex *model.IndexInfo, indices []*model.IndexInfo) bool {
	is := false
	virtualIndexCols := GenIndexCols(virtualIndex)
	for _, idx := range indices {
		indexCols := GenIndexCols(idx)
		if reflect.DeepEqual(virtualIndexCols, indexCols) {
			is = true
			break
		}
	}
	return is
}

func (ia *IdxAdvisor) addCandidate(virtualIdx *CandidateIdx) {
	in := false
	for _, candidateIdx := range ia.Candidate_idx {
		if reflect.DeepEqual(candidateIdx.Index.Index.Columns, virtualIdx.Index.Index.Columns) && reflect.DeepEqual(candidateIdx.Index.Table.Name, virtualIdx.Index.Table.Name) {
			candidateIdx.Benefit += virtualIdx.Benefit
			in = true
			break
		}
	}

	if !in {
		ia.Candidate_idx = append(ia.Candidate_idx, virtualIdx)
	}
}
