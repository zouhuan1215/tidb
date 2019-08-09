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
	queryChan chan string

	ready         atomic.Value
	Candidate_idx []*CandidateIdx
}

// CandidateIdx includes in index and its benefit.
type CandidateIdx struct {
	Index   *model.IndexInfo
	Benefit float64
}

// NewIdxAdv create a new IdxAdvisor.
func NewIdxAdv(db *sql.DB) *IdxAdvisor {
	ia := &IdxAdvisor{dbClient: db}
	ia.ready.Store(false)

	idxadvPool.push(ia)
	return ia
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

		for {
			query, ok := <-ia.queryChan
			if !ok {
				// No more query
				return
			}
			fmt.Printf("*********************************************************************************\n")
			fmt.Printf("%v\n", query)
			fmt.Printf("================================Next Query ==================================\n")
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
	for i := 1; i <= 22; i++ {
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

func GetVirtualInfoschema(is infoschema.InfoSchema, dbName, tblName string) infoschema.InfoSchema {
	// Get a copy of InfoSchema
	dbInfos := is.Clone()
	ISCopy := infoschema.MockInfoSchemaWithDBInfos(dbInfos, is.SchemaMetaVersion())

	dbname := model.NewCIStr(dbName)
	tblname := model.NewCIStr(tblName)
	tblCopy, err := ISCopy.TableByName(dbname, tblname)
	if err != nil {
		panic(err)
	}
	tblInfoCopy := tblCopy.Meta()

	// add virtual indexes to InfoSchemaCopy.TblInfo
	virtualIndexes := BuildVirtualIndexes(tblInfoCopy, dbname, tblname)
	tblInfoCopy.Indices = append(tblInfoCopy.Indices, virtualIndexes...)
	return ISCopy
}

func BuildVirtualIndexes(tblInfo *model.TableInfo, dbname, tblname model.CIStr) []*model.IndexInfo {
	indexes := GenVirtualIndexCols(tblInfo, dbname, tblname)
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

func GenVirtualIndexCols(tblInfo *model.TableInfo, dbname, tblname model.CIStr) [][]*ast.IndexColName {
	columnInfos := tblInfo.Columns
	var result [][]*ast.IndexColName
	for _, columnInfo := range columnInfos {
		idxCols := make([]*ast.IndexColName, 1, 1)
		idxCols[0] = BuildIdxColNameFromColInfo(columnInfo, dbname, tblname)
		if !IndexesHasAlreadyExist(idxCols, tblInfo.Indices) {
			result = append(result, idxCols)
		}
	}

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

	return result
}

func BuildIdxColNameFromColInfo(colInfo *model.ColumnInfo, dbname, tblname model.CIStr) *ast.IndexColName {
	idxColName := &ast.IndexColName{}
	idxColName.Column = &ast.ColumnName{Schema: dbname, Table: tblname, Name: colInfo.Name}
	idxColName.Length = -1
	return idxColName
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

func (ia *IdxAdvisor) addCandidate(virtualIdx *CandidateIdx) {
	in := false
	for _, candidateIdx := range ia.Candidate_idx {
		if reflect.DeepEqual(candidateIdx.Index.Columns, virtualIdx.Index.Columns) {
			candidateIdx.Benefit += virtualIdx.Benefit
			in = true
			break
		}
	}

	if !in {
		ia.Candidate_idx = append(ia.Candidate_idx, virtualIdx)
	}
}
