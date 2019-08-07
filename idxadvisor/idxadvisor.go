package idxadvisor

import (
	"database/sql"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/pingcap/parser/model"
)

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

type IdxAdvisor struct {
	dbClient *sql.DB

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
		//		var err error
		sqlFile := "/tmp/queries"
		queries := readQuery(&sqlFile)
		fmt.Printf("==============len(queries):%v\n", len(queries))
		//	for i, query := range queries {
		//		fmt.Printf("$$$$$$$$$$$$$$$$$$$$$$[%v]$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$\n", i+1)
		//		//	query = "select * from orders limit 10000;"
		//		_, err := ia.dbClient.Exec(query)
		//		_, err1 := ia.dbClient.Exec(query)
		//		if err != nil || err1 != nil {
		//			fmt.Printf("**********query execution error: %s***********\n", err.Error())
		//			panic(err)
		//		}
		//	}

		for i := 21; i >= 0; i-- {
			query := queries[i]
			fmt.Printf("$$$$$$$$$$$$$$$$$$$$$$[%v]$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$\n", i+1)
			//	query = "select * from orders limit 10000;"
			_, err := ia.dbClient.Exec(query)
			_, err1 := ia.dbClient.Exec(query)
			if err != nil || err1 != nil {
				fmt.Printf("**********query execution error: %s***********\n", err.Error())
				panic(err)
			}
		}

	}
}
