package idxadvisor_test

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"testing"

	"github.com/juju/errors"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/idxadvisor"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&testAnalyzeSuite{})

type testAnalyzeSuite struct {
}

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

func (s *testAnalyzeSuite) loadTableStats(fileName string, dom *domain.Domain) error {
	statsPath := filepath.Join("testdata", fileName)
	bytes, err := ioutil.ReadFile(statsPath)

	if err != nil {
		return err
	}

	statsTbl := &handle.JSONTable{}
	err = json.Unmarshal(bytes, statsTbl)

	if err != nil {
		return err
	}

	statsHandle := dom.StatsHandle()
	err = statsHandle.LoadStatsFromJSON(dom.InfoSchema(), statsTbl)

	if err != nil {
		return err
	}

	return nil
}

func (s *testAnalyzeSuite) TestSQLClient(c *C) {
	err := idxadvisor.RunSqlClient("test-mode")
	c.Assert(err, IsNil)
}

func (s *testAnalyzeSuite) TestIndexAdvisor(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)

	testkit := testkit.NewTestKit(c, store)
	idxadvisor.MockNewIdxAdv()
	defer func() {
		dom.Close()
		store.Close()
	}()

	testkit.MustExec("use test")
	testkit.MustExec("drop table if exists t, t1")
	testkit.MustExec("create table t (a int primary key, b int, c varchar(200), d datetime DEFAULT CURRENT_TIMESTAMP, e int, ts timestamp DEFAULT CURRENT_TIMESTAMP)")
	testkit.MustExec("create table t1 (a int, b int, c int, d int)")

	err = s.loadTableStats("analyzesSuiteTestIndexReadT.json", dom)
	c.Assert(err, IsNil)

	for i := 1; i < 8; i++ {
		testkit.MustExec(fmt.Sprintf("insert into t1 values(%v, %v, %v, %v)", i, i, 1, 1))
	}
	for i := 8; i < 16; i++ {
		testkit.MustExec(fmt.Sprintf("insert into t1 values(%v, %v, %v, %v)", i, i, 2, 2))
	}
	testkit.MustExec("analyze table t1")

	ctx := testkit.Se.(sessionctx.Context)
	sessionVars := ctx.GetSessionVars()
	sessionVars.HashAggFinalConcurrency = 1
	sessionVars.HashAggPartialConcurrency = 1
	connID := sessionVars.ConnectionID

	tests := []struct {
		sql []string
		res string
	}{
		{
			sql: []string{
				"select count(*) from t group by e",
				"select a, b from t1 where c in (1,3)",
			},
			res: "t: (e),t1: (c a b)",
		},
		{
			sql: []string{
				"select c, d, count(*) from t1 group by c, d",
				"select * from t where b in (select c from t1 where c>0)",
			},
			res: "t: (e),t1: (c a b),t1: (c d),t1: (c),t: (b)",
		},
		{
			sql: []string{
				"select a from t1 order by b desc",
				"select t.a from t join t1 on t.b = t1.b",
			},
			res: "t: (e),t1: (c a b),t1: (c d),t1: (c),t: (b),t1: (b a),t1: (b)",
		},
	}

	testkit.MustExec("set tidb_enable_index_advisor=1")
	for _, tt := range tests {
		for _, sql := range tt.sql {
			testkit.Exec(sql)
		}
		res, err := idxadvisor.GetRecommendIdxStr(connID)
		c.Assert(err, IsNil)
		c.Assert(res, Equals, tt.res, Commentf("for %v", tt.sql))
	}
}

func newStoreWithBootstrap() (kv.Storage, *domain.Domain, error) {
	store, err := mockstore.NewMockTikvStore()
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	session.SetSchemaLease(0)
	session.DisableStats4Test()

	dom, err := session.BootstrapSession(store)
	if err != nil {
		return nil, nil, err
	}

	dom.SetStatsUpdating(true)
	return store, dom, errors.Trace(err)
}
