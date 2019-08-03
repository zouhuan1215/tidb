package idxadvisor

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
)

var defaultDSNConfig = mysql.Config{
	User:   "root",
	Net:    "tcp",
	Addr:   "127.0.0.1:4000",
	DBName: "test",
	Strict: true,
}

const retryTime = 100

// statusPort is tidb server's status port
// TODO: statusPort should be sysVars, get it from global vars
const statusPort uint = 10080

// IdxAdvCmd and IdxCmdIden is used to distinguish if a queryis in IndexAdvisor mode
// e.g. Using IndexAdivisor Query: "using idvisor#select * from t"
const IdxAdvCmdIden = "#"
const IdxAdvCmd = "using idxadv"

type configOverrider func(*mysql.Config)

// getDSN generates a DSN string for MySQL connection.
func getDSN(overriders ...configOverrider) string {
	var config = defaultDSNConfig
	for _, overrider := range overriders {
		if overrider != nil {
			overrider(&config)
		}
	}
	return config.FormatDSN()
}

// runSqlClient runs an index advisor client using the default database 'test'.
func RunSqlClient(query string) error {
	fmt.Println("*******************************************")
	fmt.Printf("RunSqlClient\n")
	fmt.Println("*******************************************")
	waitUntilServerOnline(statusPort)
	var defMySQLConfig configOverrider
	return runSqlClient(defMySQLConfig, query)
}

func waitUntilServerOnline(statusPort uint) {
	// connect server
	retry := 0
	for ; retry < retryTime; retry++ {
		time.Sleep(time.Millisecond * 10)
		db, err := sql.Open("mysql", getDSN())
		if err == nil {
			db.Close()
			break
		}
	}

	if retry == retryTime {
		fmt.Printf("failed to connect DB in every 10ms\n")
	}

	// connect http status
	statusURL := fmt.Sprintf("http://127.0.0.1:%d/status", statusPort)
	for retry = 0; retry < retryTime; retry++ {
		resp, err := http.Get(statusURL)
		if err == nil {
			ioutil.ReadAll(resp.Body)
			resp.Body.Close()
			break
		}
		time.Sleep(time.Millisecond * 10)
	}
	if retry == retryTime {
		fmt.Printf("failed to connect HTTP status in every 10 ms")
	}

}

func runSqlClient(overrider configOverrider, query string) error {
	db, err := sql.Open("mysql", getDSN(overrider))
	defer db.Close()
	if err != nil {
		return err
	} else {
		db.Exec("SET tidb_enable_index_advisor = 1")
		for i := 0; i < 10; i++ {
			if i == 2 {
				db.Exec("SET tidb_enable_index_advisor = 0")
			}
			_, err := db.Exec(query)
			if err != nil {
				db.Exec(query)
			}
		}

	}
	return nil
}

func InIdxAvisorMode(query string) (string, bool) {
	uints := strings.Split(query, IdxAdvCmdIden)
	if strings.EqualFold(uints[0], IdxAdvCmd) {
		return uints[1], true
	} else {
		return query, false
	}
}
