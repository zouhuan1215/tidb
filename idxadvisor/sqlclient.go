package idxadvisor

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
)

// statusPort is tidb server's status port
// TODO: statusPort should be sysVars, get it from global vars
const statusPort uint = 10080
const user string = "root"
const address string = "127.0.0.1:4000"
const dbname string = "test"

const retryTime = 100

var defaultDSNConfig = mysql.Config{
	User:   user,
	Net:    "tcp",
	Addr:   address,
	DBName: dbname,
	Strict: true,
}

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

// runSqlClient runs an index advisor client
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
		ia := NewIdxAdv(db)
		err := ia.Init()
		if err != nil {
			return err
		}

		fmt.Printf("===============ia.StartTask(query)===========\n")
		ia.StartTask(query)

	}
	return nil

}

func readQuery(sqlFile *string) []string {
	queries := make([]string, 0, 0)
	for i := 1; i <= 22; i++ {
		sqlfile := *sqlFile + "/" + strconv.Itoa(i) + ".sql"
		contents, err := ioutil.ReadFile(sqlfile)
		if err != nil {
			panic(err)
		}
		sqlBegin := strings.Index(string(contents), "select")
		query := contents[sqlBegin:]
		queries = append(queries, string(query))
	}
	return queries
}
