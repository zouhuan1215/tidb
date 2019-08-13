package idxadvisor

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/printer"
	"go.uber.org/zap"
)

const retryTime = 100

type configOverrider func(*mysql.Config)

// RunIdxAdvisor runs an index advisor client
func RunIdxAdvisor(sqlFile, serverStatPort, outputPath, user, addr, pwd, dbname string) bool {
	printer.PrintIdxAdvisorInfo()
	dsnConfig := buildDSNConfig(user, addr, pwd, dbname)
	statusPort, err := strconv.ParseUint(serverStatPort, 10, 64)
	if err != nil {
		logutil.BgLogger().Error("failed to convert string type to uint type",
			zap.String("Got server's status port", serverStatPort))
		return false
	}
	waitUntilServerOnline(uint(statusPort), dsnConfig)

	err = runIdxAdvisor(sqlFile, dsnConfig, outputPath)
	if err != nil {
		logutil.BgLogger().Error("failed initializing index advisor failed",
			zap.Error(err))
		return false
	}

	return true
}

func waitUntilServerOnline(statusPort uint, dsnConfig mysql.Config) {
	// connect server
	retry := 0
	for ; retry < retryTime; retry++ {
		time.Sleep(time.Millisecond * 10)
		db, err := sql.Open("mysql", getDSN(dsnConfig))
		if err == nil {
			err = db.Close()
			if err != nil {
				logutil.BgLogger().Error("close mysql db error", zap.Error(err))
			}
			break
		}
	}

	if retry == retryTime {
		logutil.BgLogger().Warn("failed to connect DB in every 10ms\n")
	}

	// connect http status
	statusURL := fmt.Sprintf("http://127.0.0.1:%d/status", statusPort)
	for retry = 0; retry < retryTime; retry++ {
		resp, err := http.Get(statusURL)
		if err == nil {
			_, err = ioutil.ReadAll(resp.Body)
			if err != nil {
				logutil.BgLogger().Warn("resp read body failed", zap.Error(err))
			}
			err = resp.Body.Close()
			if err != nil {
				logutil.BgLogger().Error("http close connection error", zap.Error(err))
			}
			break
		}
		time.Sleep(time.Millisecond * 10)
	}
	if retry == retryTime {
		logutil.BgLogger().Warn("failed to connect HTTP status in every 10 ms")
	}
}

func runIdxAdvisor(sqlFile string, dsnConfig mysql.Config, outputPath string) error {
	var defMySQLConfig configOverrider
	db, err := sql.Open("mysql", getDSN(dsnConfig, defMySQLConfig))
	defer func() {
		err := db.Close()
		if err != nil {
			logutil.BgLogger().Error("sql db.Close() error", zap.Error(err))
		}
	}()
	if err != nil {
		return err
	}
	ia := NewIdxAdv(db, sqlFile, outputPath)
	err = ia.Init()
	if err != nil {
		return err
	}

	logutil.BgLogger().Info("[Index Advisor] start evaluating queries")
	return ia.StartTask()
}

func buildDSNConfig(user, addr, pwd, dbname string) mysql.Config {
	return mysql.Config{
		User:   user,
		Net:    "tcp",
		Addr:   addr,
		DBName: dbname,
		Strict: true,
	}
}

func getDSN(config mysql.Config, overriders ...configOverrider) string {
	for _, overrider := range overriders {
		if overrider != nil {
			overrider(&config)
		}
	}

	return config.FormatDSN()
}
