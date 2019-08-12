package idxadvisor

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"

	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/printer"
	"go.uber.org/zap"
)

const retryTime = 100

// RunIdxAdvisor runs an index advisor client
func RunIdxAdvisor(sqlFile string, loginInfo string, serverStatPort string, outputPath string) error {
	printer.PrintIdxAdvisorInfo()
	logutil.BgLogger().Info(fmt.Sprintf("[mysql client login info]: %v\n[query file]: %v\n", loginInfo, sqlFile))
	statusPort, err := strconv.ParseUint(serverStatPort, 10, 64)
	if err != nil {
		logutil.BgLogger().Error("failed to convert string type to uint type",
			zap.String("Got server's status port", serverStatPort))
	}
	waitUntilServerOnline(uint(statusPort), loginInfo)

	err = runIdxAdvisor(sqlFile, loginInfo, outputPath)
	if err != nil {
		logutil.BgLogger().Error("failed initializing index advisor failed",
			zap.Error(err))
		return err
	}

	return nil
}

func waitUntilServerOnline(statusPort uint, loginInfo string) {
	// connect server
	retry := 0
	for ; retry < retryTime; retry++ {
		time.Sleep(time.Millisecond * 10)
		db, err := sql.Open("mysql", loginInfo)
		if err == nil {
			db.Close()
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
			ioutil.ReadAll(resp.Body)
			resp.Body.Close()
			break
		}
		time.Sleep(time.Millisecond * 10)
	}
	if retry == retryTime {
		logutil.BgLogger().Warn("failed to connect HTTP status in every 10 ms")
	}
}

func runIdxAdvisor(sqlFile string, loginInfo string, outputPath string) error {
	db, err := sql.Open("mysql", loginInfo)
	defer db.Close()
	if err != nil {
		return err
	} else {
		ia := NewIdxAdv(db, sqlFile, outputPath)
		err := ia.Init()
		if err != nil {
			return err
		}

		logutil.BgLogger().Info("[Index Advisor] start evaluating queries")
		return ia.StartTask()
	}
	return nil
}
