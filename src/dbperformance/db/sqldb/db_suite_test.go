package sqldb_test

import (
	"code.cloudfoundry.org/lager"
	"dbperformance/db/sqldb"
	"dbperformance/models"
	"fmt"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"os"

	"testing"
)

var sdb *sqldb.ScalingEngineSQLDB
var (
	appId     string = "theAppId"
	reason    string = "this is reason"
	message   string = "this is message"
	errorMsg  string = "error msg"
	timestamp int64  = 1
	logger    lager.Logger
)

func TestDb(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Db Suite")
}

var _ = BeforeSuite(func() {
	var e error

	dbUrl := os.Getenv("DBURL")
	if dbUrl == "" {
		Fail("environment variable $DBURL is not set")
	}
	logger = lager.NewLogger("history-test")
	sdb, e = sqldb.NewScalingEngineDB(dbUrl, logger)
	if e != nil {
		Fail("can not connect to database" + e.Error())
	}
	// addScalingHistories()
})

var _ = AfterSuite(func() {
	if sdb != nil {
		sdb.Close()

	}
})

func addScalingHistories() {
	var historyList []*models.AppScalingHistory = []*models.AppScalingHistory{}
	failHistory := &models.AppScalingHistory{
		AppId:        appId,
		Timestamp:    timestamp,
		ScalingType:  models.ScalingType(0),
		Status:       models.ScalingStatus(1),
		OldInstances: 0,
		NewInstances: 1,
		Reason:       reason,
		Message:      message,
		Error:        errorMsg,
	}
	succeedHistory := &models.AppScalingHistory{
		AppId:        appId,
		Timestamp:    timestamp,
		ScalingType:  models.ScalingType(0),
		Status:       models.ScalingStatus(0),
		OldInstances: 0,
		NewInstances: 1,
		Reason:       reason,
		Message:      message,
		Error:        errorMsg,
	}
	ignoredHistory := &models.AppScalingHistory{
		AppId:        appId,
		Timestamp:    timestamp,
		ScalingType:  models.ScalingType(0),
		Status:       models.ScalingStatus(2),
		OldInstances: 0,
		NewInstances: 1,
		Reason:       reason,
		Message:      message,
		Error:        errorMsg,
	}
	for i := 0; i < 100*10000; i++ {
		if i%10 == 1 {
			historyList = append(historyList, succeedHistory)
		} else if i%10 == 2 {
			historyList = append(historyList, failHistory)
		} else {
			historyList = append(historyList, ignoredHistory)
		}

	}
	fmt.Printf("-------size:%d\n", len(historyList))
	sdb.SaveScalingHistoryBatch(historyList, 100)
}
