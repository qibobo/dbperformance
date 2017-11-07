package sqldb_test

import (
	"code.cloudfoundry.org/lager"
	"dbperformance/db"
	. "dbperformance/db/sqldb"
	"fmt"
	. "github.com/onsi/ginkgo"
	"os"
	"time"
)

var _ = Describe("ScalingengineSqldb", func() {

	var (
		url    string
		logger lager.Logger
		ssdb   *ScalingEngineSQLDB
		err    error
	)

	Context("test", func() {
		BeforeEach(func() {
			logger = lager.NewLogger("history-sqldb-test")
			url = os.Getenv("DBURL")
			ssdb, err = NewScalingEngineDB(url, logger)

		})
		It("Select by in", func() {
			start := time.Now().Nanosecond()
			list, _ := ssdb.RetrieveScalingHistoriesNotEqual(appId, 0, 1000*10000, db.DESC)
			end := time.Now().Nanosecond()
			fmt.Printf("========>%d, %d\n", len(list), (end-start)/(1000*1000))
		})
		It("Select by <>", func() {
			start := time.Now().Nanosecond()
			list, _ := sdb.RetrieveScalingHistoriesIn(appId, 0, 1000*10000, db.DESC)
			end := time.Now().Nanosecond()
			fmt.Printf("========>%d, %d\n", len(list), (end-start)/(1000*1000))
		})
	})

})
