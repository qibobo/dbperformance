package sqldb

import (
	"code.cloudfoundry.org/lager"
	"database/sql"
	"dbperformance/db"
	"dbperformance/models"
	"fmt"
	_ "github.com/lib/pq"
	"time"
)

type ScalingEngineSQLDB struct {
	url    string
	logger lager.Logger
	sqldb  *sql.DB
}

var tableName string = "scalinghistory"

func NewScalingEngineDB(url string, logger lager.Logger) (*ScalingEngineSQLDB, error) {
	sqldb, err := sql.Open(db.PostgresDriverName, url)
	if err != nil {

		logger.Error("open-scaling-engine-db", err, lager.Data{"url": url})
		return nil, err
	}
	err = sqldb.Ping()
	if err != nil {
		sqldb.Close()
		logger.Error("ping-scaling-engine-db", err, lager.Data{"url": url})
		return nil, err
	}
	return &ScalingEngineSQLDB{
		url:    url,
		logger: logger,
		sqldb:  sqldb,
	}, nil
}

func (sdb *ScalingEngineSQLDB) Close() error {
	err := sdb.sqldb.Close()
	if err != nil {
		sdb.logger.Error("close-scaling-engine-db", err, lager.Data{"url": sdb.url})
		return err
	}
	return nil
}
func (sdb *ScalingEngineSQLDB) LogAndRollback(tx *sql.Tx) {
	err := tx.Rollback()
	if err != nil {
		sdb.logger.Error("transaction-error, rollback", err, lager.Data{})
	}
}
func (sdb *ScalingEngineSQLDB) SaveScalingHistory(history *models.AppScalingHistory) error {
	query := "INSERT INTO " + tableName +
		"(appid, timestamp, scalingtype, status, oldinstances, newinstances, reason, message, error) " +
		" VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9)"
	_, err := sdb.sqldb.Exec(query, history.AppId, history.Timestamp, history.ScalingType, history.Status,
		history.OldInstances, history.NewInstances, history.Reason, history.Message, history.Error)

	if err != nil {
		sdb.logger.Error("save-scaling-history", err, lager.Data{"query": query, "history": history})
	}
	return err
}

func (sdb *ScalingEngineSQLDB) SaveScalingHistoryBatch(historyList []*models.AppScalingHistory, commitCount int) error {
	tx, err := sdb.sqldb.Begin()
	if err != nil {
		sdb.logger.Error("save-scaling-history-get-context error", err)
		return err
	}
	defer sdb.LogAndRollback(tx)
	statment, err := tx.Prepare("INSERT INTO " + tableName +
		"(appid, timestamp, scalingtype, status, oldinstances, newinstances, reason, message, error) " +
		" VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9)")
	if err != nil {
		sdb.logger.Error("save-scaling-history-get-statement error", err)
		return err
	}
	count := 0
	for _, history := range historyList {
		// fmt.Printf("========>save %d\n", count)
		_, err := statment.Exec(history.AppId, history.Timestamp, history.ScalingType, history.Status,
			history.OldInstances, history.NewInstances, history.Reason, history.Message, history.Error)
		if err != nil {
			sdb.logger.Error("save-scaling-history", err)
			fmt.Printf("======error:%s\n", err)
		}
		count++
		// if count%commitCount == 0 {
		// 	fmt.Printf("-------->commit:%d\n", count)
		// 	tx.Commit()
		// 	sdb.logger.Info("commit", lager.Data{"count": count})
		// }
	}
	tx.Commit()
	sdb.logger.Info("commit", lager.Data{"count": count})
	return nil
}
func (sdb *ScalingEngineSQLDB) RetrieveScalingHistoriesNotEqual(appId string, start int64, end int64, orderType db.OrderType) ([]*models.AppScalingHistory, error) {
	var orderStr string
	if orderType == db.DESC {
		orderStr = db.DESCSTR
	} else {
		orderStr = db.ASCSTR
	}
	query := "SELECT timestamp, scalingtype, status, oldinstances, newinstances, reason, message, error FROM " + tableName + " WHERE" +
		" appid = $1 " +
		" AND timestamp >= $2" +
		" AND timestamp <= $3" +
		" AND status !=2" +
		" ORDER BY timestamp " + orderStr

	if end < 0 {
		end = time.Now().UnixNano()
	}

	histories := []*models.AppScalingHistory{}
	rows, err := sdb.sqldb.Query(query, appId, start, end)
	if err != nil {
		sdb.logger.Error("retrieve-scaling-histories", err,
			lager.Data{"query": query, "appid": appId, "start": start, "end": end, "orderType": orderType})
		return nil, err
	}

	defer rows.Close()

	var timestamp int64
	var scalingType, status, oldInstances, newInstances int
	var reason, message, errorMsg string

	for rows.Next() {
		if err = rows.Scan(&timestamp, &scalingType, &status, &oldInstances, &newInstances, &reason, &message, &errorMsg); err != nil {
			sdb.logger.Error("retrieve-scaling-history-scan", err)
			return nil, err
		}

		history := models.AppScalingHistory{
			AppId:        appId,
			Timestamp:    timestamp,
			ScalingType:  models.ScalingType(scalingType),
			Status:       models.ScalingStatus(status),
			OldInstances: oldInstances,
			NewInstances: newInstances,
			Reason:       reason,
			Message:      message,
			Error:        errorMsg,
		}
		histories = append(histories, &history)
	}
	return histories, nil
}

func (sdb *ScalingEngineSQLDB) RetrieveScalingHistoriesIn(appId string, start int64, end int64, orderType db.OrderType) ([]*models.AppScalingHistory, error) {
	var orderStr string
	if orderType == db.DESC {
		orderStr = db.DESCSTR
	} else {
		orderStr = db.ASCSTR
	}
	query := "SELECT timestamp, scalingtype, status, oldinstances, newinstances, reason, message, error FROM " + tableName + " WHERE" +
		" appid = $1 " +
		" AND timestamp >= $2" +
		" AND timestamp <= $3" +
		" AND status IN (0,1)" +
		" ORDER BY timestamp " + orderStr

	if end < 0 {
		end = time.Now().UnixNano()
	}

	histories := []*models.AppScalingHistory{}
	rows, err := sdb.sqldb.Query(query, appId, start, end)
	if err != nil {
		sdb.logger.Error("retrieve-scaling-histories", err,
			lager.Data{"query": query, "appid": appId, "start": start, "end": end, "orderType": orderType})
		return nil, err
	}

	defer rows.Close()

	var timestamp int64
	var scalingType, status, oldInstances, newInstances int
	var reason, message, errorMsg string

	for rows.Next() {
		if err = rows.Scan(&timestamp, &scalingType, &status, &oldInstances, &newInstances, &reason, &message, &errorMsg); err != nil {
			sdb.logger.Error("retrieve-scaling-history-scan", err)
			return nil, err
		}

		history := models.AppScalingHistory{
			AppId:        appId,
			Timestamp:    timestamp,
			ScalingType:  models.ScalingType(scalingType),
			Status:       models.ScalingStatus(status),
			OldInstances: oldInstances,
			NewInstances: newInstances,
			Reason:       reason,
			Message:      message,
			Error:        errorMsg,
		}
		histories = append(histories, &history)
	}
	return histories, nil
}

func (sdb *ScalingEngineSQLDB) PruneScalingHistories(before int64) error {
	query := "DELETE FROM " + tableName + " WHERE timestamp <= $1"
	_, err := sdb.sqldb.Exec(query, before)
	if err != nil {
		sdb.logger.Error("failed-prune-scaling-histories-from-scalinghistory-table", err, lager.Data{"query": query, "before": before})
	}
	return err
}
