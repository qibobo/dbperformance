package db

import (
	"dbperformance/models"
)

const PostgresDriverName = "postgres"

type OrderType uint8

const (
	DESC OrderType = iota
	ASC
)
const (
	DESCSTR string = "DESC"
	ASCSTR  string = "ASC"
)

type ScalingEngineDB interface {
	SaveScalingHistory(history *models.AppScalingHistory) error
	SaveScalingHistoryBatch(historyList []*models.AppScalingHistory, commitCount int) error
	RetrieveScalingHistoriesNotEqual(appId string, start int64, end int64, orderType OrderType) ([]*models.AppScalingHistory, error)
	RetrieveScalingHistoriesIn(appId string, start int64, end int64, orderType OrderType) ([]*models.AppScalingHistory, error)
	PruneScalingHistories(before int64) error
	Close() error
}
