package task

import "market-service/internal/biz/base"

var TxTradeTaskKeyList = []string{
	TaskKeyFirstTrade,
	// TaskKeyFirstShare,
	TaskKeyDailyFirstTrade,
	TaskKeyDailyFiveTrades,
	TaskKeyDailyFiveMarketTrades,
}

const (
	TaskClaimLockKey = "user-claim-task-reward-%s"
)

const (
	TaskKeyFirstTrade            = "first-trade"
	TaskKeyFirstShare            = "first-share"
	TaskKeyDailyFirstTrade       = "daily-first-trade"
	TaskKeyDailyFiveTrades       = "daily-five-trades"
	TaskKeyDailyFiveMarketTrades = "daily-five-market-trades"
)

const (
	TaskTypeNewUser = 1
	TaskTypeDaily   = 2

	UserTaskClaimedStatusNotClaimed = 2
	UserTaskClaimedStatusClaimed    = 1

	UserTaskTxStatusSuccess = 1
	UserTaskTxStatusFailed  = 2
	UserTaskTxStatusPending = 3
)

type TaskEntity struct {
	base.BaseEntity
	UUID        string `json:"uuid"`
	Key         string `json:"key"`
	IsShow      uint8  `json:"is_show"`
	Type        uint8  `json:"type"`
	Name        string `json:"name"`
	Description string `json:"desc"`
	PicUrl      string `json:"pic_url"`
	Reward      uint64 `json:"reward"`
	JumpUrl     string `json:"jump_url"`
}

type UserTaskEntity struct {
	base.BaseEntity
	UUID      string `json:"uuid"`
	UID       string `json:"uid"`
	TaskUUID  string `json:"task_uuid"`
	TaskKey   string `json:"task_key"`
	Reward    uint64 `json:"reward"`
	Claimed   uint8  `json:"claimed"`
	ClaimedAt uint64 `json:"claimed_at"`
}
