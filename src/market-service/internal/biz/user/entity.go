package user

import (
	"encoding/json"
	"fmt"
	"market-service/internal/biz/base"

	"github.com/go-redis/redis/v8"
)

const (
	NewUserStreamKey            = "new-user-stream"
	BindInviteRelationStreamKey = "bind-invite-relation-stream"
	UserTradeStreamKey          = "user-trade-stream"
	ClaimTaskRewardStreamKey    = "claim-task-reward-stream"
)

const (
	UserFollowStatusNormal  = 1
	UserFollowStatusDeleted = 2

	NotificationStatusRead   = 1
	NotificationStatusUnRead = 2

	NotificationCategoryTrade     = 1
	NotificationCategoryCommunity = 2

	NotificationTypeReceiveInitPoints   = 1
	NotificationTypeClaimTaskMintPoints = 2
	NotificationTypeTrade               = 3
	NotificationTypeMarketDone          = 4
	NotificationTypeClaimResult         = 5
	NotificationTypeReceiveComment      = 6
	NotificationTypeReceiveLike         = 7
	NotificationTypeReceiveFollow       = 8
	NotificationTypeMintInvitePoints    = 9

	CommentTypeRoot  = 1
	CommentTypeReply = 2

	UserSourceMagiclink = 1
	UserSourcePrivy     = 2
)

type UserEntity struct {
	base.BaseEntity
	EoaAddress          string          `json:"eoa_address"`
	Address             string          `json:"address"`
	Email               string          `json:"email"`
	UID                 string          `json:"uid"`
	InviteCode          string          `json:"invite_code"`
	InviterUID          string          `json:"inviter_uid"`
	Name                string          `json:"name"`
	Avatar              string          `json:"avatar"`
	Issuer              string          `json:"issuer"`
	Description         string          `json:"description"`
	FollowerCount       int64           `json:"follower_count"`
	FollowCount         int64           `json:"follow_count"`
	PostCount           int64           `json:"post_count"`
	InviteAt            int64           `json:"invite_at"`
	EarnedInvitePoints  int64           `json:"earned_invite_points"`
	ProvideInvitePoints int64           `json:"provide_invite_points"`
	PrivyUserInfo       json.RawMessage `json:"privy_user_info"`
	Source              int8            `json:"source"`
}

type UserFollowEntity struct {
	base.BaseEntity
	UID       string `json:"uid"`
	FollowUID string `json:"follow_uid"`
	Status    uint8  `json:"status"`
}

type UserNotificationEntity struct {
	base.BaseEntity
	UUID          string          `json:"uuid"`
	UID           string          `json:"uid"`
	Type          uint8           `json:"type"`
	Category      uint8           `json:"category"`
	BizJson       json.RawMessage `json:"biz_json"`
	Status        uint8           `json:"status"`
	BaseTokenAddress string          `json:"base_token_address"`
}

// 初始积分通知 type=1
type InitPointsNotificationEntity struct {
	PointAddress string `json:"pointAddress"` // 积分代币地址
	Amount       string `json:"amount"`       // 积分数量
	Decimal      int32  `json:"decimal"`      // 积分精度
}

// 领取任务奖励mint积分通知 type=2
type ClaimTaskRewardNotificationEntity struct {
	TaskName   string `json:"taskName"`
	TaskKey    string `json:"taskKey"`
	TaskDesc   string `json:"taskDesc"`
	TaskPicUrl string `json:"taskPicUrl"`
	TaskUUID   string `json:"taskUuid"`
	TaskReward uint64 `json:"taskReward"`

	UserTaskUUID string `json:"userTaskUuid"`
}

// 交易通知 type=3
type TradeNotificationEntity struct {
	MarketAddress string `json:"marketAddress"` // 市场地址
	MarketName    string `json:"marketName"`    // 市场名称
	MarketDesc    string `json:"marketDesc"`    // 市场描述
	MarketPicUrl  string `json:"marketPicUrl"`  // 市场图片

	OptionAddress    string `json:"optionAddress"`    // 选项地址
	OptionName       string `json:"optionName"`       // 选项名称
	OptionDesc       string `json:"optionDesc"`       // 选项描述
	OptionPicUrl     string `json:"optionPicUrl"`     // 选项图片
	BaseTokenAddress string `json:"baseTokenAddress"` // 基础代币地址
	Decimal          int32  `json:"decimal"`          // 基础代币精度

	AmountIn  string `json:"amountIn"`
	AmountOut string `json:"amountOut"`

	Side uint8 `json:"side"` // 1: buy, 2: sell
}

// 市场完成通知 type=4
type MarketRedeemNotificationEntity struct {
	MarketAddress string `json:"marketAddress"` // 市场地址
	MarketName    string `json:"marketName"`    // 市场名称
	MarketDesc    string `json:"marketDesc"`    // 市场描述
	MarketPicUrl  string `json:"marketPicUrl"`  // 市场图片

	// 获胜的选项信息
	OptionAddress string `json:"optionAddress"` // 选项地址
	OptionName    string `json:"optionName"`    // 选项名称
	OptionDesc    string `json:"optionDesc"`    // 选项描述
	OptionPicUrl  string `json:"optionPicUrl"`  // 选项图片

	Decimal int32 `json:"decimal"` // 选项精度

	// 持有的获胜选项代币数量
	Amount string `json:"amount"`
}

// 发布的帖子收到评论 / 根评论收到回复 type=6
type ReceiveCommentNotificationEntity struct {
	PostUUID string `json:"postUuid"` // 帖子UUID
	PostId   uint   `json:"postId"`   // 帖子ID

	CommentUUID    string `json:"commentUuid"`    // 评论UUID
	CommentContent string `json:"commentContent"` // 评论内容
	CommentId      uint64 `json:"commentId"`      // 评论ID (1根评论就是根评论的Id 2回复就是回复的Id)

	CommentUID    string `json:"commentUid"`    // 评论者UID
	CommentName   string `json:"commentName"`   // 评论者名称
	CommentAvatar string `json:"commentAvatar"` // 评论者头像

	CommentType uint8 `json:"commentType"` // 1: 根评论 2: 回复

	MarketAddress string `json:"marketAddress"` // 市场地址
}

// 发布的帖子收到点赞 type=7
type ReceiveLikeNotificationEntity struct {
	MarketAddress string `json:"marketAddress"` // 市场地址
	PostId        uint   `json:"postId"`        // 帖子ID
	PostUUID      string `json:"postUuid"`      // 帖子UUID

	NewLikeCount uint32 `json:"newLikeCount"` // 新的点赞数量

	LikeUserUID    string `json:"likeUserUid"`    // 点赞者UID
	LikeUserName   string `json:"likeUserName"`   // 点赞者名称
	LikeUserAvatar string `json:"likeUserAvatar"` // 点赞者头像
}

// 收到关注通知 type=8
type ReceiveFollowNotificationEntity struct {
	FollowerUID    string `json:"followerUid"`    // 关注者UID
	FollowerName   string `json:"followerName"`   // 关注者名称
	FollowerAvatar string `json:"followerAvatar"` // 关注者头像
}

// 邀请者mint积分通知 type=9
type MintInviteRewardPointsNotificationEntity struct {
	InviteeUID    string `json:"inviteeUid"`    // 被邀请者UID
	InviteeName   string `json:"inviteeName"`   // 被邀请者名称
	InviteeAvatar string `json:"inviteeAvatar"` // 被邀请者头像
	Reward        uint64 `json:"reward"`        // 被邀请奖励
}

type BindInviteRelationStreamMsg struct {
	InviterUID string `json:"inviterUid"` // 邀请者UID
	InviteeUID string `json:"inviteeUid"` // 被邀请者UID
	Timestamp  int64  `json:"timestamp"`  // 绑定时间
}

func (m *BindInviteRelationStreamMsg) ParseBindInviteRelationMessage(msg redis.XMessage) error {
	dataValue, exists := msg.Values["data"]
	if !exists {
		return fmt.Errorf("missing data field")
	}

	dataStr, ok := dataValue.(string)
	if !ok {
		return fmt.Errorf("data is not a string")
	}

	if err := json.Unmarshal([]byte(dataStr), m); err != nil {
		return fmt.Errorf("failed to unmarshal message: %w", err)
	}

	return nil
}

type UserTradeStreamMsg struct {
	UID           string `json:"uid"`
	MarketAddress string `json:"market_address"`
	OptionAddress string `json:"option_address"`
	Side          uint8  `json:"side"`
	AmountIn      string `json:"amount_in"`
	AmountOut     string `json:"amount_out"`
	Price         string `json:"price"`
	Timestamp        int64  `json:"timestamp"`
	BaseTokenAddress string `json:"base_token_address"`
	OpHash           string `json:"op_hash"`
	TxHash        string `json:"tx_hash"`
}

// parseMessage 解析消息
func (m *UserTradeStreamMsg) ParseUserTradeMessage(msg redis.XMessage) error {
	dataValue, exists := msg.Values["data"]
	if !exists {
		return fmt.Errorf("missing data field")
	}

	dataStr, ok := dataValue.(string)
	if !ok {
		return fmt.Errorf("data is not a string")
	}

	if err := json.Unmarshal([]byte(dataStr), m); err != nil {
		return fmt.Errorf("failed to unmarshal message: %w", err)
	}

	return nil
}

type ClaimTaskRewardStreamMsg struct {
	UID          string `json:"uid"`
	UserTaskUuid string `json:"user_task_uuid"`
	TaskKey      string `json:"task_key"`
	Reward       uint64 `json:"reward"`
	Timestamp    uint64 `json:"timestamp"`
}

func (m *ClaimTaskRewardStreamMsg) ParseClaimTaskRewardMessage(msg redis.XMessage) error {
	dataValue, exists := msg.Values["data"]
	if !exists {
		return fmt.Errorf("missing data field")
	}

	dataStr, ok := dataValue.(string)
	if !ok {
		return fmt.Errorf("data is not a string")
	}

	if err := json.Unmarshal([]byte(dataStr), m); err != nil {
		return fmt.Errorf("failed to unmarshal message: %w", err)
	}

	return nil
}

func (m *UserEntity) ParseNewUserMessage(msg redis.XMessage) error {
	dataValue, exists := msg.Values["data"]
	if !exists {
		return fmt.Errorf("missing data field")
	}

	dataStr, ok := dataValue.(string)
	if !ok {
		return fmt.Errorf("data is not a string")
	}

	if err := json.Unmarshal([]byte(dataStr), m); err != nil {
		return fmt.Errorf("failed to unmarshal message: %w", err)
	}

	return nil
}
