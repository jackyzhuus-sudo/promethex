package redis_stream_consumer

import (
	"encoding/json"
	"fmt"
	"math/big"
	"runtime/debug"
	"time"

	assetBiz "market-service/internal/biz/asset"
	marketBiz "market-service/internal/biz/market"
	taskBiz "market-service/internal/biz/task"
	userBiz "market-service/internal/biz/user"
	"market-service/internal/conf"
	"market-service/internal/data/base"
	"market-service/internal/pkg/common"
	"market-service/internal/pkg/util"

	alarm "market-service/internal/pkg/alarm"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-redis/redis/v8"
	"github.com/shopspring/decimal"
)

// MintPointsHandler 积分消费者处理器
type MintPointsHandler struct {
	*baseStreamConsumer // 复用基础消费者功能

	log        log.Logger
	userBiz    *userBiz.UserHandler
	assetBiz   *assetBiz.AssetHandler
	marketBiz  *marketBiz.MarketHandler
	taskBiz    *taskBiz.TaskHandler
	confCustom *conf.Custom
}

func NewMintPointsHandler(
	repo base.UsercenterInfra,
	log log.Logger,
	userBiz *userBiz.UserHandler,
	assetBiz *assetBiz.AssetHandler,
	marketBiz *marketBiz.MarketHandler,
	taskBiz *taskBiz.TaskHandler,
	confCustom *conf.Custom,
) *MintPointsHandler {
	handler := &MintPointsHandler{
		log:        log,
		userBiz:    userBiz,
		assetBiz:   assetBiz,
		marketBiz:  marketBiz,
		taskBiz:    taskBiz,
		confCustom: confCustom,
	}
	handler.baseStreamConsumer = newBaseStreamConsumer(handler, repo, log)
	return handler
}

func (h *MintPointsHandler) GetConfig() StreamConsumerConfig {
	return StreamConsumerConfig{
		Name:         "mint_points_consumer",
		GroupName:    "mint-points-group",
		ConsumerName: "mint-points-consumer",
		StreamKeys: []string{
			userBiz.NewUserStreamKey,
			userBiz.BindInviteRelationStreamKey,
			userBiz.ClaimTaskRewardStreamKey,
		},
		BatchSize:     10,
		BlockDuration: 5 * time.Second,
	}
}

func (h *MintPointsHandler) ProcessMessage(ctx common.Ctx, streamKey string, msg redis.XMessage) error {
	switch streamKey {
	case userBiz.NewUserStreamKey:
		return h.processNewUserMessage(ctx, msg)
	case userBiz.BindInviteRelationStreamKey:
		return h.processBindInviteRelationMessage(ctx, msg)
	case userBiz.ClaimTaskRewardStreamKey:
		return h.processClaimTaskRewardMessage(ctx, msg)
	default:
		return fmt.Errorf("unknown stream key: %s", streamKey)
	}
}

// findPointsToken returns the Points token address and config from the AssetTokens map.
func (h *MintPointsHandler) findPointsToken() (string, *conf.Custom_AssetToken) {
	for addr, token := range h.confCustom.AssetTokens {
		if token.Symbol == "POINTS" || token.Name == "Points" {
			return addr, token
		}
	}
	return "", nil
}

// processMessage 处理单条消息
func (h *MintPointsHandler) processNewUserMessage(ctx common.Ctx, msg redis.XMessage) error {
	pointsAddr, pointsToken := h.findPointsToken()
	if pointsToken == nil {
		return fmt.Errorf("points token not found in config")
	}

	// 解析消息
	newUserEntity := &userBiz.UserEntity{}
	err := newUserEntity.ParseNewUserMessage(msg)
	if err != nil {
		return fmt.Errorf("failed to parse message: %w", err)
	}

	ctx.Log.Infof("Processing new user message: UID=%s, userEntity: %+v",
		newUserEntity.UID, newUserEntity)

	amount := new(big.Int).SetInt64(int64(h.confCustom.NewUserPoints))
	multiplier := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(pointsToken.Decimals)), nil)
	finalAmount := new(big.Int).Mul(amount, multiplier)

	// 幂等检查
	records, err := h.assetBiz.GetUserMintPoints(ctx, &assetBiz.UserMintPointsQuery{
		Uid:      newUserEntity.UID,
		Source:   assetBiz.UserMintPointsSourceNewUser,
		StatusIn: []int{int(assetBiz.UserMintPointsStatusPending), int(assetBiz.UserMintPointsStatusSuccess)},
	})
	if err != nil {
		return fmt.Errorf("failed to get user mint points: %w", err)
	}
	if len(records) > 0 {
		ctx.Log.Infof("user %s already mint points", newUserEntity.UID)
		return nil
	}

	txHash, err := h.assetBiz.MintPointsToUser(ctx, newUserEntity.UID, newUserEntity.Address, assetBiz.TxSourceMintInitPoins, finalAmount)
	if err != nil {
		return fmt.Errorf("failed to mint points: %w", err)
	}

	userMintPointsEntity := &assetBiz.UserMintPointsEntity{
		UUID:             util.GenerateUUID(),
		UID:              newUserEntity.UID,
		TokenAddress:     pointsAddr,
		BaseTokenAddress: pointsAddr,
		Amount:           decimal.NewFromBigInt(finalAmount, 0),
		Status:           assetBiz.UserMintPointsStatusPending,
		EventProcessed:   assetBiz.ProcessedNo,
		Source:           assetBiz.UserMintPointsSourceNewUser,
		TxHash:           txHash,
		// InviteUID:      newUserEntity.InviteUID,
	}

	success, err := h.assetBiz.WaitMintPointsReceipt(ctx, txHash, userMintPointsEntity)
	if err != nil {
		ctx.Log.Errorf("WaitMintPointsReceipt failed for txHash %s: %v", txHash, err)
		return err
	}

	if !success {
		ctx.Log.Errorf("Mint points transaction failed: %s", txHash)
		alarm.Lark.Send(fmt.Sprintf("user %s mint points transaction failed: %s", newUserEntity.UID, txHash))
		return nil
	}

	go func() {

		defer func() {
			if r := recover(); r != nil {
				alarm.Lark.Send(fmt.Sprintf("panic in generate new user notification: %v, stack: %s", r, string(debug.Stack())))
				ctx.Log.Errorf("panic in generate new user notification: %v, stack: %s", r, string(debug.Stack()))
			}
		}()

		// 生成mint_points通知
		bizData, err := json.Marshal(&userBiz.InitPointsNotificationEntity{
			PointAddress: pointsAddr,
			Amount:       finalAmount.String(),
			Decimal:      int32(pointsToken.Decimals),
		})
		if err != nil {
			ctx.Log.Errorf("marshal user notification entity error", err)
			return
		}
		if err = h.userBiz.GenerateNewUserNotification(ctx, &userBiz.UserNotificationEntity{
			UID:              newUserEntity.UID,
			UUID:             util.GenerateUUID(),
			Type:             userBiz.NotificationTypeReceiveInitPoints,
			BizJson:          json.RawMessage(bizData),
			Status:           userBiz.NotificationStatusUnRead,
			Category:         userBiz.NotificationCategoryTrade,
			BaseTokenAddress: pointsAddr,
		}); err != nil {
			ctx.Log.Errorf("create init points notification error", "error", err)
		}
	}()

	ctx.Log.Infof("Successfully processed mint points message: UID=%s, TxHash=%s",
		newUserEntity.UID, txHash)

	return nil
}

func (h *MintPointsHandler) processBindInviteRelationMessage(ctx common.Ctx, msg redis.XMessage) error {
	pointsAddr, pointsToken := h.findPointsToken()
	if pointsToken == nil {
		return fmt.Errorf("points token not found in config")
	}

	bindInviteRelationEntity := &userBiz.BindInviteRelationStreamMsg{}
	err := bindInviteRelationEntity.ParseBindInviteRelationMessage(msg)
	if err != nil {
		return fmt.Errorf("failed to parse message: %w", err)
	}

	ctx.Log.Infof("Processing bind invite relation message: InviterUID=%s, InviteeUID=%s",
		bindInviteRelationEntity.InviterUID, bindInviteRelationEntity.InviteeUID)

	amount := new(big.Int).SetInt64(int64(h.confCustom.NewUserPoints))
	multiplier := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(pointsToken.Decimals)), nil)
	finalAmount := new(big.Int).Mul(amount, multiplier)

	// 幂等检查
	records, err := h.assetBiz.GetUserMintPoints(ctx, &assetBiz.UserMintPointsQuery{
		Uid:       bindInviteRelationEntity.InviterUID,
		InviteUID: bindInviteRelationEntity.InviteeUID,
		Source:    assetBiz.UserMintPointsSourceInvite,
		StatusIn:  []int{int(assetBiz.UserMintPointsStatusPending), int(assetBiz.UserMintPointsStatusSuccess)},
	})
	if err != nil {
		return fmt.Errorf("failed to get user mint points: %w", err)
	}
	if len(records) > 0 {
		ctx.Log.Infof("inviter %s already mint points for invitee %s", bindInviteRelationEntity.InviterUID, bindInviteRelationEntity.InviteeUID)
		return nil
	}

	inviter, err := h.userBiz.GetUserInfo(ctx, &userBiz.UserQuery{
		UID: bindInviteRelationEntity.InviterUID,
	})
	if err != nil {
		return fmt.Errorf("failed to get inviter: %w", err)
	}
	if inviter == nil || inviter.UID == "" {
		ctx.Log.Errorf("inviter not found: %s", bindInviteRelationEntity.InviterUID)
		return fmt.Errorf("inviter not found: %s", bindInviteRelationEntity.InviterUID)
	}

	txHash, err := h.assetBiz.MintPointsToUser(ctx, inviter.UID, inviter.Address, assetBiz.TxSourceMintInviteRewardPoints, finalAmount)
	if err != nil {
		ctx.Log.Errorf("mint invite points error: %+v", err)
		alarm.Lark.Send(fmt.Sprintf("mint invite points to inviter [%s] error: %+v", bindInviteRelationEntity.InviterUID, err))
		return err
	}

	userMintPointsEntity := &assetBiz.UserMintPointsEntity{
		UUID:             util.GenerateUUID(),
		UID:              inviter.UID,
		TokenAddress:     pointsAddr,
		BaseTokenAddress: pointsAddr,
		Amount:           decimal.NewFromBigInt(finalAmount, 0),
		Status:           assetBiz.UserMintPointsStatusPending,
		Source:           assetBiz.UserMintPointsSourceInvite,
		TxHash:           txHash,
		OpHash:           "",
		EventProcessed:   assetBiz.ProcessedNo,
		InviteUID:        bindInviteRelationEntity.InviteeUID,
	}

	success, err := h.assetBiz.WaitMintPointsReceipt(ctx, txHash, userMintPointsEntity)
	if err != nil {
		ctx.Log.Errorf("async mint invite points WaitMintPointsReceipt error: %+v", err)
		alarm.Lark.Send(fmt.Sprintf("async mint invite points to inviter [%s] WaitMintPointsReceipt error: %+v", inviter.UID, err))
		return err
	}
	if !success {
		ctx.Log.Errorf("async mint invite points WaitMintPointsReceipt error: txHash: %s", txHash)
		alarm.Lark.Send(fmt.Sprintf("async mint invite points to inviter [%s] failed. txHash: %s", inviter.UID, txHash))
		return nil
	}

	err = h.userBiz.UpdateUserInvitePoints(ctx, bindInviteRelationEntity.InviteeUID, inviter.UID, int64(h.confCustom.NewUserPoints))
	if err != nil {
		ctx.Log.Errorf("async update user invite points error: %+v", err)
		alarm.Lark.Send(fmt.Sprintf("async update user invite points to inviter [%s] error: %+v", inviter.UID, err))
		return err
	}

	go func() {

		defer func() {
			if r := recover(); r != nil {
				alarm.Lark.Send(fmt.Sprintf("panic in generate mint invite points notification: %v, stack: %s", r, string(debug.Stack())))
				ctx.Log.Errorf("panic in generate mint invite points notification: %v, stack: %s", r, string(debug.Stack()))
			}
		}()

		invitee, err := h.userBiz.GetUserInfo(ctx, &userBiz.UserQuery{
			UID: bindInviteRelationEntity.InviteeUID,
		})
		if err != nil {
			ctx.Log.Errorf("failed to get invitee: %w", err)
			return
		}
		if invitee == nil || invitee.UID == "" {
			ctx.Log.Errorf("invitee not found: %s", bindInviteRelationEntity.InviteeUID)
			return
		}

		// 生成mint_points通知
		bizData, err := json.Marshal(&userBiz.MintInviteRewardPointsNotificationEntity{
			InviteeUID:    bindInviteRelationEntity.InviteeUID,
			InviteeName:   invitee.Name,
			InviteeAvatar: invitee.Avatar,
			Reward:        uint64(h.confCustom.NewUserPoints),
		})
		if err != nil {
			ctx.Log.Errorf("marshal user notification entity error", err)
			return
		}
		if err = h.userBiz.GenerateNewUserNotification(ctx, &userBiz.UserNotificationEntity{
			UID:              bindInviteRelationEntity.InviterUID,
			UUID:             util.GenerateUUID(),
			Type:             userBiz.NotificationTypeMintInvitePoints,
			BizJson:          json.RawMessage(bizData),
			Status:           userBiz.NotificationStatusUnRead,
			Category:         userBiz.NotificationCategoryTrade,
			BaseTokenAddress: pointsAddr,
		}); err != nil {
			ctx.Log.Errorf("create mint invite points notification error", "error", err)
		}
	}()

	ctx.Log.Infof("Successfully processed bind invite relation message: InviterUID=%s, InviteeUID=%s, TxHash=%s",
		bindInviteRelationEntity.InviterUID, bindInviteRelationEntity.InviteeUID, txHash)

	return nil
}

func (h *MintPointsHandler) processClaimTaskRewardMessage(ctx common.Ctx, msg redis.XMessage) error {
	pointsAddr, pointsToken := h.findPointsToken()
	if pointsToken == nil {
		return fmt.Errorf("points token not found in config")
	}

	claimTaskRewardEntity := &userBiz.ClaimTaskRewardStreamMsg{}
	err := claimTaskRewardEntity.ParseClaimTaskRewardMessage(msg)
	if err != nil {
		return fmt.Errorf("failed to parse message: %w", err)
	}

	ctx.Log.Infof("Processing claim task reward message: UID=%s, TaskKey=%s, Reward=%d",
		claimTaskRewardEntity.UID, claimTaskRewardEntity.TaskKey, claimTaskRewardEntity.Reward)

	amount := new(big.Int).SetInt64(int64(claimTaskRewardEntity.Reward))
	multiplier := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(pointsToken.Decimals)), nil)
	finalAmount := new(big.Int).Mul(amount, multiplier)

	// 幂等检查
	userTaskEntity, err := h.taskBiz.GetUserTask(ctx, &taskBiz.UserTaskQuery{
		UUID: claimTaskRewardEntity.UserTaskUuid,
	})
	if err != nil {
		return fmt.Errorf("failed to get user tasks: %w", err)
	}
	if userTaskEntity == nil || userTaskEntity.UUID == "" {
		ctx.Log.Errorf("user task not found: %s", claimTaskRewardEntity.UserTaskUuid)
		return fmt.Errorf("user task not found: %s", claimTaskRewardEntity.UserTaskUuid)
	}

	userMintPointsEntities, err := h.assetBiz.GetUserMintPoints(ctx, &assetBiz.UserMintPointsQuery{
		UserTaskUuids: []string{userTaskEntity.UUID},
		Source:        assetBiz.UserMintPointsSourceTaskClaim,
		StatusIn:      []int{int(assetBiz.UserMintPointsStatusPending), int(assetBiz.UserMintPointsStatusSuccess)},
	})
	if err != nil {
		return fmt.Errorf("failed to get user mint points: %w", err)
	}
	if len(userMintPointsEntities) > 0 {
		ctx.Log.Infof("user %s already mint points for task %s", userTaskEntity.UID, userTaskEntity.UUID)
		return nil
	}

	user, err := h.userBiz.GetUserInfo(ctx, &userBiz.UserQuery{
		UID: userTaskEntity.UID,
	})
	if err != nil {
		return fmt.Errorf("failed to get user: %w", err)
	}
	if user == nil || user.UID == "" {
		ctx.Log.Errorf("user not found: %s", userTaskEntity.UID)
		return fmt.Errorf("user not found: %s", userTaskEntity.UID)
	}

	txHash, err := h.assetBiz.MintPointsToUser(ctx, user.UID, user.Address, assetBiz.TxSourceMintTaskRewardPoints, finalAmount)
	if err != nil {
		ctx.Log.Errorf("mint task reward points error: %+v", err)
		alarm.Lark.Send(fmt.Sprintf("mint task reward points to user [%s] error: %+v", user.UID, err))
		return err
	}

	userMintPointsEntity := &assetBiz.UserMintPointsEntity{
		UUID:             util.GenerateUUID(),
		UID:              user.UID,
		TokenAddress:     pointsAddr,
		BaseTokenAddress: pointsAddr,
		Amount:           decimal.NewFromBigInt(finalAmount, 0),
		Status:           assetBiz.UserMintPointsStatusPending,
		Source:           assetBiz.UserMintPointsSourceTaskClaim,
		TxHash:           txHash,
		OpHash:           "",
		EventProcessed:   assetBiz.ProcessedNo,
		UserTaskUUID:     userTaskEntity.UUID,
	}

	success, err := h.assetBiz.WaitMintPointsReceipt(ctx, txHash, userMintPointsEntity)
	if err != nil {
		ctx.Log.Errorf("async mint task reward points WaitMintPointsReceipt error: %+v", err)
		alarm.Lark.Send(fmt.Sprintf("async mint task reward points to user [%s] WaitMintPointsReceipt error: %+v", user.UID, err))
		return err
	}
	// 失败或者不确定，不返回错误重试，增加定时任重新Mint
	if !success {
		ctx.Log.Errorf("async mint task reward points WaitMintPointsReceipt error: txHash: %s", txHash)
		alarm.Lark.Send(fmt.Sprintf("async mint task reward points to user [%s] failed. txHash: %s", user.UID, txHash))
		return nil
	}

	go func() {

		defer func() {
			if r := recover(); r != nil {
				alarm.Lark.Send(fmt.Sprintf("panic in generate claim task reward notification: %v, stack: %s", r, string(debug.Stack())))
				ctx.Log.Errorf("panic in generate claim task reward notification: %v, stack: %s", r, string(debug.Stack()))
			}
		}()

		task, err := h.taskBiz.GetTaskInfo(ctx, &taskBiz.TaskQuery{
			Key: userTaskEntity.TaskKey,
		})
		if err != nil {
			ctx.Log.Errorf("failed to get task: %w", err)
			return
		}
		if task == nil || task.Key == "" {
			ctx.Log.Errorf("task not found: %s", userTaskEntity.TaskKey)
			return
		}

		// 生成mint_points通知
		bizData, err := json.Marshal(&userBiz.ClaimTaskRewardNotificationEntity{
			TaskName:     task.Name,
			TaskKey:      task.Key,
			TaskDesc:     task.Description,
			TaskPicUrl:   task.PicUrl,
			TaskUUID:     task.UUID,
			TaskReward:   task.Reward,
			UserTaskUUID: userTaskEntity.UUID,
		})
		if err != nil {
			ctx.Log.Errorf("marshal user notification entity error", err)
			return
		}
		if err = h.userBiz.GenerateNewUserNotification(ctx, &userBiz.UserNotificationEntity{
			UID:              user.UID,
			UUID:             util.GenerateUUID(),
			Type:             userBiz.NotificationTypeClaimTaskMintPoints,
			BizJson:          json.RawMessage(bizData),
			Status:           userBiz.NotificationStatusUnRead,
			Category:         userBiz.NotificationCategoryTrade,
			BaseTokenAddress: pointsAddr,
		}); err != nil {
			ctx.Log.Errorf("create claim task reward notification error", "error", err)
		}
	}()

	return nil
}
