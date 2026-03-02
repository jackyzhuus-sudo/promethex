package asset

import (
	"encoding/json"
	"fmt"
	"market-service/internal/biz/base"
	"market-service/internal/biz/market"
	"market-service/internal/biz/task"
	"market-service/internal/biz/user"
	"market-service/internal/conf"
	"market-service/internal/pkg/alarm"
	"market-service/internal/pkg/common"
	"market-service/internal/pkg/util"
	"math/big"
	"strings"
	"time"

	"github.com/bitly/go-simplejson"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/shopspring/decimal"

	marketcenterPb "market-proto/proto/market-service/marketcenter/v1"

	"github.com/go-kratos/kratos/v2/errors"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-redis/redis/v8"
	"gorm.io/gorm"
)

var (
	ErrInvalidUserOperation = errors.New(int(marketcenterPb.ErrorCode_INVALID_USER_OPERATION), "INVALID_USER_OPERATION", "invalid user operation")
	ErrPlaceOrderTooQuick   = errors.New(int(marketcenterPb.ErrorCode_PLACE_ORDER_TOO_QUICK), "PLACE_ORDER_TOO_QUICK", "Please wait for last transaction complete.")
)

type AssetRepoInterface interface {
	base.RepoInterface

	GetOrdersWithTotal(ctx common.Ctx, query *OrderQuery) ([]*OrderEntity, int64, error)
	CreateOrder(ctx common.Ctx, orderEntity *OrderEntity) error
	CreateOrUpdateOrder(ctx common.Ctx, orderEntity *OrderEntity, updateColumns []string) error
	CreateUserClaimResult(ctx common.Ctx, userClaimResultEntity *UserClaimResultEntity) error
	CreateOrUpdateUserClaimResult(ctx common.Ctx, userClaimResultEntity *UserClaimResultEntity, updateColumns []string) error
	CreateOrUpdateUserTransferTokens(ctx common.Ctx, userTransferTokensEntity *UserTransferTokensEntity, updateColumns []string) error
	UpdateUserClaimResultByOpHash(ctx common.Ctx, userClaimResultEntity *UserClaimResultEntity, updateMap map[string]interface{}) error
	UpdateUserClaimResultByTxHash(ctx common.Ctx, userClaimResultEntity *UserClaimResultEntity, updateMap map[string]interface{}) error
	CreateUserMintPoints(ctx common.Ctx, userMintPointsEntity *UserMintPointsEntity) error

	GetUserClaimResult(ctx common.Ctx, query *UserClaimResultQuery) (*UserClaimResultEntity, error)

	UpdateOrderByTxHash(ctx common.Ctx, orderEntity *OrderEntity, updateMap map[string]interface{}) error
	UpdateOrderByOpHash(ctx common.Ctx, orderEntity *OrderEntity, updateMap map[string]interface{}) error

	CreateSendTx(ctx common.Ctx, sendTxEntity *SendTxEntity) error
	GetSendTx(ctx common.Ctx, query *SendTxQuery) (*SendTxEntity, error)
	UpdateSendTxByOpHash(ctx common.Ctx, sendTxEntity *SendTxEntity, updateMap map[string]interface{}) error
	UpdateSendTxByTxHash(ctx common.Ctx, sendTxEntity *SendTxEntity, updateMap map[string]interface{}) error

	GetTransactionReceipt(ctx common.Ctx, txHash string) (*types.Receipt, error)

	CreateUserTokenBalance(ctx common.Ctx, userTokenBalanceEntity *UserTokenBalanceEntity) error
	CreateOrUpdateUserTokenBalance(ctx common.Ctx, userTokenBalanceEntity *UserTokenBalanceEntity) error
	GetUserTokenBalanceWithTotal(ctx common.Ctx, query *UserTokenBalanceQuery) ([]*UserTokenBalanceEntity, int64, error)
	GetUserTokenBalance(ctx common.Ctx, query *UserTokenBalanceQuery) ([]*UserTokenBalanceEntity, error)

	UpdateUserPositionTokenEndStatus(ctx common.Ctx, marketAddress string, winOptionAddress string) error
	UpdateUserMarketPositionStatus(ctx common.Ctx, marketAddress string, winOptionAddress string) error
	UpdateUserTokenBalanceIsClaimed(ctx common.Ctx, uid string, optionAddress []string) error

	GetUserTokenBalancesByQueryItems(ctx common.Ctx, queryItems []*UserTokenBalanceQueryItem) ([]*UserTokenBalanceEntity, error)

	GetUserMarketPositionsWithTotal(ctx common.Ctx, query *UserMarketPositionQuery) ([]*UserMarketPositionEntity, int64, error)

	BatchCreateUserAssetValue(ctx common.Ctx, userAssetValueEntities []*UserAssetValueEntity) error
	GetUserAssetValue(ctx common.Ctx, query *UserAssetValueQuery) (*UserAssetValueEntity, error)
	GetUserAssetHistory(ctx common.Ctx, uid string, baseTokenType uint8, timeRange string) ([]*UserAssetValueEntity, error)
	CreateOrUpdateUserMarketPosition(ctx common.Ctx, userMarketPositionEntity *UserMarketPositionEntity) error
	UpdateUserMarketPositionDecrease(ctx common.Ctx, userMarketPositionEntity *UserMarketPositionEntity) error

	GetUserTotalValue(ctx common.Ctx, query *UserTokenBalanceQuery) (decimal.Decimal, error)
	GetUserMarketPositionsByValue(ctx common.Ctx, query *UserTokenBalanceQuery) ([]*MarketValue, int64, error)

	// CreateOrUpdateUserTokenBalance(ctx common.Ctx, userTokenBalanceEntity *UserTokenBalanceEntity) error
	UpdateUserTokenBalanceByUidAndTokenAddress(ctx common.Ctx, userTokenBalanceEntity *UserTokenBalanceEntity, updateMap map[string]interface{}) error

	// CreateUserAssetValue(ctx common.Ctx, userAssetValueEntity *UserAssetValueEntity) error

	CreateUserTransferTokens(ctx common.Ctx, userTransferTokensEntity *UserTransferTokensEntity) error

	GetSendTxs(ctx common.Ctx, query *SendTxQuery) ([]*SendTxEntity, error)
	GetSendTxsWithTotal(ctx common.Ctx, query *SendTxQuery) ([]*SendTxEntity, int64, error)

	//
	GetOrders(ctx common.Ctx, query *OrderQuery) ([]*OrderEntity, error)
	GetUserClaimResults(ctx common.Ctx, query *UserClaimResultQuery) ([]*UserClaimResultEntity, error)
	GetUserMintPoints(ctx common.Ctx, query *UserMintPointsQuery) ([]*UserMintPointsEntity, error)
	GetUserTransferTokens(ctx common.Ctx, query *UserTransferTokensQuery) ([]*UserTransferTokensEntity, error)

	GetOrdersDistinctMarkets(ctx common.Ctx, query *OrderQuery) ([]string, error)

	GetUserEarnedPoints(ctx common.Ctx, uid string, source uint8) (decimal.Decimal, error)

	// alchemy
	GetPaymasterStubData(ctx common.Ctx, userOperation *UserOperation) (string, string, error)
	EstimateUserOperationGas(ctx common.Ctx, userOperation *UserOperation) (string, string, string, string, error)
	GetPaymasterData(ctx common.Ctx, userOperation *UserOperation) (string, string, error)
	RequestGasAndPaymasterAndData(ctx common.Ctx, userOperation *UserOperation) (*UserOperation, error)
	GetUserOperationReceipt(ctx common.Ctx, userOpHash string) (*simplejson.Json, error)
	SendUserOperationToAlchemy(ctx common.Ctx, userOperation *UserOperation) (string, error)
	MintERC20Token(ctx common.Ctx, tokenAddress string, to string, amount *big.Int) (string, error)
	CallContractView(ctx common.Ctx, contractAddr string, data []byte) ([]byte, error)

	// leaderboard
	ZRevRank(ctx common.Ctx, key, member string) (int64, error)
	ZScore(ctx common.Ctx, key, member string) (float64, error)
	ZRevRangeWithScores(ctx common.Ctx, key string, start, stop int64) ([]redis.Z, error)
	ZCount(ctx common.Ctx, key string, min, max string) (int64, error)
}

type AssetHandler struct {
	assetRepo  AssetRepoInterface
	marketRepo market.MarketRepoInterface // ?

	log        *log.Helper
	confCustom *conf.Custom

	// - -
	userRepo user.UserRepoInterface
	taskRepo task.TaskRepoInterface
}

func (h *AssetHandler) GetRepo() AssetRepoInterface {
	return h.assetRepo
}

func NewAssetHandler(userRepo user.UserRepoInterface, taskRepo task.TaskRepoInterface, assetRepo AssetRepoInterface, marketRepo market.MarketRepoInterface, confCustom *conf.Custom, logger log.Logger) *AssetHandler {
	return &AssetHandler{
		assetRepo:  assetRepo,
		marketRepo: marketRepo,
		confCustom: confCustom,
		log:        log.NewHelper(logger),
		userRepo:   userRepo,
		taskRepo:   taskRepo,
	}
}

func (h *AssetHandler) ProcessUserBaseTokenUpdate(ctx common.Ctx, userTokenBalanceEntity *UserTokenBalanceEntity) error {
	err := h.assetRepo.ExecTx(ctx, func(ctx common.Ctx, db *gorm.DB) error {
		err := h.assetRepo.CreateOrUpdateUserTokenBalance(ctx, userTokenBalanceEntity)
		if err != nil {
			return errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
		}

		// 1. points转账 来源 mint_points / transfer_base_token / 交易
		// 2. usdc转账 来源 transfer_base_token / 交易

		// 外部转入 创建交易记录
		if userTokenBalanceEntity.Side == uint8(marketcenterPb.UpdateUserBaseTokenBalanceRequest_SIDE_DEPOSIT) && userTokenBalanceEntity.FromAddress != "0x0000000000000000000000000000000000000000" {

			marketEntity, err := h.marketRepo.GetMarket(ctx, &market.MarketQuery{
				Address: userTokenBalanceEntity.FromAddress,
			})
			if err != nil {
				return errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
			}

			if marketEntity != nil && marketEntity.Address != "" {
				return nil
			}

			err = h.assetRepo.CreateSendTx(ctx, &SendTxEntity{
				BaseTokenType: uint8(userTokenBalanceEntity.BaseTokenType),
				UID:           userTokenBalanceEntity.UID,
				Type:          TxTypeNormal,
				Source:        TxSourceTransferDeposit,
				Status:        SendTxStatusExecSuccess,
				TxHash:        userTokenBalanceEntity.TxHash,
				Chain:         h.confCustom.Chain,
			})

			if err != nil {
				ctx.Log.Errorf("CreateSendTx error: %+v", err)
				return errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
			}

			err = h.assetRepo.CreateUserTransferTokens(ctx, &UserTransferTokensEntity{
				UUID:            util.GenerateUUID(),
				UID:             userTokenBalanceEntity.UID,
				TokenAddress:    userTokenBalanceEntity.TokenAddress,
				ExternalAddress: userTokenBalanceEntity.FromAddress,
				Side:            UserTransferTokensSideDeposit,
				BaseTokenType:   userTokenBalanceEntity.BaseTokenType,
				Amount:          userTokenBalanceEntity.TransferAmount,
				Status:          UserTransferTokensStatusSuccess,
				TxHash:          userTokenBalanceEntity.TxHash,
				EventProcessed:  ProcessedYes,
			})
			if err != nil {
				ctx.Log.Errorf("CreateUserTransferTokens error: %+v", err)
				return errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
			}

		}

		return nil
	})
	return err
}

func (h *AssetHandler) GetPayMasterData(ctx common.Ctx, userOperation *UserOperation) (*PaymasterData, error) {
	err := userOperation.Validate()
	if err != nil {
		ctx.Log.Errorf("UserOperation Validate error: %v", err)
		return nil, errors.New(int(marketcenterPb.ErrorCode_INVALID_USER_OPERATION), "INVALID_USER_OPERATION", err.Error())
	}

	estimatePaymaster, estimatePaymasterData, err := h.assetRepo.GetPaymasterStubData(ctx, userOperation)
	if err != nil {
		ctx.Log.Errorf("GetPaymasterStubData error: %+v", err)
		return nil, errors.New(int(marketcenterPb.ErrorCode_ALCHEMY), "ALCHEMY_ERROR", "get paymaster stub data error: "+err.Error())
	}
	userOperation.Paymaster = estimatePaymaster
	userOperation.PaymasterData = estimatePaymasterData

	preVerificationGas, verificationGasLimit, callGasLimit, paymasterVerificationGasLimit, err := h.assetRepo.EstimateUserOperationGas(ctx, userOperation)
	if err != nil {
		// Fallback: use generous gas values when estimation fails.
		// Alchemy's eth_estimateUserOperationGas simulation does not persist
		// intermediate state changes in multi-call batches (e.g. approve + deposit,
		// setApprovalForAll + withdraw), causing execution reverts during estimation
		// even though the on-chain execution would succeed.
		ctx.Log.Warnf("EstimateUserOperationGas failed, using fallback gas values: %v", err)
		preVerificationGas = "0x100000"     // 1,048,576
		verificationGasLimit = "0x100000"   // 1,048,576
		callGasLimit = "0x300000"           // 3,145,728 (generous for approve + trade)
		paymasterVerificationGasLimit = "0x100000" // 1,048,576
	}
	userOperation.PreVerificationGas = preVerificationGas
	userOperation.VerificationGasLimit = verificationGasLimit
	userOperation.CallGasLimit = callGasLimit
	userOperation.PaymasterVerificationGasLimit = paymasterVerificationGasLimit
	userOperation.PaymasterPostOpGasLimit = "0x0"

	paymaster, paymasterData, err := h.assetRepo.GetPaymasterData(ctx, userOperation)
	if err != nil {
		ctx.Log.Errorf("GetPaymasterData error: %+v", err)
		return nil, errors.New(int(marketcenterPb.ErrorCode_ALCHEMY), "ALCHEMY_ERROR", "get paymaster data failed: "+err.Error())
	}
	return &PaymasterData{
		PaymasterAddress:              paymaster,
		PaymasterData:                 paymasterData,
		PaymasterVerificationGasLimit: paymasterVerificationGasLimit,
		PreVerificationGas:            preVerificationGas,
		VerificationGasLimit:          verificationGasLimit,
		CallGasLimit:                  callGasLimit,
	}, nil
}

func (h *AssetHandler) PlaceOrder(ctx common.Ctx, orderEntity *OrderEntity) (*OrderEntity, error) {

	lockKey := fmt.Sprintf(UserOperationLockKey, orderEntity.UID)
	_, ok, err := h.assetRepo.AcquireLock(ctx, lockKey, 10*time.Second)
	if err != nil {
		return nil, errors.New(int(marketcenterPb.ErrorCode_REDIS), "REDIS_ERROR", err.Error())
	}
	if !ok {
		return nil, ErrPlaceOrderTooQuick
	}

	err = orderEntity.Tx.UserOperation.Validate()
	if err != nil {
		ctx.Log.Errorf("UserOperation Validate error: %v", err)
		return nil, errors.New(int(marketcenterPb.ErrorCode_INVALID_USER_OPERATION), "INVALID_USER_OPERATION", err.Error())
	}

	calldata, ok := orderEntity.Tx.UserOperation.CallData.(string)
	if !ok {
		ctx.Log.Errorf("PlaceOrder CallData is not a string")
		return nil, errors.New(int(marketcenterPb.ErrorCode_INVALID_USER_OPERATION), "INVALID_USER_OPERATION", "CallData is not a string")
	}

	callDataMarketAddress, err := util.ExtractMarketContractFromCalldata(calldata)
	if err != nil {
		ctx.Log.Errorf("ExtractMarketContractFromCalldata error: %+v", err)
		return nil, errors.New(int(marketcenterPb.ErrorCode_INVALID_USER_OPERATION), "INVALID_USER_OPERATION", "invalid calldata: "+err.Error())
	}

	marketEntity, err := h.marketRepo.GetMarket(ctx, &market.MarketQuery{
		Address: callDataMarketAddress,
	})
	if err != nil {
		ctx.Log.Errorf("GetMarketByAddress error: %+v", err)
		return nil, errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}
	if marketEntity == nil || marketEntity.Address != callDataMarketAddress {
		return nil, errors.New(int(marketcenterPb.ErrorCode_INVALID_USER_OPERATION), "INVALID_USER_OPERATION", "invalid market address: "+callDataMarketAddress)
	}

	orderEntity.BaseTokenType = uint8(marketEntity.TokenType)
	orderEntity.Tx.BaseTokenType = uint8(marketEntity.TokenType)

	opHash, err := h.assetRepo.SendUserOperationToAlchemy(ctx, orderEntity.Tx.UserOperation)
	if err != nil {
		ctx.Log.Errorf("PlaceOrder SendUserOperationToAlchemy error: %+v", err)
		return nil, errors.New(int(marketcenterPb.ErrorCode_ALCHEMY), "ALCHEMY_ERROR", "SendUserOperationToAlchemy error:"+err.Error())
	}
	if opHash == "" {
		ctx.Log.Errorf("PlaceOrder SendUserOperationToAlchemy empty opHash")
		return nil, errors.New(int(marketcenterPb.ErrorCode_ALCHEMY), "ALCHEMY_ERROR", "response opHash is empty")
	}

	orderEntity.OpHash = opHash
	orderEntity.Tx.OpHash = opHash
	orderEntity.Tx.UID = orderEntity.UID
	orderEntity.UUID = util.GenerateUUID()

	err = h.assetRepo.ExecTx(ctx, func(ctx common.Ctx, db *gorm.DB) error {
		// TODO send_tx表也记录业务信息
		if err := h.assetRepo.CreateSendTx(ctx, orderEntity.Tx); err != nil {
			ctx.Log.Errorf("PlaceOrder CreateSendTx error: %+v", err)
			return errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
		}
		return nil
	})
	if err != nil {
		ctx.Log.Errorf("PlaceOrder ExecTx error: %v", err)
		return nil, err
	}

	return orderEntity, nil
}

// CTFSendUserOp sends a CTF UserOperation (for liquidity/redeem, no order entity needed).
func (h *AssetHandler) CTFSendUserOp(ctx common.Ctx, sendTx *SendTxEntity) (string, error) {
	lockKey := fmt.Sprintf(UserOperationLockKey, sendTx.UID)
	_, ok, err := h.assetRepo.AcquireLock(ctx, lockKey, 10*time.Second)
	if err != nil {
		return "", errors.New(int(marketcenterPb.ErrorCode_REDIS), "REDIS_ERROR", err.Error())
	}
	if !ok {
		return "", ErrPlaceOrderTooQuick
	}

	err = sendTx.UserOperation.Validate()
	if err != nil {
		ctx.Log.Errorf("CTFSendUserOp UserOperation Validate error: %v", err)
		return "", errors.New(int(marketcenterPb.ErrorCode_INVALID_USER_OPERATION), "INVALID_USER_OPERATION", err.Error())
	}

	opHash, err := h.assetRepo.SendUserOperationToAlchemy(ctx, sendTx.UserOperation)
	if err != nil {
		ctx.Log.Errorf("CTFSendUserOp SendUserOperationToAlchemy error: %+v", err)
		return "", errors.New(int(marketcenterPb.ErrorCode_ALCHEMY), "ALCHEMY_ERROR", "SendUserOperationToAlchemy error:"+err.Error())
	}
	if opHash == "" {
		return "", errors.New(int(marketcenterPb.ErrorCode_ALCHEMY), "ALCHEMY_ERROR", "response opHash is empty")
	}

	sendTx.OpHash = opHash
	err = h.assetRepo.CreateSendTx(ctx, sendTx)
	if err != nil {
		ctx.Log.Errorf("CTFSendUserOp CreateSendTx error: %+v", err)
		return "", errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}

	return opHash, nil
}

func (h *AssetHandler) WaitPlaceOrderUserOperationReceipt(ctx common.Ctx, orderEntity *OrderEntity) {

	timer := time.NewTimer(time.Second * 60)
	for {
		select {
		case <-timer.C:
			ctx.Log.Warnf("WaitPlaceOrderUserOperationReceipt timeout, opHash: %s, uid: %s", orderEntity.OpHash, orderEntity.UID)
			alarm.Lark.Send(fmt.Sprintf("WaitPlaceOrderUserOperationReceipt timeout, opHash: %s, uid: %s", orderEntity.OpHash, orderEntity.UID))
			return
		case <-ctx.Ctx.Done():
			ctx.Log.Warnf("WaitPlaceOrderUserOperationReceipt context done, opHash: %s, uid: %s", orderEntity.OpHash, orderEntity.UID)
			alarm.Lark.Send(fmt.Sprintf("WaitPlaceOrderUserOperationReceipt context done, opHash: %s, uid: %s", orderEntity.OpHash, orderEntity.UID))
			return
		default:
			result, err := h.assetRepo.GetUserOperationReceipt(ctx, orderEntity.OpHash)
			if err != nil {
				ctx.Log.Errorf("WaitPlaceOrderUserOperationReceipt GetUserOperationReceipt error: %+v", err)
				return
			}
			if result == nil || result.Interface() == nil || result.Get("receipt") == nil {
				time.Sleep(200 * time.Millisecond)
				continue
			}

			isSuccess := result.Get("success").MustBool()
			if isSuccess {
				txHash := result.Get("receipt").Get("transactionHash").MustString()

				err = h.assetRepo.ExecTx(ctx, func(ctx common.Ctx, db *gorm.DB) error {

					err = h.assetRepo.UpdateSendTxByOpHash(ctx, &SendTxEntity{
						OpHash: orderEntity.OpHash,
					}, map[string]interface{}{
						"tx_hash": txHash,
						"status":  SendTxStatusExecSuccess,
					})
					if err != nil {
						ctx.Log.Errorf("WaitPlaceOrderUserOperationReceipt UpdateSendTxByOpHash error: %+v", err)
						return err
					}

					orderEntity.TxHash = txHash
					orderEntity.Status = OrderStatusSuccess
					err = h.assetRepo.CreateOrUpdateOrder(ctx, orderEntity, []string{"op_hash", "tx_hash", "status", "min_receive_amount", "amount", "price"})
					if err != nil {
						ctx.Log.Errorf("WaitPlaceOrderUserOperationReceipt CreateOrUpdateOrder error: %+v", err)
						return err
					}
					return nil
				})
				if err != nil {
					ctx.Log.Errorf("WaitPlaceOrderUserOperationReceipt ExecTx error: %+v", err)
					return
				}
				return
			} else {
				reason := result.Get("reason")
				reasonStr := ""
				if reason != nil && reason.Interface() != nil {
					reasonBytes, _ := reason.MarshalJSON()
					reasonStr = string(reasonBytes)
				}

				err = h.assetRepo.ExecTx(ctx, func(ctx common.Ctx, db *gorm.DB) error {
					err = h.assetRepo.UpdateSendTxByOpHash(ctx, &SendTxEntity{
						OpHash: orderEntity.OpHash,
					}, map[string]interface{}{
						"status":  SendTxStatusSendFailed,
						"err_msg": reasonStr,
					})
					if err != nil {
						ctx.Log.Errorf("WaitPlaceOrderUserOperationReceipt UpdateSendTxByOpHash error: %+v", err)
						return err
					}

					return nil
				})
				if err != nil {
					ctx.Log.Errorf("WaitPlaceOrderUserOperationReceipt ExecTx error: %+v", err)
					return
				}
				return
			}
		}
	}

}

func (h *AssetHandler) WaitClaimMarketResultUserOperationReceipt(ctx common.Ctx, userClaimResultEntity *UserClaimResultEntity) {

	timer := time.NewTimer(time.Second * 60)
	for {
		select {
		case <-timer.C:
			ctx.Log.Warnf("WaitClaimMarketResultUserOperationReceipt timeout, opHash: %s, uid: %s", userClaimResultEntity.OpHash, userClaimResultEntity.UID)
			alarm.Lark.Send(fmt.Sprintf("WaitClaimMarketResultUserOperationReceipt timeout, opHash: %s, uid: %s", userClaimResultEntity.OpHash, userClaimResultEntity.UID))
			return
		case <-ctx.Ctx.Done():
			ctx.Log.Warnf("WaitClaimMarketResultUserOperationReceipt context done, opHash: %s, uid: %s", userClaimResultEntity.OpHash, userClaimResultEntity.UID)
			alarm.Lark.Send(fmt.Sprintf("WaitClaimMarketResultUserOperationReceipt context done, opHash: %s, uid: %s", userClaimResultEntity.OpHash, userClaimResultEntity.UID))
			return
		default:
			result, err := h.assetRepo.GetUserOperationReceipt(ctx, userClaimResultEntity.OpHash)
			if err != nil {
				ctx.Log.Errorf("WaitClaimMarketResultUserOperationReceipt GetUserOperationReceipt error: %+v", err)
				return
			}
			if result == nil || result.Interface() == nil || result.Get("receipt") == nil {
				time.Sleep(200 * time.Millisecond)
				continue
			}

			isSuccess := result.Get("success").MustBool()
			if isSuccess {
				txHash := result.Get("receipt").Get("transactionHash").MustString()

				err = h.assetRepo.ExecTx(ctx, func(ctx common.Ctx, db *gorm.DB) error {

					err = h.assetRepo.UpdateSendTxByOpHash(ctx, &SendTxEntity{
						OpHash: userClaimResultEntity.OpHash,
					}, map[string]interface{}{
						"tx_hash": txHash,
						"status":  SendTxStatusExecSuccess,
					})
					if err != nil {
						ctx.Log.Errorf("WaitClaimMarketResultUserOperationReceipt UpdateSendTxByOpHash error: %+v", err)
						return err
					}

					userClaimResultEntity.TxHash = txHash
					userClaimResultEntity.Status = UserClaimResultStatusSuccess
					err = h.assetRepo.CreateOrUpdateUserClaimResult(ctx, userClaimResultEntity, []string{"op_hash", "tx_hash", "status"})
					if err != nil {
						ctx.Log.Errorf("WaitClaimMarketResultUserOperationReceipt CreateOrUpdateUserClaimResult error: %+v", err)
						return err
					}
					return nil
				})
				if err != nil {
					ctx.Log.Errorf("WaitClaimMarketResultUserOperationReceipt ExecTx error: %+v", err)
					return
				}
				return

			} else {
				reason := result.Get("reason")
				reasonStr := ""
				if reason != nil && reason.Interface() != nil {
					reasonBytes, _ := reason.MarshalJSON()
					reasonStr = string(reasonBytes)
				}

				err = h.assetRepo.ExecTx(ctx, func(ctx common.Ctx, db *gorm.DB) error {

					err = h.assetRepo.UpdateSendTxByOpHash(ctx, &SendTxEntity{
						OpHash: userClaimResultEntity.OpHash,
					}, map[string]interface{}{
						"status":  SendTxStatusSendFailed,
						"err_msg": reasonStr,
					})
					if err != nil {
						ctx.Log.Errorf("WaitClaimMarketResultUserOperationReceipt UpdateSendTxByOpHash error: %+v", err)
						return err
					}

					return nil
				})
				if err != nil {
					ctx.Log.Errorf("WaitClaimMarketResultUserOperationReceipt ExecTx error: %+v", err)
					return
				}
				return
			}
		}
	}
}

func (h *AssetHandler) WaitTransferBaseTokenUserOperationReceipt(ctx common.Ctx, userTransferTokensEntity *UserTransferTokensEntity) {

	timer := time.NewTimer(time.Second * 60)
	for {
		select {
		case <-timer.C:
			ctx.Log.Warnf("WaitTransferBaseTokenUserOperationReceipt timeout, opHash: %s, uid: %s", userTransferTokensEntity.OpHash, userTransferTokensEntity.UID)
			alarm.Lark.Send(fmt.Sprintf("WaitTransferBaseTokenUserOperationReceipt timeout, opHash: %s, uid: %s", userTransferTokensEntity.OpHash, userTransferTokensEntity.UID))
			return
		case <-ctx.Ctx.Done():
			alarm.Lark.Send(fmt.Sprintf("WaitTransferBaseTokenUserOperationReceipt context done, opHash: %s, uid: %s", userTransferTokensEntity.OpHash, userTransferTokensEntity.UID))
			ctx.Log.Warnf("WaitTransferBaseTokenUserOperationReceipt context done, opHash: %s, uid: %s", userTransferTokensEntity.OpHash, userTransferTokensEntity.UID)
			return
		default:
			result, err := h.assetRepo.GetUserOperationReceipt(ctx, userTransferTokensEntity.OpHash)
			if err != nil {
				ctx.Log.Errorf("WaitTransferBaseTokenUserOperationReceipt GetUserOperationReceipt error: %+v", err)
				return
			}
			if result == nil || result.Interface() == nil || result.Get("receipt") == nil {
				time.Sleep(200 * time.Millisecond)
				continue
			}

			isSuccess := result.Get("success").MustBool()
			if isSuccess {
				txHash := result.Get("receipt").Get("transactionHash").MustString()

				err = h.assetRepo.ExecTx(ctx, func(ctx common.Ctx, db *gorm.DB) error {
					err = h.assetRepo.UpdateSendTxByOpHash(ctx, &SendTxEntity{
						OpHash: userTransferTokensEntity.OpHash,
					}, map[string]interface{}{
						"tx_hash": txHash,
						"status":  SendTxStatusExecSuccess,
					})

					if err != nil {
						ctx.Log.Errorf("WaitTransferBaseTokenUserOperationReceipt ExecTx error: %+v", err)
						return err
					}

					userTransferTokensEntity.TxHash = txHash
					userTransferTokensEntity.Status = UserTransferTokensStatusSuccess
					err = h.assetRepo.CreateUserTransferTokens(ctx, userTransferTokensEntity)
					if err != nil {
						ctx.Log.Errorf("WaitTransferBaseTokenUserOperationReceipt CreateOrUpdateUserTransferTokens error: %+v", err)
						return err
					}
					return nil
				})
				if err != nil {
					ctx.Log.Errorf("WaitTransferBaseTokenUserOperationReceipt ExecTx error: %+v", err)
					return
				}
				return
			} else {
				reason := result.Get("reason")
				reasonStr := ""
				if reason != nil && reason.Interface() != nil {
					reasonBytes, _ := reason.MarshalJSON()
					reasonStr = string(reasonBytes)
				}

				err = h.assetRepo.ExecTx(ctx, func(ctx common.Ctx, db *gorm.DB) error {
					err = h.assetRepo.UpdateSendTxByOpHash(ctx, &SendTxEntity{
						OpHash: userTransferTokensEntity.OpHash,
					}, map[string]interface{}{
						"status":  SendTxStatusSendFailed,
						"err_msg": reasonStr,
					})
					if err != nil {
						ctx.Log.Errorf("WaitTransferBaseTokenUserOperationReceipt UpdateSendTxByOpHash error: %+v", err)
						return err
					}
					return nil
				})
				if err != nil {
					ctx.Log.Errorf("WaitTransferBaseTokenUserOperationReceipt ExecTx error: %+v", err)
					return
				}
				return
			}
		}
	}
}

func (h *AssetHandler) ClaimMarketResult(ctx common.Ctx, userClaimResultEntity *UserClaimResultEntity) (*UserClaimResultEntity, error) {

	marketEntity, err := h.marketRepo.GetMarket(ctx, &market.MarketQuery{
		Address: userClaimResultEntity.MarketAddress,
	})
	if err != nil {
		ctx.Log.Errorf("GetMarketByAddress error: %+v", err)
		return nil, errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}
	if marketEntity == nil || marketEntity.Address != userClaimResultEntity.MarketAddress {
		return nil, errors.New(int(marketcenterPb.ErrorCode_INVALID_USER_OPERATION), "INVALID_USER_OPERATION", "invalid market address: "+userClaimResultEntity.MarketAddress)
	}

	userTokenBalanceEntities, err := h.assetRepo.GetUserTokenBalance(ctx, &UserTokenBalanceQuery{
		UID:           userClaimResultEntity.UID,
		MarketAddress: userClaimResultEntity.MarketAddress,
		NoZero:        true,
	})
	if err != nil {
		return nil, errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}
	hasWinOption := false
	loseOptionAddressList := make([]string, 0)
	for _, userTokenBalanceEntity := range userTokenBalanceEntities {
		if userTokenBalanceEntity.TokenAddress == marketEntity.Result {
			hasWinOption = true
		} else {
			loseOptionAddressList = append(loseOptionAddressList, userTokenBalanceEntity.TokenAddress)
		}
	}

	if len(loseOptionAddressList) > 0 {
		err = h.assetRepo.UpdateUserTokenBalanceIsClaimed(ctx, userClaimResultEntity.UID, loseOptionAddressList)
		if err != nil {
			ctx.Log.Errorf("ClaimMarketResult UpdateUserTokenBalanceIsClaimed error: %+v", err)
			return nil, errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
		}
	}

	if !hasWinOption {
		return userClaimResultEntity, nil
	}

	lockKey := fmt.Sprintf(UserOperationLockKey, userClaimResultEntity.UID)
	_, ok, err := h.assetRepo.AcquireLock(ctx, lockKey, 10*time.Second)
	if err != nil {
		return nil, errors.New(int(marketcenterPb.ErrorCode_REDIS), "REDIS_ERROR", err.Error())
	}
	if !ok {
		return nil, ErrPlaceOrderTooQuick
	}

	// TODO 校验calldata 调用的函数 to的合约地址
	err = userClaimResultEntity.Tx.UserOperation.Validate()
	if err != nil {
		ctx.Log.Errorf("UserOperation Validate error: %v", err)
		return nil, errors.New(int(marketcenterPb.ErrorCode_INVALID_USER_OPERATION), "INVALID_USER_OPERATION", err.Error())
	}

	userClaimResultEntity.BaseTokenType = uint8(marketEntity.TokenType)
	userClaimResultEntity.Tx.BaseTokenType = uint8(marketEntity.TokenType)

	opHash, err := h.assetRepo.SendUserOperationToAlchemy(ctx, userClaimResultEntity.Tx.UserOperation)
	if err != nil {
		ctx.Log.Errorf("ClaimMarketResult SendUserOperationToAlchemy error: %+v", err)
		return nil, errors.New(int(marketcenterPb.ErrorCode_ALCHEMY), "ALCHEMY_ERROR", "SendUserOperationToAlchemy error:"+err.Error())
	}

	userClaimResultEntity.OpHash = opHash
	userClaimResultEntity.Tx.OpHash = opHash
	userClaimResultEntity.Tx.UID = userClaimResultEntity.UID
	userClaimResultEntity.UUID = util.GenerateUUID()

	err = h.assetRepo.ExecTx(ctx, func(ctx common.Ctx, db *gorm.DB) error {
		if err := h.assetRepo.CreateSendTx(ctx, userClaimResultEntity.Tx); err != nil {
			ctx.Log.Errorf("ClaimMarketResult CreateSendTx error: %+v", err)
			return err
		}
		return nil
	})
	if err != nil {
		ctx.Log.Errorf("ClaimMarketResult ExecTx error: %v", err)
		return nil, errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}

	return userClaimResultEntity, nil
}

func (h *AssetHandler) TransferBaseToken(ctx common.Ctx, userTransferTokensEntity *UserTransferTokensEntity) (*UserTransferTokensEntity, error) {

	err := userTransferTokensEntity.Tx.UserOperation.Validate()
	if err != nil {
		ctx.Log.Errorf("UserOperation Validate error: %v", err)
		return nil, errors.New(int(marketcenterPb.ErrorCode_INVALID_USER_OPERATION), "INVALID_USER_OPERATION", err.Error())
	}

	opHash, err := h.assetRepo.SendUserOperationToAlchemy(ctx, userTransferTokensEntity.Tx.UserOperation)
	if err != nil {
		ctx.Log.Errorf("TransferBaseToken SendUserOperationToAlchemy error: %+v", err)
		return nil, errors.New(int(marketcenterPb.ErrorCode_ALCHEMY), "ALCHEMY_ERROR", "SendUserOperationToAlchemy error:"+err.Error())
	}

	userTransferTokensEntity.OpHash = opHash
	userTransferTokensEntity.Tx.OpHash = opHash
	userTransferTokensEntity.Tx.UID = userTransferTokensEntity.UID
	userTransferTokensEntity.UUID = util.GenerateUUID()

	err = h.assetRepo.ExecTx(ctx, func(ctx common.Ctx, db *gorm.DB) error {
		if err := h.assetRepo.CreateSendTx(ctx, userTransferTokensEntity.Tx); err != nil {
			ctx.Log.Errorf("TransferBaseToken CreateSendTx error: %+v", err)
			return err
		}
		return nil
	})
	if err != nil {
		ctx.Log.Errorf("TransferBaseToken ExecTx error: %v", err)
		return nil, errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}

	return userTransferTokensEntity, nil
}

func (h *AssetHandler) UpdateOrderPrice(ctx common.Ctx, orderEntity *OrderEntity) error {
	err := h.assetRepo.UpdateOrderByTxHash(ctx, orderEntity, map[string]interface{}{
		"deal_price": orderEntity.DealPrice,
	})
	if err != nil {
		ctx.Log.Errorf("UpdateOrderPrice error: %+v", err)
		return errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}
	return nil
}

func (h *AssetHandler) GetUserTokenBalanceWithTotal(ctx common.Ctx, query *UserTokenBalanceQuery) ([]*UserTokenBalanceEntity, int64, error) {
	userTokenBalanceEntities, total, err := h.assetRepo.GetUserTokenBalanceWithTotal(ctx, query)
	if err != nil {
		return nil, 0, err
	}
	return userTokenBalanceEntities, total, nil
}

func (h *AssetHandler) GetOrdersWithTotal(ctx common.Ctx, query *OrderQuery) ([]*OrderEntity, int64, error) {
	orderEntities, total, err := h.assetRepo.GetOrdersWithTotal(ctx, query)
	if err != nil {
		return nil, 0, errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}
	return orderEntities, total, nil
}

func (h *AssetHandler) GetOrders(ctx common.Ctx, query *OrderQuery) ([]*OrderEntity, error) {
	orderEntities, err := h.assetRepo.GetOrders(ctx, query)
	if err != nil {
		return nil, errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}
	return orderEntities, nil
}

func (h *AssetHandler) GetUserTokenBalance(ctx common.Ctx, query *UserTokenBalanceQuery) ([]*UserTokenBalanceEntity, error) {
	userTokenBalanceEntities, err := h.assetRepo.GetUserTokenBalance(ctx, query)
	if err != nil {
		return nil, errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}
	return userTokenBalanceEntities, nil
}

func (h *AssetHandler) ProcessMarketDepositEventInAssetHandler(ctx common.Ctx, req *marketcenterPb.ProcessMarketDepositOrWithdrawEventRequest) error {
	// 1. 更新order订单  实际成交价格与数量  (TODO status->succ ?)
	// 2. 更新用户 user_token_balance表总余额 (balance) 买入所以还要更新 avg_buy_price 字段
	// 3. 更新用户该市场持仓总价值

	userOptionOutBalance, err := decimal.NewFromString(req.UserOptionTokenBalance)
	if err != nil {
		return errors.New(int(marketcenterPb.ErrorCode_PARAM), "PARAM_ERROR", "user option token balance is not a valid decimal")
	}

	// amountIn 投入的资产代币数量
	amountIn, err := decimal.NewFromString(req.AmountIn)
	if err != nil {
		return errors.New(int(marketcenterPb.ErrorCode_PARAM), "PARAM_ERROR", "amount in is not a valid decimal")
	}
	// amountOut 获取的条件代币数量
	amountOut, err := decimal.NewFromString(req.AmountOut)
	if err != nil {
		return errors.New(int(marketcenterPb.ErrorCode_PARAM), "PARAM_ERROR", "amount out is not a valid decimal")
	}
	buyPirce := amountIn.Div(amountOut).Mul(decimal.New(1, int32(req.Decimal)))

	userTokenBalanceEntities, err := h.assetRepo.GetUserTokenBalance(ctx, &UserTokenBalanceQuery{
		UID:          req.Uid,
		TokenAddress: req.UserOptionTokenAddress,
	})
	if err != nil {
		return err
	}

	// 这里biz层不加事务 放到service层加事务

	err = h.assetRepo.CreateOrUpdateOrder(ctx, &OrderEntity{
		UUID:           util.GenerateUUID(),
		UID:            req.Uid,
		OptionAddress:  req.UserOptionTokenAddress,
		MarketAddress:  req.MarketAddress,
		BaseTokenType:  uint8(req.BaseTokenType),
		Side:           OrderSideBuy,
		EventProcessed: ProcessedYes,

		TxHash: req.TxHash,
		Status: OrderStatusSuccess,

		DealPrice:     buyPirce,
		Amount:        amountIn,
		ReceiveAmount: amountOut,
	}, []string{"tx_hash", "status", "option_address", "market_address", "deal_price", "amount", "receive_amount", "event_processed"})
	if err != nil {
		ctx.Log.Errorf("CreateOrUpdateOrder error: %+v", err)
		return errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}

	if len(userTokenBalanceEntities) > 0 && userTokenBalanceEntities[0].UID == req.Uid {
		userTokenBalanceEntity := userTokenBalanceEntities[0]

		oldAvgBuyPrice := userTokenBalanceEntity.AvgBuyPrice // 没算精度信息（1次）
		oldBalance := userTokenBalanceEntity.Balance         // 没算精度信息（1次）
		oldTotalValue := oldAvgBuyPrice.Mul(oldBalance)      // 没算精度信息（2次）
		newTotalValue := buyPirce.Mul(amountOut)             // 没算精度信息（2次）
		totalValue := oldTotalValue.Add(newTotalValue)       // 没算精度信息（2次）
		// newBalance := userOptionOutBalance
		newBalance := oldBalance.Add(amountOut)      // 没算精度信息（1次）
		newAvgBuyPrice := totalValue.Div(newBalance) // 没算精度信息（1次）

		// CTF option tokens are ERC1155, on-chain balance query returns 0.
		// Use computed balance (oldBalance + amountOut) when on-chain balance is zero.
		if userOptionOutBalance.IsZero() {
			userTokenBalanceEntity.Balance = newBalance
		} else {
			userTokenBalanceEntity.Balance = userOptionOutBalance // 没算精度信息（1次）
		}
		userTokenBalanceEntity.AvgBuyPrice = newAvgBuyPrice   // 没算精度信息（1次）
		err = h.assetRepo.UpdateUserTokenBalanceByUidAndTokenAddress(ctx, userTokenBalanceEntity, map[string]interface{}{
			"balance":       userTokenBalanceEntity.Balance,
			"avg_buy_price": userTokenBalanceEntity.AvgBuyPrice,
			"block_number":  req.BlockNumber,
		})
		if err != nil {
			ctx.Log.Errorf("UpdateUserTokenBalanceByUidAndTokenAddress error: %+v", err)
			return errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
		}

	} else {
		// CTF option tokens are ERC1155, on-chain balance query returns 0.
		// Use amountOut (shares received) as initial balance when on-chain balance is zero.
		initBalance := userOptionOutBalance
		if initBalance.IsZero() {
			initBalance = amountOut
		}
		userTokenBalanceEntity := &UserTokenBalanceEntity{
			UID:           req.Uid,
			TokenAddress:  req.UserOptionTokenAddress,
			MarketAddress: req.MarketAddress,
			Balance:       initBalance, // 没算精度信息1次
			AvgBuyPrice:   buyPirce,             // 没算精度信息1次
			BlockNumber:   req.BlockNumber,
			Decimal:       uint8(req.Decimal),
			BaseTokenType: uint8(req.BaseTokenType),
			Type:          uint8(TypeUserTokenBalanceOption),
		}
		err = h.assetRepo.CreateUserTokenBalance(ctx, userTokenBalanceEntity)
		if err != nil {
			ctx.Log.Errorf("CreateUserTokenBalance error: %+v", err)
			return errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
		}
	}

	/*
		err = h.assetRepo.CreateOrUpdateUserMarketPosition(ctx, &UserMarketPositionEntity{
			UID:           req.Uid,
			MarketAddress: req.MarketAddress,
			BaseTokenType: uint8(req.BaseTokenType),
			TotalValue:    amountIn,
		})
		if err != nil {
			ctx.Log.Errorf("CreateOrUpdateUserMarketPosition error: %+v", err)
			return base.ErrInternal.WithMetadata(map[string]string{"error": err.Error()})
		}
	*/

	return nil
}

func (h *AssetHandler) ProcessMarketWithdrawEventInAssetHandler(ctx common.Ctx, req *marketcenterPb.ProcessMarketDepositOrWithdrawEventRequest) error {
	// 1. 更新order订单  实际成交价格与数量 卖出所以还要更新 pnl 字段 (TODO status->succ ?)
	// 2. 更新用户 user_token_balance表总余额 (balance) 卖出 avg_buy_price 字段保持不变
	// 3. 更新用户该市场持仓总价值

	userOptionInBalance, err := decimal.NewFromString(req.UserOptionTokenBalance)
	if err != nil {
		return errors.New(int(marketcenterPb.ErrorCode_PARAM), "PARAM_ERROR", "user option token balance is not a valid decimal")
	}

	amountIn, err := decimal.NewFromString(req.AmountIn)
	if err != nil {
		return errors.New(int(marketcenterPb.ErrorCode_PARAM), "PARAM_ERROR", "amount in is not a valid decimal")
	}

	amountOut, err := decimal.NewFromString(req.AmountOut)
	if err != nil {
		return errors.New(int(marketcenterPb.ErrorCode_PARAM), "PARAM_ERROR", "amount out is not a valid decimal")
	}

	sellPirce := amountOut.Div(amountIn).Mul(decimal.New(1, int32(req.Decimal)))

	userTokenBalanceEntities, err := h.assetRepo.GetUserTokenBalance(ctx, &UserTokenBalanceQuery{
		UID:          req.Uid,
		TokenAddress: req.UserOptionTokenAddress,
	})
	if err != nil {
		return errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}
	// 不可能卖出时 用户user_token_balance还不存在 直接报错
	if len(userTokenBalanceEntities) == 0 || userTokenBalanceEntities[0].UID != req.Uid {
		ctx.Log.Errorf("user token balance not found")
		return errors.New(int(marketcenterPb.ErrorCode_PARAM), "PARAM_ERROR", "user token balance not found")
	}

	// 这里biz层不加事务 放到service层加事务
	userTokenBalanceEntity := userTokenBalanceEntities[0]
	// oldBalance := userTokenBalanceEntity.Balance
	oldPrice := userTokenBalanceEntity.AvgBuyPrice

	priceChange := sellPirce.Sub(oldPrice)
	pnl := priceChange.Mul(amountIn).Div(decimal.New(1, int32(userTokenBalanceEntity.Decimal)))

	err = h.assetRepo.CreateOrUpdateOrder(ctx, &OrderEntity{
		UUID:          util.GenerateUUID(),
		UID:           req.Uid,
		OptionAddress: req.UserOptionTokenAddress,
		MarketAddress: req.MarketAddress,
		BaseTokenType: uint8(req.BaseTokenType),
		Side:          OrderSideSell,

		TxHash: req.TxHash,
		Status: OrderStatusSuccess,

		EventProcessed: ProcessedYes,
		DealPrice:      sellPirce,
		Amount:         amountIn,
		ReceiveAmount:  amountOut,
		Pnl:            pnl,
	}, []string{"tx_hash", "status", "option_address", "market_address", "deal_price", "amount", "receive_amount", "pnl", "event_processed"})
	if err != nil {
		ctx.Log.Errorf("CreateOrUpdateOrder error: %+v", err)
		return errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}

	// CTF option tokens are ERC1155, on-chain balance query returns 0.
	// Use computed balance (oldBalance - amountIn) when on-chain balance is zero.
	withdrawBalance := userOptionInBalance
	if withdrawBalance.IsZero() {
		withdrawBalance = userTokenBalanceEntity.Balance.Sub(amountIn)
		if withdrawBalance.IsNegative() {
			withdrawBalance = decimal.Zero
		}
	}
	err = h.assetRepo.UpdateUserTokenBalanceByUidAndTokenAddress(ctx, userTokenBalanceEntity, map[string]interface{}{
		"balance":      withdrawBalance, // 没算精度信息（1次）
		"block_number": req.BlockNumber,
	})
	if err != nil {
		ctx.Log.Errorf("UpdateUserTokenBalanceByUidAndTokenAddress error: %+v", err)
		return errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}

	/*
		err = h.assetRepo.UpdateUserMarketPositionDecrease(ctx, &UserMarketPositionEntity{
			UID:           req.Uid,
			MarketAddress: req.MarketAddress,
			BaseTokenType: uint8(req.BaseTokenType),
			TotalValue:    amountOut, // 没算精度信息（1次）
		})
		if err != nil {
			ctx.Log.Errorf("UpdateUserMarketPositionDecrease error: %+v", err)
			return base.ErrInternal.WithMetadata(map[string]string{"error": err.Error()})
		}
	*/
	return nil
}

func (h *AssetHandler) ProcessMarketClaimResultEventInAssetHandler(ctx common.Ctx, req *marketcenterPb.ProcessMarketClaimResultEventRequest) error {
	amount, err := decimal.NewFromString(req.Amount)
	if err != nil {
		return errors.New(int(marketcenterPb.ErrorCode_PARAM), "PARAM_ERROR", "amount is not a valid decimal")
	}
	optionBalance, err := decimal.NewFromString(req.OptionBalance)
	if err != nil {
		return errors.New(int(marketcenterPb.ErrorCode_PARAM), "PARAM_ERROR", "option balance is not a valid decimal")
	}

	err = h.assetRepo.CreateOrUpdateUserClaimResult(ctx, &UserClaimResultEntity{
		UUID:           util.GenerateUUID(),
		UID:            req.Uid,
		MarketAddress:  req.MarketAddress,
		OptionAddress:  req.OptionAddress,
		Amount:         amount,
		BaseTokenType:  uint8(req.BaseTokenType),
		Status:         UserClaimResultStatusSuccess,
		EventProcessed: ProcessedYes,
		TxHash:         req.TxHash,
	}, []string{"amount", "status", "event_processed", "tx_hash"})
	if err != nil {
		ctx.Log.Errorf("CreateOrUpdateUserClaimResult error: %+v", err)
		return errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}

	err = h.assetRepo.UpdateUserTokenBalanceByUidAndTokenAddress(ctx, &UserTokenBalanceEntity{
		UID:          req.Uid,
		TokenAddress: req.OptionAddress,
	}, map[string]interface{}{
		"balance":      optionBalance,
		"block_number": req.BlockNumber,
	})
	if err != nil {
		ctx.Log.Errorf("UpdateUserTokenBalanceByUidAndTokenAddress error: %+v", err)
		return errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}

	/*
		err = h.assetRepo.UpdateUserMarketPositionDecrease(ctx, &UserMarketPositionEntity{
			UID:           req.Uid,
			MarketAddress: req.MarketAddress,
			BaseTokenType: uint8(req.BaseTokenType),
			TotalValue:    amount,
		})
		if err != nil {
			ctx.Log.Errorf("UpdateUserMarketPositionDecrease error: %+v", err)
			return base.ErrInternal.WithMetadata(map[string]string{"error": err.Error()})
		}
	*/
	return nil
}

func (h *AssetHandler) GetUserAssetValue(ctx common.Ctx, query *UserAssetValueQuery) (*UserAssetValueEntity, error) {
	userAssetValueEntity, err := h.assetRepo.GetUserAssetValue(ctx, query)
	if err != nil {
		ctx.Log.Errorf("GetUserAssetValue error: %+v", err)
		return nil, errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}
	return userAssetValueEntity, nil
}

func (h *AssetHandler) GetUserAssetHistory(ctx common.Ctx, uid string, baseTokenType uint8, timeRange string) ([]*UserAssetValueEntity, error) {
	userAssetValueEntities, err := h.assetRepo.GetUserAssetHistory(ctx, uid, baseTokenType, timeRange)
	if err != nil {
		ctx.Log.Errorf("GetUserAssetHistory error: %+v", err)
		return nil, errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}
	return userAssetValueEntities, nil
}

func (h *AssetHandler) MintPointsToUser(ctx common.Ctx, uid string, userAddress string, txSource uint8, amount *big.Int) (string, error) {

	if txSource != TxSourceMintInitPoins && txSource != TxSourceMintInviteRewardPoints && txSource != TxSourceMintTaskRewardPoints {
		return "", errors.New(int(marketcenterPb.ErrorCode_PARAM), "PARAM_ERROR", "source is not valid")
	}

	txHash, err := h.assetRepo.MintERC20Token(ctx, h.confCustom.AssetTokens.Points.Address, userAddress, amount)
	if err != nil {
		ctx.Log.Errorf("MintPointsToNewUser MintERC20Token error: %+v", err)
		return "", errors.New(int(marketcenterPb.ErrorCode_ALCHEMY), "ALCHEMY_ERROR", "mint points to user failed: "+err.Error())
	}

	err = h.assetRepo.CreateSendTx(ctx, &SendTxEntity{
		UID:           uid,
		TxHash:        txHash,
		Source:        txSource,
		Status:        SendTxStatusSending,
		Chain:         h.confCustom.Chain,
		Type:          TxTypeNormal,
		BaseTokenType: uint8(BaseTokenTypePoints),
	})
	if err != nil {
		ctx.Log.Errorf("MintPointsToNewUser CreateSendTx error: %+v", err)
		return "", errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}

	return txHash, nil
}

func (h *AssetHandler) WaitMintPointsReceipt(ctx common.Ctx, txHash string, userMintPointsEntity *UserMintPointsEntity) (bool, error) {
	timer := time.NewTimer(60 * time.Second)

	for {
		select {
		case <-timer.C:
			ctx.Log.Errorf("WaitMintPointsToNewUserReceipt timeout")
			alarm.Lark.Send(fmt.Sprintf("WaitMintPointsToNewUserReceipt timeout. user: %s, txHash: %s", userMintPointsEntity.UID, txHash))
			return false, errors.New(int(marketcenterPb.ErrorCode_INTERNAL), "TIMEOUT", "wait mint points receipt timeout")
		case <-ctx.Ctx.Done():
			ctx.Log.Errorf("WaitMintPointsToNewUserReceipt context done. user: %s, txHash: %s", userMintPointsEntity.UID, txHash)
			alarm.Lark.Send(fmt.Sprintf("WaitMintPointsToNewUserReceipt context done.user: %s, txHash: %s", userMintPointsEntity.UID, txHash))
			return false, errors.New(int(marketcenterPb.ErrorCode_INTERNAL), "INTERNAL", "context done")
		default:
			receipt, err := h.assetRepo.GetTransactionReceipt(ctx, txHash)
			if err != nil {
				// 如果是"not found"错误，说明交易还没有被确认，继续等待
				if strings.Contains(err.Error(), "not found") {
					time.Sleep(500 * time.Millisecond)
					continue
				}
				return false, err
			}
			if receipt == nil {
				time.Sleep(500 * time.Millisecond)
				continue
			}

			var status uint8
			if receipt.Status == types.ReceiptStatusSuccessful {
				status = SendTxStatusExecSuccess
			} else {
				status = SendTxStatusSendFailed
			}
			userMintPointsEntity.Status = status
			h.assetRepo.ExecTx(ctx, func(ctx common.Ctx, db *gorm.DB) error {
				err = h.assetRepo.UpdateSendTxByTxHash(ctx, &SendTxEntity{
					TxHash: txHash,
				}, map[string]interface{}{
					"status": status,
				})

				if err != nil {
					ctx.Log.Errorf("WaitMintPointsToNewUserReceipt UpdateSendTxByTxHash error: %+v", err)
					return err
				}

				err = h.assetRepo.CreateUserMintPoints(ctx, userMintPointsEntity)
				if err != nil {
					ctx.Log.Errorf("WaitMintPointsToNewUserReceipt CreateUserMintPoints error: %+v", err)
					return err
				}

				return nil
			})
			if err != nil {
				ctx.Log.Errorf("WaitMintPointsToNewUserReceipt UpdateSendTxByTxHash error: %+v", err)
				return false, err
			}
			if status == SendTxStatusSendFailed {
				return false, nil
			}
			return true, nil
		}
	}

}

func (h *AssetHandler) GetUserTokenBalancesByQueryItems(ctx common.Ctx, queryItems []*UserTokenBalanceQueryItem) ([]*UserTokenBalanceEntity, error) {
	userTokenBalanceEntities, err := h.assetRepo.GetUserTokenBalancesByQueryItems(ctx, queryItems)
	if err != nil {
		ctx.Log.Errorf("GetUserTokenBalancesByQueryItems error: %+v", err)
		return nil, errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}
	return userTokenBalanceEntities, nil
}

func (h *AssetHandler) CalculateUserAssetValue(ctx common.Ctx, uid string, assetAddress string, baseTokenType uint8) (*UserAssetValueEntity, error) {
	// 计算用户当前的
	//	1. 持仓总价值(t_user_token_balance, t_option_token_price)
	//  2. 该资产代币的余额(t_user_token_balance)
	//  3. pnl(已实现t_order ; 未实现 t_user_token_balance)

	userAssetValueEntity := &UserAssetValueEntity{
		UID:           uid,
		AssetAddress:  assetAddress,
		BaseTokenType: baseTokenType,
		Value:         decimal.Zero,
		Balance:       decimal.Zero,
		Portfolio:     decimal.Zero,
		Pnl:           decimal.Zero,
		Time:          time.Now(),

		PortfolioPnl: decimal.Zero,
	}

	userTokenBalanceEntities, err := h.assetRepo.GetUserTokenBalance(ctx, &UserTokenBalanceQuery{
		UID:            uid,
		BaseTokenType:  baseTokenType,
		Type:           TypeUserTokenBalanceOption,
		NoZero:         true,
		StatusNotEqual: UserTokenBalanceStatusEndLose,
	})
	if err != nil {
		return nil, errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}

	optionTokenPriceMap := make(map[string]decimal.Decimal)
	if len(userTokenBalanceEntities) > 0 {
		optionAddressList := make([]string, 0, len(userTokenBalanceEntities))
		for _, userTokenBalanceEntity := range userTokenBalanceEntities {
			optionAddressList = append(optionAddressList, userTokenBalanceEntity.TokenAddress)
		}

		optionTokenPriceEntities, err := h.marketRepo.GetLatestOptionPrice(ctx, optionAddressList)
		if err != nil {
			return nil, errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
		}

		for _, optionTokenPriceEntity := range optionTokenPriceEntities {
			optionTokenPriceMap[optionTokenPriceEntity.TokenAddress] = optionTokenPriceEntity.Price
		}
	}

	portfolio := decimal.Zero
	for _, userTokenBalanceEntity := range userTokenBalanceEntities {
		price := decimal.Zero
		if optionTokenPrice, ok := optionTokenPriceMap[userTokenBalanceEntity.TokenAddress]; ok {
			price = optionTokenPrice // 没算精度信息（1次）
		}
		portfolio = portfolio.Add(price.Mul(userTokenBalanceEntity.Balance).Div(decimal.New(1, int32(userTokenBalanceEntity.Decimal)))) // 没算精度信息（1次）
	}
	userAssetValueEntity.Portfolio = portfolio

	//-----
	assetTokenBalanceEntities, err := h.assetRepo.GetUserTokenBalance(ctx, &UserTokenBalanceQuery{
		UID:           uid,
		BaseTokenType: baseTokenType,
		Type:          TypeUserTokenBalanceBaseAsset,
		NoZero:        true,
	})
	if err != nil {
		return nil, errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}

	if len(assetTokenBalanceEntities) > 0 {
		userAssetValueEntity.Balance = assetTokenBalanceEntities[0].Balance
	}

	userAssetValueEntity.Value = userAssetValueEntity.Portfolio.Add(userAssetValueEntity.Balance)

	// 持仓的pnl
	portfolioPnl := decimal.Zero
	for _, userTokenBalanceEntity := range userTokenBalanceEntities {
		nowPrice := decimal.Zero
		if optionTokenPrice, ok := optionTokenPriceMap[userTokenBalanceEntity.TokenAddress]; ok {
			nowPrice = optionTokenPrice // 没算精度信息（1次）
		}
		avgBuyPrice := userTokenBalanceEntity.AvgBuyPrice                                                                                                         // 没算精度信息（1次）
		portfolioPnl = portfolioPnl.Add(nowPrice.Sub(avgBuyPrice).Mul(userTokenBalanceEntity.Balance).Div(decimal.New(1, int32(userTokenBalanceEntity.Decimal)))) // 没算精度信息（1次）
	}

	orderEntities, _, err := h.assetRepo.GetOrdersWithTotal(ctx, &OrderQuery{
		UID:           uid,
		BaseTokenType: baseTokenType,
		Side:          OrderSideSell,
		Status:        OrderStatusSuccess,
	})
	if err != nil {
		return nil, errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}

	sellPnl := decimal.Zero
	for _, orderEntity := range orderEntities {
		sellPnl = sellPnl.Add(orderEntity.Pnl)
	}
	pnl := portfolioPnl.Add(sellPnl)

	userAssetValueEntity.PortfolioPnl = portfolioPnl
	userAssetValueEntity.SelfPnl = sellPnl
	userAssetValueEntity.Pnl = pnl
	return userAssetValueEntity, nil
}

func (h *AssetHandler) BatchCreateUserAssetValue(ctx common.Ctx, userAssetValueEntities []*UserAssetValueEntity) error {
	err := h.assetRepo.BatchCreateUserAssetValue(ctx, userAssetValueEntities)
	if err != nil {
		return errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}
	return nil
}

func (h *AssetHandler) ProcessMarketEndInAssetHandler(ctx common.Ctx, marketAddress string, winOptionAddress string) error {
	if err := h.assetRepo.UpdateUserPositionTokenEndStatus(ctx, marketAddress, winOptionAddress); err != nil {
		return errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}

	/*
		if err := h.assetRepo.UpdateUserMarketPositionStatus(ctx, marketAddress, winOptionAddress); err != nil {
			return errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
		}
	*/
	return nil
}

func (h *AssetHandler) GetMarketEndUserPositionsMap(ctx common.Ctx, marketAddress string, winOptionAddress string) (map[string]*UserTokenBalanceEntity, error) {
	pageSize := 100
	page := 1
	m := make(map[string]*UserTokenBalanceEntity)
	for {

		userTokenBalanceEntities, _, err := h.assetRepo.GetUserTokenBalanceWithTotal(ctx, &UserTokenBalanceQuery{
			MarketAddress: marketAddress,
			Type:          TypeUserTokenBalanceOption,
			NoZero:        true,
			BaseQuery: base.BaseQuery{
				Limit:  int32(pageSize),
				Offset: int32(pageSize * (page - 1)),
			},
		})
		if err != nil {
			return nil, errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
		}

		for _, userTokenBalanceEntity := range userTokenBalanceEntities {

			if value, ok := m[userTokenBalanceEntity.UID]; ok && value.TokenAddress == winOptionAddress {
				continue
			}
			m[userTokenBalanceEntity.UID] = userTokenBalanceEntity
		}

		if len(userTokenBalanceEntities) < pageSize {
			break
		}
		page++
	}

	return m, nil

}

func (h *AssetHandler) GetUserTotalValue(ctx common.Ctx, query *UserTokenBalanceQuery) (decimal.Decimal, error) {
	totalValue, err := h.assetRepo.GetUserTotalValue(ctx, query)
	if err != nil {
		return decimal.Zero, errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}
	return totalValue, nil
}

func (h *AssetHandler) GetUserMarketPositionsByValue(ctx common.Ctx, query *UserTokenBalanceQuery) ([]*MarketValue, int64, error) {
	marketValues, total, err := h.assetRepo.GetUserMarketPositionsByValue(ctx, query)
	if err != nil {
		return nil, 0, errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}
	return marketValues, total, nil
}

func (h *AssetHandler) IsClaimedMarketResult(ctx common.Ctx, uid string, marketAddress string) (bool, error) {
	userClaimResultEntity, err := h.assetRepo.GetUserClaimResult(ctx, &UserClaimResultQuery{
		UID:            uid,
		MarketAddress:  marketAddress,
		Status:         UserClaimResultStatusSuccess,
		EventProcessed: ProcessedYes,
	})
	if err != nil {
		return false, errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}
	if userClaimResultEntity == nil || userClaimResultEntity.UUID == "" {
		return false, nil
	}
	return true, nil
}

func (h *AssetHandler) GetUserMintPoints(ctx common.Ctx, query *UserMintPointsQuery) ([]*UserMintPointsEntity, error) {
	userMintPointsEntity, err := h.assetRepo.GetUserMintPoints(ctx, query)
	if err != nil {
		return nil, errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}
	return userMintPointsEntity, nil
}

func (h *AssetHandler) GetUserTransactions(ctx common.Ctx, query *SendTxQuery) ([]*marketcenterPb.GetUserTransactionsResponse_Transaction, int64, error) {
	sendTxEntities, total, err := h.assetRepo.GetSendTxsWithTotal(ctx, query)
	if err != nil {
		return nil, 0, errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}

	if len(sendTxEntities) == 0 {
		return []*marketcenterPb.GetUserTransactionsResponse_Transaction{}, 0, nil
	}

	// 按 source 分组收集 txHash
	buyTxHashes := []string{}
	sellTxHashes := []string{}
	claimTxHashes := []string{}
	mintTxHashes := []string{}
	transferTxHashes := []string{}

	for _, sendTx := range sendTxEntities {
		switch sendTx.Source {
		case TxSourceBuy:
			buyTxHashes = append(buyTxHashes, sendTx.TxHash)
		case TxSourceSell:
			sellTxHashes = append(sellTxHashes, sendTx.TxHash)
		case TxSourceUserClaim:
			claimTxHashes = append(claimTxHashes, sendTx.TxHash)
		case TxSourceMintInitPoins, TxSourceMintInviteRewardPoints, TxSourceMintTaskRewardPoints:
			mintTxHashes = append(mintTxHashes, sendTx.TxHash)
		case TxSourceTransferDeposit, TxSourceTransferWithdraw:
			transferTxHashes = append(transferTxHashes, sendTx.TxHash)
		}
	}

	// 批量查询详细信息
	orderMap := make(map[string]*OrderEntity)
	claimMap := make(map[string]*UserClaimResultEntity)
	mintMap := make(map[string]*UserMintPointsEntity)
	transferMap := make(map[string]*UserTransferTokensEntity)

	// 查询 order 信息
	if len(buyTxHashes) > 0 || len(sellTxHashes) > 0 {
		allOrderTxHashes := append(buyTxHashes, sellTxHashes...)
		orders, err := h.assetRepo.GetOrders(ctx, &OrderQuery{
			TxHashList: allOrderTxHashes,
		})
		if err != nil {
			ctx.Log.Errorf("GetOrdersByTxHashes error: %+v", err)
			return nil, 0, errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
		}
		for _, order := range orders {
			orderMap[order.TxHash] = order
		}
	}

	// 查询 claim 信息
	if len(claimTxHashes) > 0 {
		claims, err := h.assetRepo.GetUserClaimResults(ctx, &UserClaimResultQuery{
			TxHashList: claimTxHashes,
		})
		if err != nil {
			ctx.Log.Errorf("GetUserClaimResultsByTxHashes error: %+v", err)
			return nil, 0, errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
		}
		for _, claim := range claims {
			claimMap[claim.TxHash] = claim
		}
	}

	// 查询 mint 信息
	if len(mintTxHashes) > 0 {
		mints, err := h.assetRepo.GetUserMintPoints(ctx, &UserMintPointsQuery{
			TxHashList: mintTxHashes,
		})
		if err != nil {
			ctx.Log.Errorf("GetUserMintPointsByTxHashes error: %+v", err)
			return nil, 0, errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
		}
		for _, mint := range mints {
			mintMap[mint.TxHash] = mint
		}
	}

	// 查询 transfer 信息
	if len(transferTxHashes) > 0 {
		transfers, err := h.assetRepo.GetUserTransferTokens(ctx, &UserTransferTokensQuery{
			TxHashList: transferTxHashes,
		})
		if err != nil {
			ctx.Log.Errorf("GetUserTransferTokensByTxHashes error: %+v", err)
			return nil, 0, errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
		}
		for _, transfer := range transfers {
			transferMap[transfer.TxHash] = transfer
		}
	}

	// 收集需要查询的市场和选项地址
	marketAddresses := make(map[string]bool)
	optionAddresses := make(map[string]bool)

	for _, order := range orderMap {
		marketAddresses[order.MarketAddress] = true
		optionAddresses[order.OptionAddress] = true
	}
	for _, claim := range claimMap {
		marketAddresses[claim.MarketAddress] = true
		optionAddresses[claim.OptionAddress] = true
	}

	// 查询市场信息
	marketMap := make(map[string]*market.MarketEntity)
	optionMap := make(map[string]*market.OptionEntity)

	if len(marketAddresses) > 0 {
		marketAddressList := make([]string, 0, len(marketAddresses))
		for addr := range marketAddresses {
			marketAddressList = append(marketAddressList, addr)
		}

		markets, err := h.marketRepo.GetMarkets(ctx, &market.MarketQuery{
			AddressList: marketAddressList,
		})
		if err != nil {
			ctx.Log.Errorf("GetMarkets error: %+v", err)
			return nil, 0, errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
		}
		for _, m := range markets {
			marketMap[m.Address] = m
		}
	}

	if len(optionAddresses) > 0 {
		optionAddressList := make([]string, 0, len(optionAddresses))
		for addr := range optionAddresses {
			optionAddressList = append(optionAddressList, addr)
		}

		options, err := h.marketRepo.GetOptions(ctx, &market.OptionQuery{
			AddressList: optionAddressList,
		})
		if err != nil {
			ctx.Log.Errorf("GetOptions error: %+v", err)
			return nil, 0, errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
		}
		for _, opt := range options {
			optionMap[opt.Address] = opt
		}
	}

	// 组装返回数据
	transactions := make([]*marketcenterPb.GetUserTransactionsResponse_Transaction, 0, len(sendTxEntities))

	for _, sendTx := range sendTxEntities {
		transaction := &marketcenterPb.GetUserTransactionsResponse_Transaction{
			Uid:       sendTx.UID,
			Timestamp: uint32(sendTx.CreatedAt.Unix()),
			Status:    uint32(sendTx.Status),
			TxHash:    sendTx.TxHash,
		}

		switch sendTx.Source {
		case TxSourceBuy:
			transaction.Type = marketcenterPb.TxType_TX_TYPE_BUY
			transaction.Side = 1 // 转入
			if order, exists := orderMap[sendTx.TxHash]; exists {
				transaction.BaseTokenType = marketcenterPb.BaseTokenType(order.BaseTokenType)
				transaction.Amount = order.Amount.String()

				// 设置token地址和精度
				if order.BaseTokenType == BaseTokenTypePoints {
					transaction.TokenAddress = h.confCustom.AssetTokens.Points.Address
					transaction.Decimal = h.confCustom.AssetTokens.Points.Decimals
				} else {
					transaction.TokenAddress = h.confCustom.AssetTokens.Usdc.Address
					transaction.Decimal = h.confCustom.AssetTokens.Usdc.Decimals
				}

				buyData := &TxDataBuy{}
				if marketEntity, exists := marketMap[order.MarketAddress]; exists {
					buyData.MarketAddress = marketEntity.Address
					buyData.MarketName = marketEntity.Name
					buyData.MarketPicUrl = marketEntity.PicUrl
					buyData.MarketDescription = marketEntity.Description
				}
				if optionEntity, exists := optionMap[order.OptionAddress]; exists {
					buyData.OptionAddress = optionEntity.Address
					buyData.OptionName = optionEntity.Name
					buyData.OptionDescription = optionEntity.Description
				}

				buyDataJson, err := json.Marshal(buyData)
				if err != nil {
					ctx.Log.Errorf("Marshal buyData error: %+v", err)
					return nil, 0, errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
				}
				transaction.BizData = string(buyDataJson)
			}

		case TxSourceSell:
			transaction.Type = marketcenterPb.TxType_TX_TYPE_SELL
			transaction.Side = 2 // 转出
			if order, exists := orderMap[sendTx.TxHash]; exists {
				transaction.BaseTokenType = marketcenterPb.BaseTokenType(order.BaseTokenType)
				transaction.Amount = order.ReceiveAmount.String()

				// 设置token地址和精度
				if order.BaseTokenType == BaseTokenTypePoints {
					transaction.TokenAddress = h.confCustom.AssetTokens.Points.Address
					transaction.Decimal = h.confCustom.AssetTokens.Points.Decimals
				} else {
					transaction.TokenAddress = h.confCustom.AssetTokens.Usdc.Address
					transaction.Decimal = h.confCustom.AssetTokens.Usdc.Decimals
				}

				sellData := &TxDataSell{}
				if marketEntity, exists := marketMap[order.MarketAddress]; exists {
					sellData.MarketAddress = marketEntity.Address
					sellData.MarketName = marketEntity.Name
					sellData.MarketPicUrl = marketEntity.PicUrl
					sellData.MarketDescription = marketEntity.Description
				}
				if optionEntity, exists := optionMap[order.OptionAddress]; exists {
					sellData.OptionAddress = optionEntity.Address
					sellData.OptionName = optionEntity.Name
					sellData.OptionDescription = optionEntity.Description
				}

				sellDataJson, err := json.Marshal(sellData)
				if err != nil {
					ctx.Log.Errorf("Marshal sellData error: %+v", err)
					return nil, 0, errors.New(int(marketcenterPb.ErrorCode_INTERNAL), "INTERNAL_ERROR", err.Error())
				}
				transaction.BizData = string(sellDataJson)
			}

		case TxSourceUserClaim:
			transaction.Type = marketcenterPb.TxType_TX_TYPE_CLAIM
			transaction.Side = 1 // 转入
			if claim, exists := claimMap[sendTx.TxHash]; exists {
				transaction.BaseTokenType = marketcenterPb.BaseTokenType(claim.BaseTokenType)
				transaction.Amount = claim.Amount.String()

				// 设置token地址和精度
				if claim.BaseTokenType == BaseTokenTypePoints {
					transaction.TokenAddress = h.confCustom.AssetTokens.Points.Address
					transaction.Decimal = h.confCustom.AssetTokens.Points.Decimals
				} else {
					transaction.TokenAddress = h.confCustom.AssetTokens.Usdc.Address
					transaction.Decimal = h.confCustom.AssetTokens.Usdc.Decimals
				}

				claimData := &TxDataClaim{}
				if marketEntity, exists := marketMap[claim.MarketAddress]; exists {
					claimData.MarketAddress = marketEntity.Address
					claimData.MarketName = marketEntity.Name
					claimData.MarketPicUrl = marketEntity.PicUrl
					claimData.MarketDescription = marketEntity.Description
				}
				if optionEntity, exists := optionMap[claim.OptionAddress]; exists {
					claimData.OptionAddress = optionEntity.Address
					claimData.OptionName = optionEntity.Name
					claimData.OptionDescription = optionEntity.Description
				}

				claimDataJson, err := json.Marshal(claimData)
				if err != nil {
					ctx.Log.Errorf("Marshal claimData error: %+v", err)
					return nil, 0, errors.New(int(marketcenterPb.ErrorCode_INTERNAL), "INTERNAL_ERROR", err.Error())
				}
				transaction.BizData = string(claimDataJson)
			}

		case TxSourceMintInitPoins:
			transaction.Type = marketcenterPb.TxType_TX_TYPE_MINT_POINTS
			transaction.Side = 1 // 转入
			transaction.BaseTokenType = marketcenterPb.BaseTokenType_BASE_TOKEN_TYPE_POINTS
			transaction.TokenAddress = h.confCustom.AssetTokens.Points.Address
			transaction.Decimal = h.confCustom.AssetTokens.Points.Decimals

			if mint, exists := mintMap[sendTx.TxHash]; exists {
				transaction.Amount = mint.Amount.String()

			}

			mintData := &TxDataMintPoints{}
			mintDataJson, err := json.Marshal(mintData)
			if err != nil {
				ctx.Log.Errorf("Marshal mintData error: %+v", err)
				return nil, 0, errors.New(int(marketcenterPb.ErrorCode_INTERNAL), "INTERNAL_ERROR", err.Error())
			}
			transaction.BizData = string(mintDataJson)

		case TxSourceMintInviteRewardPoints:
			transaction.Type = marketcenterPb.TxType_TX_TYPE_MINT_INVITE_POINTS
			transaction.Side = 1 // 转入
			transaction.BaseTokenType = marketcenterPb.BaseTokenType_BASE_TOKEN_TYPE_POINTS
			transaction.TokenAddress = h.confCustom.AssetTokens.Points.Address
			transaction.Decimal = h.confCustom.AssetTokens.Points.Decimals

			mintInviteData := &TxDataMintInvitePoints{
				// TODO: 如果需要邀请用户信息，需要从其他地方查询
			}

			if mint, exists := mintMap[sendTx.TxHash]; exists {
				transaction.Amount = mint.Amount.String()
				mintInviteData.InvitedUid = mint.InviteUID

				invitee, err := h.userRepo.GetUser(ctx, &user.UserQuery{
					UID: mint.InviteUID,
				})
				if err != nil {
					ctx.Log.Errorf("GetUser error: %+v", err)
					return nil, 0, errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
				}
				if invitee != nil && invitee.UID != "" {
					mintInviteData.InvitedUid = invitee.UID
					mintInviteData.InvitedName = invitee.Name
					mintInviteData.InvitedAvator = invitee.Avatar
				}
			}

			mintInviteDataJson, err := json.Marshal(mintInviteData)
			if err != nil {
				ctx.Log.Errorf("Marshal mintInviteData error: %+v", err)
				return nil, 0, errors.New(int(marketcenterPb.ErrorCode_INTERNAL), "INTERNAL_ERROR", err.Error())
			}
			transaction.BizData = string(mintInviteDataJson)

		case TxSourceMintTaskRewardPoints:
			transaction.Type = marketcenterPb.TxType_TX_TYPE_MINT_TASK_REWARD_POINTS
			transaction.Side = 1 // 转入
			transaction.BaseTokenType = marketcenterPb.BaseTokenType_BASE_TOKEN_TYPE_POINTS
			transaction.TokenAddress = h.confCustom.AssetTokens.Points.Address
			transaction.Decimal = h.confCustom.AssetTokens.Points.Decimals

			mintTaskRewardData := &TxDataMintTaskRewardPoints{
				// TODO: 如果需要任务信息，需要从其他地方查询
			}
			if mint, exists := mintMap[sendTx.TxHash]; exists {
				transaction.Amount = mint.Amount.String()
				mintTaskRewardData.UserTaskUUID = mint.UserTaskUUID

				userTask, err := h.taskRepo.GetUserTask(ctx, &task.UserTaskQuery{
					UUID: mint.UserTaskUUID,
				})
				if err != nil {
					ctx.Log.Errorf("GetUserTask error: %+v", err)
					return nil, 0, errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
				}
				if userTask != nil && userTask.UUID != "" {
					task, err := h.taskRepo.GetTask(ctx, &task.TaskQuery{
						Key: userTask.TaskKey,
					})
					if err != nil {
						ctx.Log.Errorf("GetTask error: %+v", err)
						return nil, 0, errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
					}
					if task != nil && task.Key != "" {
						mintTaskRewardData.TaskUUID = task.UUID
						mintTaskRewardData.TaskKey = task.Key
						mintTaskRewardData.TaskName = task.Name
						mintTaskRewardData.Reward = task.Reward
					}
				}
			}

			mintTaskRewardDataJson, err := json.Marshal(mintTaskRewardData)
			if err != nil {
				ctx.Log.Errorf("Marshal mintTaskRewardData error: %+v", err)
				return nil, 0, errors.New(int(marketcenterPb.ErrorCode_INTERNAL), "INTERNAL_ERROR", err.Error())
			}
			transaction.BizData = string(mintTaskRewardDataJson)

		case TxSourceTransferDeposit:
			transaction.Type = marketcenterPb.TxType_TX_TYPE_DEPOSIT
			transaction.Side = 1 // 转入
			if transfer, exists := transferMap[sendTx.TxHash]; exists {
				transaction.BaseTokenType = marketcenterPb.BaseTokenType(transfer.BaseTokenType)
				transaction.TokenAddress = transfer.TokenAddress
				transaction.Amount = transfer.Amount.String()

				// 设置精度
				if transfer.BaseTokenType == BaseTokenTypePoints {
					transaction.Decimal = h.confCustom.AssetTokens.Points.Decimals
				} else {
					transaction.Decimal = h.confCustom.AssetTokens.Usdc.Decimals
				}

				depositData := &TxDataDeposit{
					Address: transfer.ExternalAddress,
				}
				depositDataJson, err := json.Marshal(depositData)
				if err != nil {
					ctx.Log.Errorf("Marshal depositData error: %+v", err)
					return nil, 0, errors.New(int(marketcenterPb.ErrorCode_INTERNAL), "INTERNAL_ERROR", err.Error())
				}
				transaction.BizData = string(depositDataJson)
			}

		case TxSourceTransferWithdraw:
			transaction.Type = marketcenterPb.TxType_TX_TYPE_WITHDRAW
			transaction.Side = 2 // 转出
			if transfer, exists := transferMap[sendTx.TxHash]; exists {
				transaction.BaseTokenType = marketcenterPb.BaseTokenType(transfer.BaseTokenType)
				transaction.TokenAddress = transfer.TokenAddress
				transaction.Amount = transfer.Amount.String()

				// 设置精度
				if transfer.BaseTokenType == BaseTokenTypePoints {
					transaction.Decimal = h.confCustom.AssetTokens.Points.Decimals
				} else {
					transaction.Decimal = h.confCustom.AssetTokens.Usdc.Decimals
				}

				withdrawData := &TxDataWithdraw{
					Address: transfer.ExternalAddress,
				}
				withdrawDataJson, err := json.Marshal(withdrawData)
				if err != nil {
					ctx.Log.Errorf("Marshal withdrawData error: %+v", err)
					return nil, 0, errors.New(int(marketcenterPb.ErrorCode_INTERNAL), "INTERNAL_ERROR", err.Error())
				}
				transaction.BizData = string(withdrawDataJson)
			}
		}

		transactions = append(transactions, transaction)
	}

	return transactions, total, nil
}

func (h *AssetHandler) GetOrdersDistinctMarkets(ctx common.Ctx, query *OrderQuery) ([]string, error) {
	markets, err := h.assetRepo.GetOrdersDistinctMarkets(ctx, query)
	if err != nil {
		return nil, errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}
	return markets, nil
}

func (h *AssetHandler) GetUserEarnedPoints(ctx common.Ctx, uid string, source uint8) (decimal.Decimal, error) {
	points, err := h.assetRepo.GetUserEarnedPoints(ctx, uid, source)
	if err != nil {
		return decimal.Zero, errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}
	return points, nil
}

func (h *AssetHandler) UpdateLeaderboard(ctx common.Ctx, leaderboardKey string, uid string, score float64) error {
	_, err := h.assetRepo.ZIncrBy(ctx, leaderboardKey, score, uid)
	if err != nil {
		return errors.New(int(marketcenterPb.ErrorCode_REDIS), "REDIS_ERROR", err.Error())
	}
	return nil
}

func (h *AssetHandler) GetLeaderboardEntries(ctx common.Ctx, leaderboardKey string, start, stop int64) ([]redis.Z, error) {
	entries, err := h.assetRepo.ZRevRangeWithScores(ctx, leaderboardKey, start, stop)
	if err != nil {
		return nil, errors.New(int(marketcenterPb.ErrorCode_REDIS), "REDIS_ERROR", err.Error())
	}
	return entries, nil
}

func (h *AssetHandler) GetLeaderboardTotal(ctx common.Ctx, leaderboardKey string, min, max string) (int64, error) {
	total, err := h.assetRepo.ZCount(ctx, leaderboardKey, min, max)
	if err != nil {
		return 0, errors.New(int(marketcenterPb.ErrorCode_REDIS), "REDIS_ERROR", err.Error())
	}
	return total, nil
}

func (h *AssetHandler) GetUserRank(ctx common.Ctx, leaderboardKey string, uid string) (int64, error) {
	rank, err := h.assetRepo.ZRevRank(ctx, leaderboardKey, uid)
	if err != nil {
		return -1, errors.New(int(marketcenterPb.ErrorCode_REDIS), "REDIS_ERROR", err.Error())
	}
	return rank, nil
}

func (h *AssetHandler) GetUserScore(ctx common.Ctx, leaderboardKey string, uid string) (float64, error) {
	score, err := h.assetRepo.ZScore(ctx, leaderboardKey, uid)
	if err != nil {
		return 0, errors.New(int(marketcenterPb.ErrorCode_REDIS), "REDIS_ERROR", err.Error())
	}
	return score, nil
}
