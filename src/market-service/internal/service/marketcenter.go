package service

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	marketcenter "market-proto/proto/market-service/marketcenter/v1"
	marketcenterPb "market-proto/proto/market-service/marketcenter/v1"
	assetBiz "market-service/internal/biz/asset"
	"market-service/internal/biz/base"
	marketBiz "market-service/internal/biz/market"
	userBiz "market-service/internal/biz/user"
	"market-service/internal/pkg/alarm"
	"market-service/internal/pkg/common"
	"market-service/internal/pkg/util"
	"market-service/internal/sse_message"
	"math/big"
	"runtime/debug"
	"strings"
	"time"

	"github.com/go-kratos/kratos/v2/errors"

	"github.com/shopspring/decimal"
	"gorm.io/gorm"
)

func (s *MarketService) CreateMarketsAndOptions(ctx context.Context, req *marketcenter.CreateMarketsAndOptionsRequest) (*marketcenter.CreateMarketsAndOptionsResponse, error) {
	c := common.NewBaseCtx(ctx, s.log)

	marketEntityList := make([]*marketBiz.MarketEntity, 0, len(req.Markets))
	for _, market := range req.Markets {
		marketEntity := &marketBiz.MarketEntity{
			Address: market.Address,
			Name:    market.Name,
			TokenType: func() uint8 {
				if market.BaseTokenAddress == s.confCustom.AssetTokens.Points.Address {
					return marketBiz.BaseTokenTypePoints
				} else if market.BaseTokenAddress == s.confCustom.AssetTokens.Usdc.Address {
					return marketBiz.BaseTokenTypeUsdc
				}
				return marketBiz.BaseTokenTypePoints
			}(),
			OracleAddress:    market.OracleAddress,
			EventId:          market.EventId,
			ConditionId:      market.ConditionId,
			QuestionId:       market.QuestionId,
			OutcomeSlotCount: market.OutcomeSlotCount,
			Deadline:      market.Deadline,
			TxHash:        market.TxHash,
		}
		for _, option := range market.Options {
			marketEntity.Options = append(marketEntity.Options, &marketBiz.OptionEntity{
				MarketAddress: market.Address,
				Address:       option.Address,
				Name:          option.Name,
				Symbol:        option.Symbol,
				Description:   option.Description,
				Decimal:       uint8(option.Decimal),
				Index:         option.Index,
				BaseTokenType: marketEntity.TokenType,
				PositionId:    option.PositionId,
			})
		}
		marketEntityList = append(marketEntityList, marketEntity)
	}

	newMarketEntityList, err := s.marketHandler.CreateMarketAndOptions(c, marketEntityList)
	if err != nil {
		return nil, err
	}

	go func(newCtx common.Ctx) {
		defer func() {
			if err := recover(); err != nil {
				alarm.Lark.Send(fmt.Sprintf("embedding market info async EmbeddingMarketInfo panic err: %+v, stack: %+v", err, string(debug.Stack())))
				newCtx.Log.Errorf("embedding market info async EmbeddingMarketInfo panic err: %+v, stack: %+v", err, string(debug.Stack()))
			}
		}()
		newCtx.Log.Infof("start embedding market info async EmbeddingMarketInfo, marketEntityList: %+v", newMarketEntityList)
		if err := s.marketHandler.EmbeddingMarketInfo(newCtx, newMarketEntityList); err != nil {
			alarm.Lark.Send(fmt.Sprintf("embedding market info async EmbeddingMarketInfo error, traceId: %+v, err: %+v", common.GetTraceId(newCtx.Ctx), err))
			newCtx.Log.Errorf("embedding market info async EmbeddingMarketInfo error: %+v", err)
		}
	}(common.CloneBaseCtx(c, s.log))

	return &marketcenter.CreateMarketsAndOptionsResponse{}, nil
}

func (s *MarketService) GetMarketsAndOptionsForBlockListener(ctx context.Context, req *marketcenter.GetMarketsAndOptionsForBlockListenerRequest) (*marketcenter.GetMarketsAndOptionsForBlockListenerResponse, error) {
	c := common.NewBaseCtx(ctx, s.log)
	// 去重
	marketAddressMap := make(map[string]struct{})
	marketEntityList := make([]*marketBiz.MarketEntity, 0, len(req.MarketAddresses))
	for _, marketAddress := range req.MarketAddresses {
		if _, exists := marketAddressMap[marketAddress]; !exists {
			marketAddressMap[marketAddress] = struct{}{}
			marketEntityList = append(marketEntityList, &marketBiz.MarketEntity{
				Address: marketAddress,
			})
		}
	}
	marketEntityList, err := s.marketHandler.GetMarketsAndOptionsForBlockListener(c, marketEntityList)
	if err != nil {
		return nil, err
	}

	rsp := &marketcenter.GetMarketsAndOptionsForBlockListenerResponse{}
	for _, marketEntity := range marketEntityList {
		if marketEntity == nil {
			continue
		}
		market := &marketcenter.GetMarketsAndOptionsForBlockListenerResponse_Market{
			Address:       marketEntity.Address,
			BaseTokenType: marketcenter.BaseTokenType(marketEntity.TokenType),
		}
		for _, option := range marketEntity.Options {
			market.Options = append(market.Options, &marketcenter.GetMarketsAndOptionsForBlockListenerResponse_Market_Option{
				Address:       option.Address,
				Index:         option.Index,
				MarketAddress: marketEntity.Address,
				Decimal:       uint32(option.Decimal),
				BaseTokenType: marketcenter.BaseTokenType(marketEntity.TokenType),
			})
		}
		rsp.Markets = append(rsp.Markets, market)
	}
	return rsp, nil
}

func (s *MarketService) GetPayMasterData(ctx context.Context, req *marketcenter.GetPayMasterDataRequest) (*marketcenter.GetPayMasterDataResponse, error) {
	c := common.NewBaseCtx(ctx, s.log)
	paymasterData, err := s.assetHandler.GetPayMasterData(c, &assetBiz.UserOperation{
		Sender: req.UserOperation.Sender,
		Nonce:  req.UserOperation.Nonce,
		// InitCode:                      req.UserOperation.InitCode,
		CallData:                      req.UserOperation.CallData,
		CallGasLimit:                  req.UserOperation.CallGasLimit,
		VerificationGasLimit:          req.UserOperation.VerificationGasLimit,
		PreVerificationGas:            req.UserOperation.PreVerificationGas,
		MaxFeePerGas:                  req.UserOperation.MaxFeePerGas,
		MaxPriorityFeePerGas:          req.UserOperation.MaxPriorityFeePerGas,
		Signature:                     req.UserOperation.Signature,
		Factory:                       req.UserOperation.Factory,
		FactoryData:                   req.UserOperation.FactoryData,
		Paymaster:                     req.UserOperation.Paymaster,
		PaymasterData:                 req.UserOperation.PaymasterData,
		PaymasterVerificationGasLimit: req.UserOperation.PaymasterVerificationGasLimit,
		PaymasterPostOpGasLimit:       req.UserOperation.PaymasterPostOpGasLimit,
	})
	if err != nil {
		return nil, err
	}
	return &marketcenter.GetPayMasterDataResponse{
		Paymaster:                     paymasterData.PaymasterAddress,
		PaymasterData:                 paymasterData.PaymasterData,
		PaymasterVerificationGasLimit: paymasterData.PaymasterVerificationGasLimit,
		PreVerificationGas:            paymasterData.PreVerificationGas,
		VerificationGasLimit:          paymasterData.VerificationGasLimit,
		CallGasLimit:                  paymasterData.CallGasLimit,
	}, nil
}

func (s *MarketService) PlaceOrder(ctx context.Context, req *marketcenter.PlaceOrderRequest) (*marketcenter.PlaceOrderResponse, error) {
	return nil, errors.New(int(marketcenterPb.ErrorCode_INTERNAL), "Trading Paused Due to Upgrading", "Trading Paused Due to Upgrading")
	c := common.NewBaseCtx(ctx, s.log)
	amount, err := decimal.NewFromString(req.Amount)
	if err != nil {
		return nil, err
	}
	minReceiveAmount, err := decimal.NewFromString(req.MinReceiveAmount)
	if err != nil {
		return nil, err
	}
	price, err := decimal.NewFromString(req.Price)
	if err != nil {
		return nil, err
	}
	orderEntity := &assetBiz.OrderEntity{
		// basetokentype在biz层中设置
		UID:              req.Uid,
		MarketAddress:    req.MarketAddress,
		OptionAddress:    req.OptionAddress,
		Side:             uint8(req.Side),
		Amount:           amount,
		MinReceiveAmount: minReceiveAmount,
		Deadline:         time.Unix(int64(req.Deadline), 0),
		Price:            price,
		Status:           assetBiz.OrderStatusPending,
		Tx: &assetBiz.SendTxEntity{
			UID:    req.Uid,
			Status: assetBiz.SendTxStatusSending,
			Chain:  s.confCustom.Chain,
			Type:   assetBiz.TxTypeUserOperation,
			Source: func() uint8 {
				if req.Side == marketcenter.PlaceOrderRequest_SIDE_DEPOSIT {
					return assetBiz.TxSourceBuy
				}
				return assetBiz.TxSourceSell
			}(),
			UserOperation: &assetBiz.UserOperation{
				Sender:                        req.UserOperation.Sender,
				Nonce:                         req.UserOperation.Nonce,
				CallData:                      req.UserOperation.CallData,
				CallGasLimit:                  req.UserOperation.CallGasLimit,
				VerificationGasLimit:          req.UserOperation.VerificationGasLimit,
				PreVerificationGas:            req.UserOperation.PreVerificationGas,
				MaxFeePerGas:                  req.UserOperation.MaxFeePerGas,
				MaxPriorityFeePerGas:          req.UserOperation.MaxPriorityFeePerGas,
				Signature:                     req.UserOperation.Signature,
				Factory:                       req.UserOperation.Factory,
				FactoryData:                   req.UserOperation.FactoryData,
				Paymaster:                     req.UserOperation.Paymaster,
				PaymasterData:                 req.UserOperation.PaymasterData,
				PaymasterVerificationGasLimit: req.UserOperation.PaymasterVerificationGasLimit,
				PaymasterPostOpGasLimit:       req.UserOperation.PaymasterPostOpGasLimit,
			},
		},
	}
	newOrderEntity, err := s.assetHandler.PlaceOrder(c, orderEntity)
	if err != nil {
		if !errors.Is(err, assetBiz.ErrPlaceOrderTooQuick) {
			s.assetHandler.GetRepo().ReleaseLockDirect(c, fmt.Sprintf(assetBiz.UserOperationLockKey, req.Uid))
		}
		return nil, err
	}

	go func(newCtx common.Ctx) {
		defer func() {
			if err := recover(); err != nil {
				alarm.Lark.Send(fmt.Sprintf("place order async WaitPlaceOrderUserOperationReceipt panic err: %+v, stack: %+v", err, string(debug.Stack())))
				newCtx.Log.Errorf("place order async WaitPlaceOrderUserOperationReceipt panic err: %+v, stack: %+v", err, string(debug.Stack()))
			}
		}()
		s.assetHandler.WaitPlaceOrderUserOperationReceipt(newCtx, newOrderEntity)
	}(common.CloneBaseCtx(c, s.log))

	return &marketcenter.PlaceOrderResponse{OpHash: newOrderEntity.OpHash}, nil
}

func (s *MarketService) ClaimMarketResult(ctx context.Context, req *marketcenter.ClaimMarketResultRequest) (*marketcenter.ClaimMarketResultResponse, error) {
	c := common.NewBaseCtx(ctx, s.log)

	userClaimResultEntity := &assetBiz.UserClaimResultEntity{
		UID:           req.Uid,
		MarketAddress: req.MarketAddress,
		OptionAddress: req.OptionAddress,
		Status:        assetBiz.UserClaimResultStatusPending,
		Tx: &assetBiz.SendTxEntity{
			UID:    req.Uid,
			Status: assetBiz.SendTxStatusSending,
			Chain:  s.confCustom.Chain,
			Type:   assetBiz.TxTypeUserOperation,
			Source: assetBiz.TxSourceUserClaim,
			UserOperation: &assetBiz.UserOperation{
				Sender:                        req.UserOperation.Sender,
				Nonce:                         req.UserOperation.Nonce,
				CallData:                      req.UserOperation.CallData,
				CallGasLimit:                  req.UserOperation.CallGasLimit,
				VerificationGasLimit:          req.UserOperation.VerificationGasLimit,
				PreVerificationGas:            req.UserOperation.PreVerificationGas,
				MaxFeePerGas:                  req.UserOperation.MaxFeePerGas,
				MaxPriorityFeePerGas:          req.UserOperation.MaxPriorityFeePerGas,
				Signature:                     req.UserOperation.Signature,
				Factory:                       req.UserOperation.Factory,
				FactoryData:                   req.UserOperation.FactoryData,
				Paymaster:                     req.UserOperation.Paymaster,
				PaymasterData:                 req.UserOperation.PaymasterData,
				PaymasterVerificationGasLimit: req.UserOperation.PaymasterVerificationGasLimit,
				PaymasterPostOpGasLimit:       req.UserOperation.PaymasterPostOpGasLimit,
			},
		},
	}
	newUserClaimResultEntity, err := s.assetHandler.ClaimMarketResult(c, userClaimResultEntity)
	if err != nil {
		if !errors.Is(err, assetBiz.ErrPlaceOrderTooQuick) {
			s.assetHandler.GetRepo().ReleaseLockDirect(c, fmt.Sprintf(assetBiz.UserOperationLockKey, req.Uid))
		}
		return nil, err
	}
	if newUserClaimResultEntity.OpHash != "" {
		go func(newCtx common.Ctx) {
			defer func() {
				if err := recover(); err != nil {
					newCtx.Log.Errorf("claim market result async WaitClaimMarketResultUserOperationReceipt panic err: %+v, stack: %+v", err, string(debug.Stack()))
					alarm.Lark.Send(fmt.Sprintf("claim market result async WaitClaimMarketResultUserOperationReceipt panic err: %+v, stack: %+v", err, string(debug.Stack())))
				}
			}()
			s.assetHandler.WaitClaimMarketResultUserOperationReceipt(newCtx, newUserClaimResultEntity)
		}(common.CloneBaseCtx(c, s.log))
	}

	return &marketcenter.ClaimMarketResultResponse{OpHash: newUserClaimResultEntity.OpHash}, nil
}

func (s *MarketService) TransferBaseToken(ctx context.Context, req *marketcenter.TransferBaseTokenRequest) (*marketcenter.TransferBaseTokenResponse, error) {
	c := common.NewBaseCtx(ctx, s.log)

	amount, err := decimal.NewFromString(req.Amount)
	if err != nil {
		return nil, errors.New(int(marketcenter.ErrorCode_PARAM), "PARAM_ERROR", err.Error())
	}

	userTransferTokensEntity := &assetBiz.UserTransferTokensEntity{
		UID: req.Uid,
		TokenAddress: func() string {
			if req.BaseTokenType == marketcenter.BaseTokenType_BASE_TOKEN_TYPE_USDC {
				return s.confCustom.AssetTokens.Usdc.Address
			}
			return s.confCustom.AssetTokens.Points.Address
		}(),
		ExternalAddress: req.ToAddress,
		Side:            assetBiz.UserTransferTokensSideWithdraw,
		BaseTokenType:   uint8(req.BaseTokenType),
		Amount:          amount,
		Status:          assetBiz.UserTransferTokensStatusPending,
		EventProcessed:  assetBiz.ProcessedNo,
		Tx: &assetBiz.SendTxEntity{
			BaseTokenType: uint8(req.BaseTokenType),
			UID:           req.Uid,
			Status:        assetBiz.SendTxStatusSending,
			Chain:         s.confCustom.Chain,
			Type:          assetBiz.TxTypeUserOperation,
			Source:        assetBiz.TxSourceTransferWithdraw,
			UserOperation: &assetBiz.UserOperation{
				Sender:                        req.UserOperation.Sender,
				Nonce:                         req.UserOperation.Nonce,
				CallData:                      req.UserOperation.CallData,
				CallGasLimit:                  req.UserOperation.CallGasLimit,
				VerificationGasLimit:          req.UserOperation.VerificationGasLimit,
				PreVerificationGas:            req.UserOperation.PreVerificationGas,
				MaxFeePerGas:                  req.UserOperation.MaxFeePerGas,
				MaxPriorityFeePerGas:          req.UserOperation.MaxPriorityFeePerGas,
				Signature:                     req.UserOperation.Signature,
				Factory:                       req.UserOperation.Factory,
				FactoryData:                   req.UserOperation.FactoryData,
				Paymaster:                     req.UserOperation.Paymaster,
				PaymasterData:                 req.UserOperation.PaymasterData,
				PaymasterVerificationGasLimit: req.UserOperation.PaymasterVerificationGasLimit,
				PaymasterPostOpGasLimit:       req.UserOperation.PaymasterPostOpGasLimit,
			},
		},
	}
	newUserTransferTokensEntity, err := s.assetHandler.TransferBaseToken(c, userTransferTokensEntity)
	if err != nil {
		return nil, err
	}

	go func(newCtx common.Ctx) {
		defer func() {
			if err := recover(); err != nil {
				newCtx.Log.Errorf("transfer base token async WaitTransferBaseTokenUserOperationReceipt panic err: %+v, stack: %+v", err, string(debug.Stack()))
				alarm.Lark.Send(fmt.Sprintf("transfer base token async WaitTransferBaseTokenUserOperationReceipt panic err: %+v, stack: %+v", err, string(debug.Stack())))
			}
		}()
		s.assetHandler.WaitTransferBaseTokenUserOperationReceipt(newCtx, newUserTransferTokensEntity)
	}(common.CloneBaseCtx(c, s.log))
	return &marketcenter.TransferBaseTokenResponse{OpHash: newUserTransferTokensEntity.OpHash}, nil
}

func (s *MarketService) UpdateUserMarketFollowStatus(ctx context.Context, req *marketcenter.UpdateUserMarketFollowStatusRequest) (*marketcenter.UpdateUserMarketFollowStatusResponse, error) {
	c := common.NewBaseCtx(ctx, s.log)
	err := s.marketHandler.UpdateUserMarketFollowStatus(c, &marketBiz.UserMarketFollowEntity{
		UID:           req.Uid,
		MarketAddress: req.MarketAddress,
		Status:        uint8(req.Status),
	})
	if err != nil {
		return nil, err
	}
	return &marketcenter.UpdateUserMarketFollowStatusResponse{}, nil
}

func (s *MarketService) GetFollowedMarkets(ctx context.Context, req *marketcenter.GetFollowedMarketsRequest) (*marketcenter.GetFollowedMarketsResponse, error) {
	c := common.NewBaseCtx(ctx, s.log)
	if req.Page <= 0 {
		req.Page = 1
	}
	if req.PageSize <= 0 {
		req.PageSize = 10
	} else if req.PageSize > 100 {
		req.PageSize = 100
	}

	marketEntityList, total, err := s.marketHandler.GetUserFollowedMarketsWithTotal(c, &marketBiz.UserMarketFollowQuery{
		UID:    req.Uid,
		Status: marketBiz.UserMarketFollowStatusActive,
		BaseQuery: base.BaseQuery{
			Order:  "id desc",
			Limit:  int32(req.PageSize),
			Offset: int32((req.Page - 1) * req.PageSize),
		},
	})
	if err != nil {
		return nil, err
	}
	if len(marketEntityList) == 0 {
		return &marketcenter.GetFollowedMarketsResponse{
			Total:   0,
			Markets: make([]*marketcenter.GetFollowedMarketsResponse_Market, 0),
		}, nil
	}

	rsp := &marketcenter.GetFollowedMarketsResponse{
		Total:   uint32(total),
		Markets: make([]*marketcenter.GetFollowedMarketsResponse_Market, 0, len(marketEntityList)),
	}
	for _, marketEntity := range marketEntityList {
		rsp.Markets = append(rsp.Markets, &marketcenter.GetFollowedMarketsResponse_Market{
			Address:     marketEntity.Address,
			Name:        marketEntity.Name,
			PicUrl:      marketEntity.PicUrl,
			Description: marketEntity.Description,
			Status:      uint32(marketEntity.Status),
			Deadline:    int64(marketEntity.Deadline),
		})
	}
	return rsp, nil
}

func (s *MarketService) GetHoldingPositionsMarkets(ctx context.Context, req *marketcenter.GetHoldingPositionsMarketsRequest) (*marketcenter.GetHoldingPositionsMarketsResponse, error) {
	c := common.NewBaseCtx(ctx, s.log)
	if req.Page <= 0 {
		req.Page = 1
	}
	if req.PageSize <= 0 {
		req.PageSize = 10
	} else if req.PageSize > 100 {
		req.PageSize = 100
	}

	rsp := &marketcenter.GetHoldingPositionsMarketsResponse{
		Markets: make([]*marketcenter.GetHoldingPositionsMarketsResponse_Market, 0),
	}

	userTotalValue, err := s.assetHandler.GetUserTotalValue(c, &assetBiz.UserTokenBalanceQuery{
		UID:           req.Uid,
		BaseTokenType: uint8(req.BaseTokenType),
	})
	if err != nil {
		return nil, err
	}

	marketValues, total, err := s.assetHandler.GetUserMarketPositionsByValue(c, &assetBiz.UserTokenBalanceQuery{
		UID:           req.Uid,
		BaseTokenType: uint8(req.BaseTokenType),
		BaseQuery: base.BaseQuery{
			Limit:  int32(req.PageSize),
			Offset: int32((req.Page - 1) * req.PageSize),
		},
	})
	if err != nil {
		return nil, err
	}

	if len(marketValues) == 0 {
		return rsp, nil
	}

	marketAddressList := make([]string, 0, len(marketValues))
	for _, marketValue := range marketValues {
		marketAddressList = append(marketAddressList, marketValue.MarketAddress)
	}

	// 2. 查市场基础信息
	marketEntityList, err := s.marketHandler.GetMarkets(c, &marketBiz.MarketQuery{
		AddressList: marketAddressList,
	})
	if err != nil {
		return nil, err
	}
	marketAddressToMarketEntityMap := make(map[string]*marketBiz.MarketEntity)
	for _, marketEntity := range marketEntityList {
		marketAddressToMarketEntityMap[marketEntity.Address] = marketEntity
	}

	// 3. 查市场的option基础信息
	optionEntityList, err := s.marketHandler.GetOptions(c, &marketBiz.OptionQuery{
		MarketAddressList: marketAddressList,
	})
	if err != nil {
		return nil, err
	}
	optionAddressToOptionEntityMap := make(map[string]*marketBiz.OptionEntity)
	for _, optionEntity := range optionEntityList {
		optionAddressToOptionEntityMap[optionEntity.Address] = optionEntity
	}

	// 4. 查用户具体持仓的option表
	userTokenBalanceEntities, err := s.assetHandler.GetUserTokenBalance(c, &assetBiz.UserTokenBalanceQuery{
		UID:               req.Uid,
		MarketAddressList: marketAddressList,
		Type:              assetBiz.TypeUserTokenBalanceOption,
		BaseTokenType:     uint8(req.BaseTokenType),
		StatusNotEqual:    assetBiz.UserTokenBalanceStatusEndLose,
		NoZero:            true,
	})
	if err != nil {
		return nil, err
	}

	decimals := 0
	if req.BaseTokenType == marketcenter.BaseTokenType_BASE_TOKEN_TYPE_USDC {
		decimals = int(s.confCustom.AssetTokens.Usdc.Decimals)
	} else {
		decimals = int(s.confCustom.AssetTokens.Points.Decimals)
	}
	rspMarketInfoList := make([]*marketcenter.GetHoldingPositionsMarketsResponse_Market, 0, len(marketAddressList))
	for _, marketValueSt := range marketValues {
		marketInfo := &marketcenter.GetHoldingPositionsMarketsResponse_Market{
			Address:              marketValueSt.MarketAddress,
			UserMarketTotalValue: marketValueSt.TotalValue.Div(decimal.New(1, int32(decimals))).String(),
			Positions:            make([]*marketcenter.GetHoldingPositionsMarketsResponse_Market_Position, 0, len(userTokenBalanceEntities)),
		}

		if marketEntity, ok := marketAddressToMarketEntityMap[marketValueSt.MarketAddress]; ok {
			marketInfo.Name = marketEntity.Name
			marketInfo.PicUrl = marketEntity.PicUrl
			marketInfo.Description = marketEntity.Description
			marketInfo.Status = uint32(marketEntity.Status)
			marketInfo.MarketVolume = marketEntity.Volume.String()
			marketInfo.MarketParticipantsCount = uint32(marketEntity.ParticipantsCount)
			marketInfo.BaseTokenType = marketcenter.BaseTokenType(marketEntity.TokenType)
		}

		for _, userTokenBalanceEntity := range userTokenBalanceEntities {
			if userTokenBalanceEntity.MarketAddress != marketValueSt.MarketAddress {
				continue
			}

			position := &marketcenter.GetHoldingPositionsMarketsResponse_Market_Position{
				TokenAddress: userTokenBalanceEntity.TokenAddress,
				Balance:      userTokenBalanceEntity.Balance.String(),
				Decimal:      uint32(userTokenBalanceEntity.Decimal),
			}

			if optionEntity, ok := optionAddressToOptionEntityMap[userTokenBalanceEntity.TokenAddress]; ok {
				position.TokenName = optionEntity.Name
				position.TokenSymbol = optionEntity.Symbol
				position.TokenPicUrl = optionEntity.PicUrl
				position.TokenDescription = optionEntity.Description
			}

			marketInfo.Positions = append(marketInfo.Positions, position)
		}
		rspMarketInfoList = append(rspMarketInfoList, marketInfo)
	}

	return &marketcenter.GetHoldingPositionsMarketsResponse{
		Total:      uint32(total),
		TotalValue: userTotalValue.Div(decimal.New(1, int32(decimals))).String(),
		Markets:    rspMarketInfoList,
	}, nil
}

func (s *MarketService) GetHotMarkets(ctx context.Context, req *marketcenter.GetHotMarketsRequest) (*marketcenter.GetHotMarketsResponse, error) {
	c := common.NewBaseCtx(ctx, s.log)
	marketEntityList, err := s.marketHandler.GetMarkets(c, &marketBiz.MarketQuery{
		Status:        marketBiz.MarketStatusRunnig,
		IsShow:        uint8(marketBiz.MarketShow),
		IsNotDeadline: true,
		BaseTokenType: uint8(req.BaseTokenType),
		BaseQuery: base.BaseQuery{
			Order: "volume desc",
			Limit: 6,
		},
	})
	if err != nil {
		return nil, err
	}
	rsp := &marketcenter.GetHotMarketsResponse{
		Markets: make([]*marketcenter.GetHotMarketsResponse_Market, 0, len(marketEntityList)),
	}
	for _, marketEntity := range marketEntityList {
		rsp.Markets = append(rsp.Markets, &marketcenter.GetHotMarketsResponse_Market{
			Address:       marketEntity.Address,
			Name:          marketEntity.Name,
			PicUrl:        marketEntity.PicUrl,
			Description:   marketEntity.Description,
			BaseTokenType: marketcenter.BaseTokenType(marketEntity.TokenType),
		})
	}
	return rsp, nil
}

func (s *MarketService) GetUserTrades(ctx context.Context, req *marketcenter.GetUserTradesRequest) (*marketcenter.GetUserTradesResponse, error) {
	c := common.NewBaseCtx(ctx, s.log)
	if req.Page <= 0 {
		req.Page = 1
	}
	if req.PageSize <= 0 {
		req.PageSize = 10
	} else if req.PageSize > 100 {
		req.PageSize = 100
	}

	rsp := &marketcenter.GetUserTradesResponse{
		Total:  0,
		Orders: make([]*marketcenter.GetUserTradesResponse_Order, 0),
	}

	orderEntities, total, err := s.assetHandler.GetOrdersWithTotal(c, &assetBiz.OrderQuery{
		UID:            req.Uid,
		MarketAddress:  req.MarketAddress,
		Status:         assetBiz.OrderStatusSuccess,
		EventProcessed: assetBiz.ProcessedYes,
		BaseTokenType:  uint8(req.BaseTokenType),
		BaseQuery: base.BaseQuery{
			Order:  "id desc",
			Limit:  int32(req.PageSize),
			Offset: int32((req.Page - 1) * req.PageSize),
		},
	})
	if err != nil {
		return nil, err
	}
	if len(orderEntities) == 0 {
		return rsp, nil
	}

	optionAddressList := make([]string, 0, len(orderEntities))
	for _, orderEntity := range orderEntities {
		optionAddressList = append(optionAddressList, orderEntity.OptionAddress)
	}

	optionEntityList, err := s.marketHandler.GetOptions(c, &marketBiz.OptionQuery{
		AddressList: optionAddressList,
	})
	if err != nil {
		return nil, err
	}

	marketAddressList := make([]string, 0, len(orderEntities))
	for _, orderEntity := range orderEntities {
		marketAddressList = append(marketAddressList, orderEntity.MarketAddress)
	}

	marketEntityList, err := s.marketHandler.GetMarkets(c, &marketBiz.MarketQuery{
		AddressList: marketAddressList,
	})
	if err != nil {
		return nil, err
	}
	marketAddressToMarketEntityMap := make(map[string]*marketBiz.MarketEntity)
	for _, marketEntity := range marketEntityList {
		marketAddressToMarketEntityMap[marketEntity.Address] = marketEntity
	}

	optionAddressToOptionEntityMap := make(map[string]*marketBiz.OptionEntity)
	for _, optionEntity := range optionEntityList {
		optionAddressToOptionEntityMap[optionEntity.Address] = optionEntity
	}

	for _, orderEntity := range orderEntities {
		rspOrder := &marketcenter.GetUserTradesResponse_Order{
			Uuid:          orderEntity.UUID,
			Uid:           orderEntity.UID,
			Side:          uint32(orderEntity.Side),
			Amount:        orderEntity.Amount.String(),
			ReceiveAmount: orderEntity.ReceiveAmount.String(),
			DealPrice:     orderEntity.DealPrice.String(),
			Timestamp:     uint64(orderEntity.CreatedAt.Unix()),
			BaseTokenType: marketcenter.BaseTokenType(orderEntity.BaseTokenType),
		}

		if optionEntity, ok := optionAddressToOptionEntityMap[orderEntity.OptionAddress]; ok {
			rspOrder.Option = &marketcenter.GetUserTradesResponse_Order_Option{
				Address:     optionEntity.Address,
				Name:        optionEntity.Name,
				Description: optionEntity.Description,
				Symbol:      optionEntity.Symbol,
				PicUrl:      optionEntity.PicUrl,
				Decimal:     uint32(optionEntity.Decimal),
			}

			if marketEntity, ok := marketAddressToMarketEntityMap[orderEntity.MarketAddress]; ok {
				rspOrder.Option.MarketAddress = marketEntity.Address
				rspOrder.Option.MarketName = marketEntity.Name
				rspOrder.Option.MarketPicUrl = marketEntity.PicUrl
				rspOrder.Option.MarketDescription = marketEntity.Description
				rspOrder.Option.MarketParticipantsCount = uint32(marketEntity.ParticipantsCount)
			}
		}
		rsp.Orders = append(rsp.Orders, rspOrder)
	}
	rsp.Total = uint32(total)
	return rsp, nil
}

func (s *MarketService) GetMarketTrades(ctx context.Context, req *marketcenter.GetMarketTradesRequest) (*marketcenter.GetMarketTradesResponse, error) {
	c := common.NewBaseCtx(ctx, s.log)

	if req.Page <= 0 {
		req.Page = 1
	}
	if req.PageSize <= 0 {
		req.PageSize = 10
	} else if req.PageSize > 100 {
		req.PageSize = 100
	}

	rsp := &marketcenter.GetMarketTradesResponse{
		Total:  0,
		Orders: make([]*marketcenter.GetMarketTradesResponse_Order, 0),
	}

	orderEntities, total, err := s.assetHandler.GetOrdersWithTotal(c, &assetBiz.OrderQuery{
		MarketAddress:  req.Address,
		Status:         assetBiz.OrderStatusSuccess,
		EventProcessed: assetBiz.ProcessedYes,
		BaseQuery: base.BaseQuery{
			Order:  "id desc",
			Limit:  int32(req.PageSize),
			Offset: int32((req.Page - 1) * req.PageSize),
		},
	})
	if err != nil {
		return nil, err
	}
	if len(orderEntities) == 0 {
		return rsp, nil
	}
	optionAddressList := make([]string, 0, len(orderEntities))
	for _, orderEntity := range orderEntities {
		optionAddressList = append(optionAddressList, orderEntity.OptionAddress)
	}

	optionEntityList, err := s.marketHandler.GetOptions(c, &marketBiz.OptionQuery{
		AddressList: optionAddressList,
	})
	if err != nil {
		return nil, err
	}
	optionAddressToOptionEntityMap := make(map[string]*marketBiz.OptionEntity)
	for _, optionEntity := range optionEntityList {
		optionAddressToOptionEntityMap[optionEntity.Address] = optionEntity
	}
	for _, orderEntity := range orderEntities {
		rspOrder := &marketcenter.GetMarketTradesResponse_Order{
			Uuid:          orderEntity.UUID,
			Uid:           orderEntity.UID,
			Side:          uint32(orderEntity.Side),
			Amount:        orderEntity.Amount.String(),
			ReceiveAmount: orderEntity.ReceiveAmount.String(),
			DealPrice:     orderEntity.DealPrice.String(),
			Timestamp:     uint64(orderEntity.CreatedAt.Unix()),
		}

		if optionEntity, ok := optionAddressToOptionEntityMap[orderEntity.OptionAddress]; ok {
			rspOrder.Option = &marketcenter.GetMarketTradesResponse_Order_Option{
				Address:     optionEntity.Address,
				Name:        optionEntity.Name,
				Description: optionEntity.Description,
				Symbol:      optionEntity.Symbol,
				PicUrl:      optionEntity.PicUrl,
				Decimal:     uint32(optionEntity.Decimal),
			}
		}

		rsp.Orders = append(rsp.Orders, rspOrder)
	}
	rsp.Total = uint32(total)
	return rsp, nil
}

func (s *MarketService) GetMarketUsersPositions(ctx context.Context, req *marketcenter.GetMarketUsersPositionsRequest) (*marketcenter.GetMarketUsersPositionsResponse, error) {
	c := common.NewBaseCtx(ctx, s.log)

	// 查用户持仓余额
	userTokenBalanceEntities, err := s.assetHandler.GetUserTokenBalance(c, &assetBiz.UserTokenBalanceQuery{
		UIDList:       req.Uids,
		MarketAddress: req.MarketAddress,
		Type:          assetBiz.TypeUserTokenBalanceOption,
		MinBalance:    assetBiz.MinPointBalance,
	})
	if err != nil {
		return nil, err
	}

	if len(userTokenBalanceEntities) == 0 {
		return &marketcenter.GetMarketUsersPositionsResponse{}, nil
	}

	optionsAddressList := make([]string, 0, len(userTokenBalanceEntities))
	uidToTokenBalanceMap := make(map[string][]*assetBiz.UserTokenBalanceEntity)
	for _, userTokenBalanceEntity := range userTokenBalanceEntities {
		optionsAddressList = append(optionsAddressList, userTokenBalanceEntity.TokenAddress)
		uidToTokenBalanceMap[userTokenBalanceEntity.UID] = append(uidToTokenBalanceMap[userTokenBalanceEntity.UID], userTokenBalanceEntity)
	}

	// 查条件代币基础信息
	optionEntityList, err := s.marketHandler.GetOptions(c, &marketBiz.OptionQuery{
		AddressList: optionsAddressList,
	})
	if err != nil {
		return nil, err
	}

	optionAddressToOptionEntityMap := make(map[string]*marketBiz.OptionEntity)
	for _, optionEntity := range optionEntityList {
		optionAddressToOptionEntityMap[optionEntity.Address] = optionEntity
	}

	rsp := &marketcenter.GetMarketUsersPositionsResponse{
		UserPositions: make([]*marketcenter.GetMarketUsersPositionsResponse_UserPosition, 0, len(userTokenBalanceEntities)),
	}

	for uid, tokenBalanceList := range uidToTokenBalanceMap {
		userPosition := &marketcenter.GetMarketUsersPositionsResponse_UserPosition{
			Uid:       uid,
			Positions: make([]*marketcenter.GetMarketUsersPositionsResponse_UserPosition_Position, 0, len(tokenBalanceList)),
		}
		for _, tokenBalance := range tokenBalanceList {
			onePosition := &marketcenter.GetMarketUsersPositionsResponse_UserPosition_Position{
				TokenAddress: tokenBalance.TokenAddress,
				Balance:      tokenBalance.Balance.String(),
				Decimal:      uint32(tokenBalance.Decimal),
			}

			if optionEntity, ok := optionAddressToOptionEntityMap[tokenBalance.TokenAddress]; ok {
				onePosition.TokenName = optionEntity.Name
				onePosition.TokenSymbol = optionEntity.Symbol
				onePosition.TokenPicUrl = optionEntity.PicUrl
				onePosition.TokenDescription = optionEntity.Description
			}
			userPosition.Positions = append(userPosition.Positions, onePosition)
		}
		rsp.UserPositions = append(rsp.UserPositions, userPosition)
	}
	return rsp, nil
}

func (s *MarketService) GetMarketDetail(ctx context.Context, req *marketcenter.GetMarketDetailRequest) (*marketcenter.GetMarketDetailResponse, error) {
	c := common.NewBaseCtx(ctx, s.log)
	marketAddress, err := util.ToChecksumAddress(req.Address)
	if err != nil {
		return nil, marketBiz.ErrMarketNotFound.WithCause(err)
	}
	marketEntity, err := s.marketHandler.GetMarketDetail(c, marketAddress)
	if err != nil {
		return nil, err
	}

	if marketEntity == nil || marketEntity.Address == "" {
		return nil, marketBiz.ErrMarketNotFound
	}

	c.Log.Debugf("GetMarketDetail marketEntity: %v", marketEntity)

	rsp := &marketcenter.GetMarketDetailResponse{
		Address:     marketEntity.Address,
		Name:        marketEntity.Name,
		PicUrl:      marketEntity.PicUrl,
		Description: marketEntity.Description,
		Deadline:    marketEntity.Deadline,
		Volume:      marketEntity.Volume.String(),
		Decimal: func() uint32 {
			if marketEntity.TokenType == assetBiz.BaseTokenTypePoints {
				return s.confCustom.AssetTokens.Points.Decimals
			} else if marketEntity.TokenType == assetBiz.BaseTokenTypeUsdc {
				return s.confCustom.AssetTokens.Usdc.Decimals
			} else {
				return 6
			}
		}(),
		BaseTokenType: marketcenter.BaseTokenType(marketEntity.TokenType),
		BaseTokenAddress: func() string {
			if marketEntity.TokenType == assetBiz.BaseTokenTypePoints {
				return s.confCustom.AssetTokens.Points.Address
			} else if marketEntity.TokenType == assetBiz.BaseTokenTypeUsdc {
				return s.confCustom.AssetTokens.Usdc.Address
			} else {
				return s.confCustom.AssetTokens.Points.Address
			}
		}(),
		ParticipantsCount:   marketEntity.ParticipantsCount,
		Rules:               marketEntity.Rules,
		RulesFileUrl:        marketEntity.RulesUrl,
		ResultOptionAddress: marketEntity.Result,
		Status:              uint32(marketEntity.Status),
		IsFollowed:          marketcenter.IsFollowed_IS_FOLLOWED_NO,
		IsClaim:             marketcenter.GetMarketDetailResponse_IS_CLAIMED_NO,
		Options:             make([]*marketcenter.GetMarketDetailResponse_Option, 0, len(marketEntity.Options)),
		EventId:          marketEntity.EventId,
		ConditionId:      marketEntity.ConditionId,
		QuestionId:       marketEntity.QuestionId,
		OutcomeSlotCount: marketEntity.OutcomeSlotCount,
	}

	if req.Uid != "" {
		isFollowed, err := s.marketHandler.IsFollowedMarket(c, marketEntity.Address, req.Uid)
		if err != nil {
			return nil, err
		}
		if isFollowed {
			rsp.IsFollowed = marketcenter.IsFollowed_IS_FOLLOWED_YES
		}

		if marketEntity.Status == marketBiz.MarketStatusEnd {
			isClaim, err := s.assetHandler.IsClaimedMarketResult(c, req.Uid, marketEntity.Address)
			if err != nil {
				return nil, err
			}
			if isClaim {
				rsp.IsClaim = marketcenter.GetMarketDetailResponse_IS_CLAIMED_YES
			}
		}

	}

	for _, option := range marketEntity.Options {
		c.Log.Debugf("GetMarketDetail option: %v", option)
		rspOption := &marketcenter.GetMarketDetailResponse_Option{
			Address:     option.Address,
			Name:        option.Name,
			Symbol:      option.Symbol,
			PicUrl:      option.PicUrl,
			Decimal:     uint32(option.Decimal),
			Description: option.Description,
			PositionId:  option.PositionId,
		}

		if option.OptionTokenPrice != nil {
			rspOption.Price = option.OptionTokenPrice.Price.String()
		}
		if marketEntity.Status != marketBiz.MarketStatusRunnig && option.Address == marketEntity.Result {
			rspOption.Price = decimal.NewFromInt(1).Mul(decimal.New(1, int32(option.Decimal))).String()
		}

		rsp.Options = append(rsp.Options, rspOption)
	}

	return rsp, nil
}

func (s *MarketService) GetUserPositions(ctx context.Context, req *marketcenter.GetUserPositionsRequest) (*marketcenter.GetUserPositionsResponse, error) {
	c := common.NewBaseCtx(ctx, s.log)

	if req.Uid == "" {
		return nil, errors.New(int(marketcenter.ErrorCode_PARAM), "PARAM_ERROR", "missing uid")
	}

	if req.Page <= 0 {
		req.Page = 1
	}
	if req.PageSize <= 0 {
		req.PageSize = 10
	} else if req.PageSize > 100 {
		req.PageSize = 100
	}

	// 1. 查用户持仓表
	userTokenBalanceEntities, total, err := s.assetHandler.GetUserTokenBalanceWithTotal(c, &assetBiz.UserTokenBalanceQuery{
		UID:           req.Uid,
		MarketAddress: req.MarketAddress,
		TokenAddress:  req.OptionAddress,
		BaseTokenType: uint8(req.BaseTokenType),
		Type:          assetBiz.TypeUserTokenBalanceOption,
		MinBalance:    assetBiz.MinPointBalance,
		BaseQuery: base.BaseQuery{
			Order:  "balance desc",
			Limit:  int32(req.PageSize),
			Offset: int32((req.Page - 1) * req.PageSize),
		},
	})
	if err != nil {
		return nil, err
	}

	// 2. 查条件代币基础信息
	optionAddressList := make([]string, 0, len(userTokenBalanceEntities))
	for _, userTokenBalanceEntity := range userTokenBalanceEntities {
		optionAddressList = append(optionAddressList, userTokenBalanceEntity.TokenAddress)
	}
	optionEntityList, err := s.marketHandler.GetOptions(c, &marketBiz.OptionQuery{
		AddressList: optionAddressList,
	})
	if err != nil {
		return nil, err
	}
	optionAddressToOptionEntityMap := make(map[string]*marketBiz.OptionEntity)
	for _, optionEntity := range optionEntityList {
		optionAddressToOptionEntityMap[optionEntity.Address] = optionEntity
	}

	// 3. 查市场基础信息
	marketAddressList := make([]string, 0, len(userTokenBalanceEntities))
	for _, userTokenBalanceEntity := range userTokenBalanceEntities {
		marketAddressList = append(marketAddressList, userTokenBalanceEntity.MarketAddress)
	}
	marketEntityList, err := s.marketHandler.GetMarkets(c, &marketBiz.MarketQuery{
		AddressList: marketAddressList,
	})
	if err != nil {
		return nil, err
	}
	marketAddressToMarketEntityMap := make(map[string]*marketBiz.MarketEntity)
	for _, marketEntity := range marketEntityList {
		marketAddressToMarketEntityMap[marketEntity.Address] = marketEntity
	}

	// 4. 查option当前价格
	optionPriceEntityList, err := s.marketHandler.GetOptionPrices(c, optionAddressList)
	if err != nil {
		return nil, err
	}
	optionAddressToOptionPriceEntityMap := make(map[string]*marketBiz.OptionTokenPriceEntity)
	for _, optionPriceEntity := range optionPriceEntityList {
		optionAddressToOptionPriceEntityMap[optionPriceEntity.TokenAddress] = optionPriceEntity
	}

	rspPositions := make([]*marketcenter.GetUserPositionsResponse_Position, 0, len(userTokenBalanceEntities))
	for _, userTokenBalanceEntity := range userTokenBalanceEntities {

		balance := userTokenBalanceEntity.Balance
		entryPrice := userTokenBalanceEntity.AvgBuyPrice
		betValue := balance.Mul(entryPrice).Div(decimal.New(1, int32(userTokenBalanceEntity.Decimal)))

		onePosition := &marketcenter.GetUserPositionsResponse_Position{
			Balance:    balance.String(),
			EntryPrice: entryPrice.String(),
			BetValue:   betValue.String(),
			Status:     uint32(userTokenBalanceEntity.Status),
			IsClaimed:  uint32(userTokenBalanceEntity.IsClaimed),
		}

		if optionPriceEntity, ok := optionAddressToOptionPriceEntityMap[userTokenBalanceEntity.TokenAddress]; ok {
			marketPirce := optionPriceEntity.Price
			currentValue := balance.Mul(marketPirce).Div(decimal.New(1, int32(userTokenBalanceEntity.Decimal)))

			onePosition.MarketPirce = marketPirce.String()
			onePosition.CurrentValue = currentValue.String()
			onePosition.Pnl = currentValue.Sub(betValue).String()
			onePosition.ToWin = balance.String()
		}

		if marketEntity, ok := marketAddressToMarketEntityMap[userTokenBalanceEntity.MarketAddress]; ok {
			onePosition.MarketAddress = marketEntity.Address
			onePosition.MarketName = marketEntity.Name
			onePosition.MarketDescription = marketEntity.Description
			onePosition.MarketPicUrl = marketEntity.PicUrl
			onePosition.BaseTokenType = marketcenter.BaseTokenType(marketEntity.TokenType)
			onePosition.Deadline = int64(marketEntity.Deadline)
		}

		if optionEntity, ok := optionAddressToOptionEntityMap[userTokenBalanceEntity.TokenAddress]; ok {
			onePosition.OptionAddress = optionEntity.Address
			onePosition.OptionName = optionEntity.Name
			onePosition.OptionSymbol = optionEntity.Symbol
			onePosition.OptionPicUrl = optionEntity.PicUrl
			onePosition.OptionDecimal = uint32(optionEntity.Decimal)
			onePosition.OptionDescription = optionEntity.Description
		}

		rspPositions = append(rspPositions, onePosition)
	}

	return &marketcenter.GetUserPositionsResponse{
		Total:     uint32(total),
		Positions: rspPositions,
	}, nil
}

func (s *MarketService) GetUserLatestAssetValue(ctx context.Context, req *marketcenter.GetUserLatestAssetValueRequest) (*marketcenter.GetUserLatestAssetValueResponse, error) {
	c := common.NewBaseCtx(ctx, s.log)

	if req.Uid == "" {
		return nil, errors.New(int(marketcenter.ErrorCode_PARAM), "PARAM_ERROR", "missing uid")
	}

	assetAddress := ""
	decimalNum := uint32(0)
	if req.BaseTokenType == marketcenter.BaseTokenType_BASE_TOKEN_TYPE_POINTS {
		assetAddress = s.confCustom.AssetTokens.Points.Address
		decimalNum = s.confCustom.AssetTokens.Points.Decimals
	} else if req.BaseTokenType == marketcenter.BaseTokenType_BASE_TOKEN_TYPE_USDC {
		assetAddress = s.confCustom.AssetTokens.Usdc.Address
		decimalNum = s.confCustom.AssetTokens.Usdc.Decimals
	} else {
		return nil, errors.New(int(marketcenter.ErrorCode_PARAM), "PARAM_ERROR", "invalid base token type")
	}
	userAssetValueEntity, err := s.assetHandler.CalculateUserAssetValue(c, req.Uid, assetAddress, uint8(req.BaseTokenType))
	if err != nil {
		return nil, err
	}

	// 查询用户在volume和pnl排行榜中的排名
	var pnlRank int64
	var volumeScore float64

	// 构建排行榜key
	baseTokenType := uint8(req.BaseTokenType)
	volumeLeaderboardKey := fmt.Sprintf(assetBiz.VolumeAllTimeLeaderboard, baseTokenType)
	pnlLeaderboardKey := fmt.Sprintf(assetBiz.PnlAllTimeLeaderboard, baseTokenType)

	volumeScore, err = s.assetHandler.GetUserScore(c, volumeLeaderboardKey, req.Uid)
	if err != nil {
		c.Log.Errorf("GetUserLatestAssetValue GetUserScore volume error: %v", err)
		return nil, err
	}

	// 查询pnl排行榜
	pnlRank, err = s.assetHandler.GetUserRank(c, pnlLeaderboardKey, req.Uid)
	if err != nil {
		c.Log.Errorf("GetUserLatestAssetValue GetUserRank pnl error: %v", err)
		return nil, err
	}
	if pnlRank < 0 {
		pnlRank, err = s.assetHandler.GetLeaderboardTotal(c, pnlLeaderboardKey, "(0", "+inf")
		if err != nil {
			c.Log.Errorf("GetLeaderboard GetLeaderboardTotal error: %v", err)
			return nil, err
		}

	}
	go func(newCtx common.Ctx) {
		latestRecord, err := s.assetHandler.GetUserAssetValue(newCtx, &assetBiz.UserAssetValueQuery{
			UID:           req.Uid,
			AssetAddress:  assetAddress,
			BaseTokenType: uint8(req.BaseTokenType),
			BaseQuery: base.BaseQuery{
				Order: "created_at desc",
				Limit: 1,
			},
		})
		if err != nil {
			newCtx.Log.Errorf("GetUserAssetValue error: %+v", err)
			return
		}

		// 如果有最新记录，比较业务数据是否相同
		if latestRecord != nil && latestRecord.UID != "" {
			// 比较关键业务字段：Value、Balance、Portfolio、Pnl、BaseTokenType
			if latestRecord.Value.Equal(userAssetValueEntity.Value) &&
				latestRecord.Balance.Equal(userAssetValueEntity.Balance) &&
				latestRecord.Portfolio.Equal(userAssetValueEntity.Portfolio) &&
				latestRecord.Pnl.Equal(userAssetValueEntity.Pnl) {
				// 数据完全相同，不需要插入新记录
				newCtx.Log.Infof("UserAssetValue data unchanged, skip insert for uid=%s, asset=%s", req.Uid, assetAddress)
				return
			}
		}

		err = s.assetHandler.BatchCreateUserAssetValue(newCtx, []*assetBiz.UserAssetValueEntity{userAssetValueEntity})
		if err != nil {
			newCtx.Log.Errorf("BatchCreateUserAssetValue error: %+v", err)
			return
		}

	}(common.CloneBaseCtx(c, s.log))

	return &marketcenter.GetUserLatestAssetValueResponse{
		Value:     userAssetValueEntity.Value.String(),
		Balance:   userAssetValueEntity.Balance.String(),
		Portfolio: userAssetValueEntity.Portfolio.String(),
		Pnl:       userAssetValueEntity.Pnl.String(),
		Decimal:   decimalNum,
		Volume:    decimal.NewFromFloat(volumeScore).String(),
		PnlRank:   uint32(pnlRank + 1),
	}, nil
}

func (s *MarketService) SearchMarket(ctx context.Context, req *marketcenter.SearchMarketRequest) (*marketcenter.SearchMarketResponse, error) {
	c := common.NewBaseCtx(ctx, s.log)

	if req.Page <= 0 {
		req.Page = 1
	}
	if req.PageSize <= 0 {
		req.PageSize = 10
	} else if req.PageSize > 100 {
		req.PageSize = 100
	}
	marketEntityList, total, err := s.marketHandler.SearchMarket(c, &marketBiz.MarketQuery{
		Search: req.Keyword,
		IsShow: marketBiz.MarketShow,
		// BaseTokenType: uint8(req.BaseTokenType),
		BaseQuery: base.BaseQuery{
			Order:  "id desc",
			Limit:  int32(req.PageSize),
			Offset: int32((req.Page - 1) * req.PageSize),
		},
	})
	if err != nil {
		return nil, err
	}

	rsp := &marketcenter.SearchMarketResponse{
		Total: uint32(total),
	}
	for _, marketEntity := range marketEntityList {
		rsp.Markets = append(rsp.Markets, &marketcenter.SearchMarketResponse_Market{
			Address:           marketEntity.Address,
			Name:              marketEntity.Name,
			Description:       marketEntity.Description,
			PicUrl:            marketEntity.PicUrl,
			Status:            uint32(marketEntity.Status),
			ParticipantsCount: uint32(marketEntity.ParticipantsCount),
			Volume:            marketEntity.Volume.String(),
			Deadline:          int64(marketEntity.Deadline),
			BaseTokenType:     marketcenter.BaseTokenType(marketEntity.TokenType),
		})
	}
	return rsp, nil
}

func (s *MarketService) GetUserAssetHistory(ctx context.Context, req *marketcenter.GetUserAssetHistoryRequest) (*marketcenter.GetUserAssetHistoryResponse, error) {
	c := common.NewBaseCtx(ctx, s.log)

	var timeRange string
	switch req.TimeInterval {
	case marketcenter.GetUserAssetHistoryRequest_TIME_INTERVAL_7D:
		timeRange = "7d"
	case marketcenter.GetUserAssetHistoryRequest_TIME_INTERVAL_1M:
		timeRange = "1m"
	case marketcenter.GetUserAssetHistoryRequest_TIME_INTERVAL_3M:
		timeRange = "3m"
	case marketcenter.GetUserAssetHistoryRequest_TIME_INTERVAL_ALL:
		timeRange = "all"
	default:
		timeRange = "all"
	}
	userAssetValueEntities, err := s.assetHandler.GetUserAssetHistory(c, req.Uid, uint8(req.BaseTokenType), timeRange)
	if err != nil {
		return nil, err
	}

	rsp := &marketcenter.GetUserAssetHistoryResponse{
		Total:         uint32(len(userAssetValueEntities)),
		BaseTokenType: marketcenter.BaseTokenType(req.BaseTokenType),
		Decimal: func() uint32 {
			switch req.BaseTokenType {
			case marketcenter.BaseTokenType_BASE_TOKEN_TYPE_POINTS:
				return uint32(s.confCustom.AssetTokens.Points.Decimals)
			case marketcenter.BaseTokenType_BASE_TOKEN_TYPE_USDC:
				return uint32(s.confCustom.AssetTokens.Usdc.Decimals)
			}
			return 6
		}(),
	}
	for _, userAssetValueEntity := range userAssetValueEntities {
		rsp.Snapshots = append(rsp.Snapshots, &marketcenter.GetUserAssetHistoryResponse_OneSnapshot{
			Value:     userAssetValueEntity.Value.String(),
			Balance:   userAssetValueEntity.Balance.String(),
			Portfolio: userAssetValueEntity.Portfolio.String(),
			Pnl:       userAssetValueEntity.Pnl.String(),
			Timestamp: uint64(userAssetValueEntity.Time.Unix()),
		})
	}
	return rsp, nil
}

func (s *MarketService) GetMarketOptionPriceHistory(ctx context.Context, req *marketcenter.GetMarketOptionPriceHistoryRequest) (*marketcenter.GetMarketOptionPriceHistoryResponse, error) {
	c := common.NewBaseCtx(ctx, s.log)
	rsp := &marketcenter.GetMarketOptionPriceHistoryResponse{
		Total:     0,
		Snapshots: make([]*marketcenter.GetMarketOptionPriceHistoryResponse_OneSnapshot, 0),
	}
	var timeRange string
	switch req.TimeInterval {
	case marketcenter.GetMarketOptionPriceHistoryRequest_TIME_INTERVAL_1H:
		timeRange = "1h"
	case marketcenter.GetMarketOptionPriceHistoryRequest_TIME_INTERVAL_6H:
		timeRange = "6h"
	case marketcenter.GetMarketOptionPriceHistoryRequest_TIME_INTERVAL_1D:
		timeRange = "1d"
	case marketcenter.GetMarketOptionPriceHistoryRequest_TIME_INTERVAL_1W:
		timeRange = "1w"
	case marketcenter.GetMarketOptionPriceHistoryRequest_TIME_INTERVAL_ALL:
		timeRange = "all"
	default:
		return nil, errors.New(int(marketcenter.ErrorCode_PARAM), "PARAM_ERROR", "invalid time interval")
	}

	tokenPricePoints, tokenAddressMap, tokenAddresses, err := s.marketHandler.GetMarketOptionPriceHistory(c, req.MarketAddress, timeRange)
	if err != nil {
		return nil, err
	}

	if len(tokenPricePoints) == 0 {
		return rsp, nil
	}

	for _, onePoint := range tokenPricePoints {
		pointTimestamp := onePoint.Timestamp
		pointTokenPricesMap := onePoint.TokenPrices
		pointTokenPrices := make([]*marketcenter.GetMarketOptionPriceHistoryResponse_OneSnapshot_TokenPrice, 0, len(pointTokenPricesMap))

		for _, tokenAddress := range tokenAddresses {
			tokenPrice, ok := pointTokenPricesMap[tokenAddress]
			if !ok {
				continue
			}

			onePointTokenPrice := &marketcenter.GetMarketOptionPriceHistoryResponse_OneSnapshot_TokenPrice{
				TokenAddress: tokenAddress,
				Price:        tokenPrice,
			}

			optionEntity, ok := tokenAddressMap[tokenAddress]
			if ok {
				onePointTokenPrice.Decimal = uint32(optionEntity.Decimal)
			}

			pointTokenPrices = append(pointTokenPrices, onePointTokenPrice)
		}

		rsp.Snapshots = append(rsp.Snapshots, &marketcenter.GetMarketOptionPriceHistoryResponse_OneSnapshot{
			Timestamp:   uint64(pointTimestamp.Unix()),
			TokenPrices: pointTokenPrices,
		})
	}
	rsp.Total = uint32(len(tokenPricePoints))
	return rsp, nil
}

func (s *MarketService) ProcessMarketDepositOrWithdrawEvent(ctx context.Context, req *marketcenter.ProcessMarketDepositOrWithdrawEventRequest) (*marketcenter.ProcessMarketDepositOrWithdrawEventResponse, error) {
	c := common.NewBaseCtx(ctx, s.log)

	assetRepo := s.assetHandler.GetRepo()
	assetRepo.ReleaseLockDirect(c, fmt.Sprintf(assetBiz.UserOperationLockKey, req.Uid))
	if req.MarketAddress == "" {
		return nil, errors.New(int(marketcenter.ErrorCode_PARAM), "PARAM_ERROR", "market address is empty")
	}
	lockKey := fmt.Sprintf("market-event-%s", req.MarketAddress)
	lockID, ok, err := assetRepo.AcquireLock(c, lockKey, 5*time.Second)
	if err != nil {
		return nil, errors.New(int(marketcenter.ErrorCode_REDIS), "REDIS_ERROR", err.Error())
	}
	if !ok {
		return nil, errors.New(int(marketcenter.ErrorCode_REDIS), "REDIS_ERROR", "wait lock timeout")
	}
	defer assetRepo.ReleaseLock(c, lockKey, lockID)

	err = assetRepo.ExecTx(c, func(ctx common.Ctx, tx *gorm.DB) error {

		// 1. assethandler
		// 	1.1 更新order订单 status->succ 实际成交价格
		// 	1.2 更新用户 user_token_balance表总余额 (balance) 买入所以还要更新 avg_buy_price 字段
		// 	1.3. 更新用户市场总持仓表

		// 2. markethandler
		// 	2.1 更新market表 参与人数 volume
		// 	2.2 更新option_price表 价格
		if req.Side == marketcenter.ProcessMarketDepositOrWithdrawEventRequest_SIDE_DEPOSIT {
			err := s.assetHandler.ProcessMarketDepositEventInAssetHandler(c, req)
			if err != nil {
				return err
			}

			err = s.marketHandler.ProcessMarketDepositEventInMarketHandler(c, req)
			if err != nil {
				return err
			}
		} else {
			err := s.assetHandler.ProcessMarketWithdrawEventInAssetHandler(c, req)
			if err != nil {
				return err
			}

			err = s.marketHandler.ProcessMarketWithdrawEventInMarketHandler(c, req)
			if err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	go func(newCtx common.Ctx) {
		defer func() {
			if err := recover(); err != nil {
				newCtx.Log.Errorf("async send sse message panic err: %+v, stack: %+v", err, string(debug.Stack()))
				alarm.Lark.Send(fmt.Sprintf("async send sse message panic err: %+v, stack: %+v", err, string(debug.Stack())))
			}
		}()
		s.userHandler.PublishUserChannel(newCtx, req.Uid, &sse_message.Msg{
			Event: sse_message.EventUserPositionChanged,
			Data: &sse_message.UserPositionChangedMsgData{
				Uid:           req.Uid,
				MarketAddress: req.MarketAddress,
				OptionAddress: req.UserOptionTokenAddress,
			},
		})

		s.userHandler.PublishUserChannel(newCtx, req.Uid, &sse_message.Msg{
			Event: sse_message.EventUserAssetChanged,
			Data: &sse_message.UserAssetChangedMsgData{
				Uid:           req.Uid,
				BaseTokenType: uint8(req.BaseTokenType),
			},
		})

		s.userHandler.PublishMarketChannel(newCtx, req.MarketAddress, &sse_message.Msg{
			Event: sse_message.EventMarketNewTrades,
			Data: &sse_message.MarketNewTradesMsgData{
				OptionAddress: req.UserOptionTokenAddress,
				MarketAddress: req.MarketAddress,
			},
		})
		s.userHandler.PublishMarketChannel(newCtx, req.MarketAddress, &sse_message.Msg{
			Event: sse_message.EventMarketPriceUpdate,
			Data: &sse_message.MarketPriceUpdateMsgData{
				MarketAddress: req.MarketAddress,
			},
		})
		s.userHandler.PublishMarketChannel(newCtx, req.MarketAddress, &sse_message.Msg{
			Event: sse_message.EventMarketInfoUpdate,
			Data: &sse_message.MarketInfoUpdateMsgData{
				MarketAddress: req.MarketAddress,
			},
		})

	}(common.CloneBaseCtx(c, s.log))

	go func(newCtx common.Ctx) {
		defer func() {
			if err := recover(); err != nil {
				newCtx.Log.Errorf("async generate trade notification error: %+v, stack: %s", err, string(debug.Stack()))
				alarm.Lark.Send(fmt.Sprintf("async generate trade notification error: %+v, stack: %s", err, string(debug.Stack())))
			}
		}()

		marketInfo, err := s.marketHandler.GetMarket(newCtx, &marketBiz.MarketQuery{
			Address: req.MarketAddress,
		})
		if err != nil {
			newCtx.Log.Errorf("get market info error", err)
			return
		}
		if marketInfo == nil || marketInfo.Address != req.MarketAddress {
			return
		}

		options, err := s.marketHandler.GetOptions(newCtx, &marketBiz.OptionQuery{
			MarketAddress: req.MarketAddress,
		})
		if err != nil {
			newCtx.Log.Errorf("get option info error", err)
			return
		}

		var optionInfo *marketBiz.OptionEntity
		for _, option := range options {
			if option.Address == req.UserOptionTokenAddress {
				optionInfo = option
				break
			}
		}
		if optionInfo == nil {
			return
		}

		bizData, err := json.Marshal(&userBiz.TradeNotificationEntity{
			MarketAddress: marketInfo.Address,
			MarketName:    marketInfo.Name,
			MarketDesc:    marketInfo.Description,
			MarketPicUrl:  marketInfo.PicUrl,
			OptionAddress: optionInfo.Address,
			OptionName:    optionInfo.Name,
			OptionDesc:    optionInfo.Description,
			OptionPicUrl:  optionInfo.PicUrl,
			BaseTokenType: marketInfo.TokenType,
			Decimal:       int32(optionInfo.Decimal),
			BaseTokenAddress: func() string {
				if marketInfo.TokenType == assetBiz.BaseTokenTypeUsdc {
					return s.confCustom.AssetTokens.Usdc.Address
				}
				return s.confCustom.AssetTokens.Points.Address
			}(),
			AmountIn:  req.AmountIn,
			AmountOut: req.AmountOut,
			Side:      uint8(req.Side),
		})
		if err != nil {
			newCtx.Log.Errorf("marshal user notification entity error", err)
			return
		}
		err = s.userHandler.GenerateNewUserNotification(newCtx, &userBiz.UserNotificationEntity{
			UID:           req.Uid,
			UUID:          util.GenerateUUID(),
			Type:          userBiz.NotificationTypeTrade,
			Category:      uint8(userBiz.NotificationCategoryTrade),
			BizJson:       json.RawMessage(bizData),
			Status:        userBiz.NotificationStatusUnRead,
			BaseTokenType: uint8(marketInfo.TokenType),
		})
		if err != nil {
			newCtx.Log.Errorf("async generate trade notification error: %+v", err)
		}

		s.userHandler.PublishUserChannel(newCtx, req.Uid, &sse_message.Msg{
			Event: sse_message.EventUserNewNotification,
			Data: &sse_message.UserNewNotificationMsgData{
				Uid: req.Uid,
			},
		})
	}(common.CloneBaseCtx(c, s.log))

	go func(newCtx common.Ctx) {
		defer func() {
			if err := recover(); err != nil {
				newCtx.Log.Errorf("async ProduceUserTradeStreamMsg panic err: %+v, stack: %+v", err, string(debug.Stack()))
				alarm.Lark.Send(fmt.Sprintf("async ProduceUserTradeStreamMsg panic err: %+v, stack: %+v", err, string(debug.Stack())))
			}
		}()

		msgId, err := s.userHandler.ProduceUserTradeStreamMsg(newCtx, &userBiz.UserTradeStreamMsg{
			UID:           req.Uid,
			MarketAddress: req.MarketAddress,
			OptionAddress: req.UserOptionTokenAddress,
			Side:          uint8(req.Side),
			AmountIn:      req.AmountIn,
			AmountOut:     req.AmountOut,
			Price:         req.Price,
			TxHash:        req.TxHash,
			Timestamp:     int64(req.BlockTime),
			BaseTokenType: uint8(marketcenter.BaseTokenType(req.BaseTokenType)),
		})
		if err != nil {
			alarm.Lark.Send(fmt.Sprintf("async ProduceUserTradeStreamMsg error: %+v, uid: %s", err, req.Uid))
			newCtx.Log.Errorf("async ProduceUserTradeStreamMsg error: %+v", err)
			return
		}
		newCtx.Log.Infof("async ProduceUserTradeStreamMsg success, msgId: %s", msgId)
	}(common.CloneBaseCtx(c, s.log))
	return &marketcenter.ProcessMarketDepositOrWithdrawEventResponse{}, nil
}

func (s *MarketService) ProcessMarketSwapEvent(ctx context.Context, req *marketcenter.ProcessMarketSwapEventRequest) (*marketcenter.ProcessMarketSwapEventResponse, error) {
	c := common.NewBaseCtx(ctx, s.log)

	err := s.marketHandler.ProcessMarketSwapEventInMarketHandler(c, req)
	if err != nil {
		return nil, err
	}

	return &marketcenter.ProcessMarketSwapEventResponse{}, nil
}

func (s *MarketService) ProcessMarketClaimResultEvent(ctx context.Context, req *marketcenter.ProcessMarketClaimResultEventRequest) (*marketcenter.ProcessMarketClaimResultEventResponse, error) {
	c := common.NewBaseCtx(ctx, s.log)

	assetRepo := s.assetHandler.GetRepo()
	assetRepo.ReleaseLockDirect(c, fmt.Sprintf(assetBiz.UserOperationLockKey, req.Uid))

	err := s.assetHandler.ProcessMarketClaimResultEventInAssetHandler(c, req)
	if err != nil {
		return nil, err
	}

	return &marketcenter.ProcessMarketClaimResultEventResponse{}, nil
}

func (s *MarketService) UpdateMarketStatus(ctx context.Context, req *marketcenter.UpdateMarketStatusRequest) (*marketcenter.UpdateMarketStatusResponse, error) {
	c := common.NewBaseCtx(ctx, s.log)

	err := s.marketHandler.UpdateMarketStatus(c, &marketBiz.MarketEntity{
		Address: req.MarketAddress,
		Status:  uint8(req.Status),
	})
	if err != nil {
		return nil, err
	}

	return &marketcenter.UpdateMarketStatusResponse{}, nil
}

func (s *MarketService) UpdateUserBaseTokenBalance(ctx context.Context, req *marketcenter.UpdateUserBaseTokenBalanceRequest) (*marketcenter.UpdateUserBaseTokenBalanceResponse, error) {
	c := common.NewBaseCtx(ctx, s.log)

	amount, err := decimal.NewFromString(req.TokenBalance.Amount)
	if err != nil {
		return nil, err
	}

	transferAmount, err := decimal.NewFromString(req.TransferAmount)
	if err != nil {
		return nil, err
	}

	userTokenBalanceEntity := &assetBiz.UserTokenBalanceEntity{
		UID:           req.Uid,
		MarketAddress: req.TokenBalance.MarketAddress,
		TokenAddress:  req.TokenBalance.TokenAddress,
		Balance:       amount,
		BlockNumber:   req.TokenBalance.BlockNumber,
		Decimal:       uint8(s.confCustom.AssetTokens.Usdc.Decimals),
		Type:          assetBiz.TypeUserTokenBalanceBaseAsset,
		BaseTokenType: func() uint8 {
			if req.TokenBalance.TokenAddress == s.confCustom.AssetTokens.Usdc.Address {
				return assetBiz.BaseTokenTypeUsdc
			}
			return assetBiz.BaseTokenTypePoints
		}(),
		TxHash:         req.TxHash,
		FromAddress:    req.From,
		ToAddress:      req.To,
		Side:           uint8(req.Side),
		TransferAmount: transferAmount,
	}
	err = s.assetHandler.ProcessUserBaseTokenUpdate(c, userTokenBalanceEntity)
	if err != nil {
		return nil, err
	}

	return &marketcenter.UpdateUserBaseTokenBalanceResponse{}, nil
}

func (s *MarketService) GetMarketsAndOptionsInfo(ctx context.Context, req *marketcenter.GetMarketsAndOptionsInfoRequest) (*marketcenter.GetMarketsAndOptionsInfoResponse, error) {
	c := common.NewBaseCtx(ctx, s.log)

	if req.Page <= 0 {
		req.Page = 1
	}
	if req.PageSize <= 0 {
		req.PageSize = 10
	} else if req.PageSize > 100 {
		req.PageSize = 100
	}
	rsp := &marketcenter.GetMarketsAndOptionsInfoResponse{
		Total:   0,
		Markets: make([]*marketcenter.GetMarketsAndOptionsInfoResponse_Market, 0),
	}

	var tagEmbedding []float64
	var err error
	if req.SortType == marketcenter.GetMarketsAndOptionsInfoRequest_SORT_TYPE_SIMILARITY {
		if len(req.HotWords) == 0 {
			return nil, errors.New(int(marketcenter.ErrorCode_PARAM), "PARAM_ERROR", "hot words is empty")
		}

		tagEmbedding, err = s.marketHandler.GetTagsEmbedding(c, req.HotWords)
		if err != nil {
			return nil, err
		}
		if len(tagEmbedding) == 0 {
			return nil, errors.New(int(marketcenter.ErrorCode_INTERNAL), "INTERNAL_ERROR", "tag embedding is empty")
		}
	}

	direction := "desc"
	if req.SortAsc {
		direction = "asc"
	}

	var newMarketAddressList []string
	if req.NewMarket {
		// 查询最新创建的16个市场
		latestMarkets, err := s.marketHandler.GetMarkets(c, &marketBiz.MarketQuery{
			IsShow:        uint8(req.IsShow),
			Status:        uint8(req.Status),
			BaseTokenType: uint8(req.BaseTokenType),
			BaseQuery: base.BaseQuery{
				Order: "id desc",
				Limit: 16,
			},
		})
		if err != nil {
			return nil, err
		}

		for _, market := range latestMarkets {
			newMarketAddressList = append(newMarketAddressList, market.Address)
		}

		if len(newMarketAddressList) == 0 {
			return rsp, nil
		}
	}

	query := &marketBiz.MarketQuery{
		Tag:           req.Tag,
		Status:        uint8(req.Status),
		BaseTokenType: uint8(req.BaseTokenType),
		IsShow:        uint8(req.IsShow),
		IsNotDeadline: true,
		Category:      req.CategoryId,
		// 新增：处理 recommend 参数
		OnlyFollowed: req.Followed,
		FollowUID:    req.Uid,
		// 如果是新市场筛选，添加地址列表限制
		AddressList: newMarketAddressList,
		BaseQuery: base.BaseQuery{
			Limit:  int32(req.PageSize),
			Offset: int32((req.Page - 1) * req.PageSize),
			Order: func() string {
				// 如果需要JOIN，ORDER BY字段也要加表前缀
				tablePrefix := ""
				if req.Followed && req.Uid != "" {
					tablePrefix = "m."
				}

				switch req.SortType {
				case marketcenter.GetMarketsAndOptionsInfoRequest_SORT_TYPE_LATEST:
					return tablePrefix + "id " + direction
				case marketcenter.GetMarketsAndOptionsInfoRequest_SORT_TYPE_VOLUME:
					return tablePrefix + "volume " + direction
				case marketcenter.GetMarketsAndOptionsInfoRequest_SORT_TYPE_PARTICIPANTS:
					return tablePrefix + "participants_count " + direction
				case marketcenter.GetMarketsAndOptionsInfoRequest_SORT_TYPE_SIMILARITY:
					return fmt.Sprintf(tablePrefix+"embedding <=> '%s'::vector", util.FormatVector(tagEmbedding))
				case marketcenter.GetMarketsAndOptionsInfoRequest_SORT_TYPE_EXPIRED:
					return tablePrefix + "deadline " + direction
				default:
					return tablePrefix + "id " + direction
				}
			}(),
		},
	}

	if req.MinVolumeTrending {
		minVolume := decimal.NewFromInt(int64(s.confCustom.MinVolume))
		var decimals uint32 = 6
		if req.BaseTokenType == marketcenter.BaseTokenType_BASE_TOKEN_TYPE_USDC {
			decimals = s.confCustom.AssetTokens.Usdc.Decimals
		} else if req.BaseTokenType == marketcenter.BaseTokenType_BASE_TOKEN_TYPE_POINTS {
			decimals = s.confCustom.AssetTokens.Points.Decimals
		}
		minVolumeDecimal := minVolume.Mul(decimal.New(1, int32(decimals)))
		query.MinVolume = minVolumeDecimal
	}
	if req.SoonExpired {
		hours := s.confCustom.MaxDeadlineHours
		query.MaxDeadline = uint64(time.Now().Unix()) + uint64(hours*3600)
	}

	marketEntityList, total, err := s.marketHandler.GetMarketsAndOptionsInfoAndPricesWithTotal(c, query, req.Uid, req.NotQueryPrice)

	if err != nil {
		return nil, err
	}
	if len(marketEntityList) == 0 {
		return rsp, nil
	}

	for _, marketEntity := range marketEntityList {
		rspMarket := &marketcenter.GetMarketsAndOptionsInfoResponse_Market{
			Address:           marketEntity.Address,
			Name:              marketEntity.Name,
			Description:       marketEntity.Description,
			PicUrl:            marketEntity.PicUrl,
			Status:            uint32(marketEntity.Status),
			ParticipantsCount: uint32(marketEntity.ParticipantsCount),
			Volume:            marketEntity.Volume.String(),
			BaseTokenType:     marketcenter.BaseTokenType(marketEntity.TokenType),
			IsFollowed:        marketcenter.IsFollowed(marketEntity.IsFollowed),
			Result:            marketEntity.Result,
			CreatedAt:         uint32(marketEntity.CreatedAt.Unix()),
			Deadline:          uint32(marketEntity.Deadline),
			Decimal: func() uint32 {
				if marketEntity.TokenType == assetBiz.BaseTokenTypePoints {
					return s.confCustom.AssetTokens.Points.Decimals
				} else if marketEntity.TokenType == assetBiz.BaseTokenTypeUsdc {
					return s.confCustom.AssetTokens.Usdc.Decimals
				} else {
					return 6
				}
			}(),
			Options: make([]*marketcenter.GetMarketsAndOptionsInfoResponse_Market_Option, 0, len(marketEntity.Options)),
			EventId:          marketEntity.EventId,
			ConditionId:      marketEntity.ConditionId,
			QuestionId:       marketEntity.QuestionId,
			OutcomeSlotCount: marketEntity.OutcomeSlotCount,
		}
		for _, optionEntity := range marketEntity.Options {
			option := &marketcenter.GetMarketsAndOptionsInfoResponse_Market_Option{
				Name:        optionEntity.Name,
				Address:     optionEntity.Address,
				Symbol:      optionEntity.Symbol,
				PicUrl:      optionEntity.PicUrl,
				Decimal:     uint32(optionEntity.Decimal),
				Index:       uint32(optionEntity.Index),
				Description: optionEntity.Description,
				PositionId:  optionEntity.PositionId,
			}
			if optionEntity.OptionTokenPrice != nil {
				option.Price = optionEntity.OptionTokenPrice.Price.String()
			}
			rspMarket.Options = append(rspMarket.Options, option)
		}
		rsp.Markets = append(rsp.Markets, rspMarket)
	}
	rsp.Total = uint32(total)
	return rsp, nil
}

func (s *MarketService) BatchGetMarketUsersPositions(ctx context.Context, req *marketcenter.BatchGetMarketUsersPositionsRequest) (*marketcenter.BatchGetMarketUsersPositionsResponse, error) {
	c := common.NewBaseCtx(ctx, s.log)
	rsp := &marketcenter.BatchGetMarketUsersPositionsResponse{
		UserMarketPositions: make([]*marketcenter.BatchGetMarketUsersPositionsResponse_UserMarketPosition, 0),
	}

	userTokenBalanceQueryItems := make([]*assetBiz.UserTokenBalanceQueryItem, 0, len(req.MarketAndUsers))
	for _, marketAndUser := range req.MarketAndUsers {
		userTokenBalanceQueryItems = append(userTokenBalanceQueryItems, &assetBiz.UserTokenBalanceQueryItem{
			UID:           marketAndUser.Uid,
			MarketAddress: marketAndUser.MarketAddress,
		})
	}
	userPositions, err := s.assetHandler.GetUserTokenBalancesByQueryItems(c, userTokenBalanceQueryItems)
	if err != nil {
		return nil, err
	}

	// 修复：按照 (UID, MarketAddress) 组合进行分组
	uidMarketToTokenBalanceMap := make(map[string][]*assetBiz.UserTokenBalanceEntity)
	for _, userPosition := range userPositions {
		key := userPosition.UID + ":" + userPosition.MarketAddress
		if _, ok := uidMarketToTokenBalanceMap[key]; !ok {
			uidMarketToTokenBalanceMap[key] = make([]*assetBiz.UserTokenBalanceEntity, 0)
		}
		uidMarketToTokenBalanceMap[key] = append(uidMarketToTokenBalanceMap[key], userPosition)
	}

	for key, tokenBalances := range uidMarketToTokenBalanceMap {
		uid := strings.Split(key, ":")[0]
		marketAddress := strings.Split(key, ":")[1]
		rspUserPosition := &marketcenter.BatchGetMarketUsersPositionsResponse_UserMarketPosition{
			Uid:           uid,
			MarketAddress: marketAddress,
			Positions:     make([]*marketcenter.BatchGetMarketUsersPositionsResponse_UserMarketPosition_Position, 0),
		}
		for _, tokenBalance := range tokenBalances {
			rspUserPosition.Positions = append(rspUserPosition.Positions, &marketcenter.BatchGetMarketUsersPositionsResponse_UserMarketPosition_Position{
				OptionAddress: tokenBalance.TokenAddress,
				Amount:        tokenBalance.Balance.String(),
				Decimal:       uint32(tokenBalance.Decimal),
			})
		}
		rsp.UserMarketPositions = append(rsp.UserMarketPositions, rspUserPosition)
	}
	return rsp, nil
}

func (s *MarketService) GetMarketTags(ctx context.Context, req *marketcenter.GetMarketTagsRequest) (*marketcenter.GetMarketTagsResponse, error) {
	c := common.NewBaseCtx(ctx, s.log)

	if req.Page <= 0 {
		req.Page = 1
	}
	if req.PageSize <= 0 {
		req.PageSize = 10
	} else if req.PageSize > 100 {
		req.PageSize = 100
	}

	marketTags, total, err := s.marketHandler.GetMarketTags(c, &marketBiz.MarketTagQuery{
		BaseQuery: base.BaseQuery{
			Limit:  int32(req.PageSize),
			Offset: int32((req.Page - 1) * req.PageSize),
		},
	})
	if err != nil {
		return nil, err
	}

	marketTagsList := make([]string, 0, len(marketTags))
	for _, tag := range marketTags {
		marketTagsList = append(marketTagsList, tag.TagName)
	}
	return &marketcenter.GetMarketTagsResponse{
		Total: uint32(total),
		Tags:  marketTagsList,
	}, nil
}

func (s *MarketService) BatchUpdateOptionPrice(ctx context.Context, req *marketcenter.BatchUpdateOptionPriceRequest) (*marketcenter.BatchUpdateOptionPriceResponse, error) {
	c := common.NewBaseCtx(ctx, s.log)

	optionTokenPriceEntityList := make([]*marketBiz.OptionTokenPriceEntity, 0, len(req.OptionPrices))
	for _, optionPrice := range req.OptionPrices {

		price, err := decimal.NewFromString(optionPrice.Price)
		if err != nil {
			return nil, err
		}

		optionTokenPriceEntityList = append(optionTokenPriceEntityList, &marketBiz.OptionTokenPriceEntity{
			TokenAddress:  optionPrice.OptionAddress,
			Price:         price,
			BlockNumber:   optionPrice.BlockNumber,
			BlockTime:     time.Unix(int64(optionPrice.BlockTime), 0),
			BaseTokenType: uint8(optionPrice.BaseTokenType),
			Decimals:      uint8(optionPrice.Decimal),
		})
	}
	err := s.marketHandler.BatchUpdateOptionPrice(c, optionTokenPriceEntityList)
	if err != nil {
		return nil, err
	}
	return &marketcenter.BatchUpdateOptionPriceResponse{}, nil
}

func (s *MarketService) ProcessMarketSettingEvent(ctx context.Context, req *marketcenter.ProcessMarketSettingEventRequest) (*marketcenter.ProcessMarketSettingEventResponse, error) {
	c := common.NewBaseCtx(ctx, s.log)

	// 更新市场状态 获胜结果 status AssertionId
	err := s.marketHandler.UpdateMarketByAddress(c, req.MarketAddress, map[string]interface{}{
		"status":       marketBiz.MarketStatusSettling,
		"result":       req.FinalOptionAddress,
		"assertion_id": req.AssertionId,
	})
	if err != nil {
		return nil, err
	}

	return &marketcenter.ProcessMarketSettingEventResponse{}, nil
}

func (s *MarketService) ProcessMarketAssertDisputedEvent(ctx context.Context, req *marketcenter.ProcessMarketAssertDisputedEventRequest) (*marketcenter.ProcessMarketAssertDisputedEventResponse, error) {
	c := common.NewBaseCtx(ctx, s.log)

	// 更新市场状态 final_option设置为空
	err := s.marketHandler.UpdateMarketByAddress(c, req.MarketAddress, map[string]interface{}{
		"status":       marketBiz.MarketStatusDisputed,
		"result":       "",
		"assertion_id": req.AssertionId,
	})
	if err != nil {
		return nil, err
	}

	return &marketcenter.ProcessMarketAssertDisputedEventResponse{}, nil
}

func (s *MarketService) ProcessMarketAssertionResolvedEvent(ctx context.Context, req *marketcenter.ProcessMarketAssertionResolvedEventRequest) (*marketcenter.ProcessMarketAssertionResolvedEventResponse, error) {
	c := common.NewBaseCtx(ctx, s.log)

	// 更新市场状态
	err := s.marketHandler.UpdateMarketByAddress(c, req.MarketAddress, map[string]interface{}{
		"status": marketBiz.MarketStatusEnd,
	})
	if err != nil {
		return nil, err
	}

	marketEntity, err := s.marketHandler.GetMarket(c, &marketBiz.MarketQuery{
		Address: req.MarketAddress,
	})
	if err != nil {
		return nil, err
	}
	if marketEntity == nil || marketEntity.Address == "" {
		return nil, errors.New(int(marketcenter.ErrorCode_DATABASE), "DATABASE_ERROR", "market not found")
	}

	optionEntityList, err := s.marketHandler.GetOptions(c, &marketBiz.OptionQuery{
		MarketAddress: req.MarketAddress,
	})
	if err != nil {
		return nil, err
	}

	var winOptionInfo *marketBiz.OptionEntity
	for _, option := range optionEntityList {
		if option.Address == marketEntity.Result {
			winOptionInfo = option
		}
	}
	if winOptionInfo == nil {
		return nil, errors.New(int(marketcenter.ErrorCode_INTERNAL), "INTERNAL_ERROR", "win option not found")
	}

	err = s.assetHandler.ProcessMarketEndInAssetHandler(c, req.MarketAddress, marketEntity.Result)
	if err != nil {
		return nil, err
	}
	optionTokenPriceEntityList := make([]*marketBiz.OptionTokenPriceEntity, 0, len(optionEntityList))

	winPrice := new(big.Int).SetInt64(int64(1))
	multiplier := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(winOptionInfo.Decimal)), nil)
	winPriceDecimal := new(big.Int).Mul(winPrice, multiplier)

	for _, option := range optionEntityList {
		optionTokenPriceEntityList = append(optionTokenPriceEntityList, &marketBiz.OptionTokenPriceEntity{
			TokenAddress: option.Address,
			Price: func() decimal.Decimal {
				if option.Address == marketEntity.Result {
					return decimal.NewFromBigInt(winPriceDecimal, 0)
				}
				return decimal.NewFromInt(0)
			}(),
			BlockNumber:   req.BlockNumber,
			BlockTime:     time.Now(),
			BaseTokenType: uint8(marketEntity.TokenType),
			Decimals:      uint8(option.Decimal),
		})
	}
	err = s.marketHandler.BatchUpdateOptionPrice(c, optionTokenPriceEntityList)
	if err != nil {
		return nil, err
	}

	go func(newCtx common.Ctx) {
		defer func() {
			if err := recover(); err != nil {
				newCtx.Log.Errorf("market end generate user notification error: %+v, stack: %s", err, string(debug.Stack()))
			}
		}()

		userPositionsMap, err := s.assetHandler.GetMarketEndUserPositionsMap(newCtx, req.MarketAddress, marketEntity.Result)
		if err != nil {
			newCtx.Log.Errorf("market end get user positions map error: %+v", err)
			return
		}

		notificationEntities := make([]*userBiz.UserNotificationEntity, 0, len(userPositionsMap))
		for uid, userPosition := range userPositionsMap {

			bizData := &userBiz.MarketRedeemNotificationEntity{
				MarketAddress: req.MarketAddress,
				MarketName:    marketEntity.Name,
				MarketDesc:    marketEntity.Description,
				MarketPicUrl:  marketEntity.PicUrl,

				OptionAddress: winOptionInfo.Address,
				OptionName:    winOptionInfo.Name,
				OptionDesc:    winOptionInfo.Description,
				OptionPicUrl:  winOptionInfo.PicUrl,
				Decimal:       int32(winOptionInfo.Decimal),

				Amount: "0",
			}
			if userPosition != nil && userPosition.TokenAddress == winOptionInfo.Address {
				bizData.Amount = userPosition.Balance.String()
			}
			bizJson, err := json.Marshal(bizData)
			if err != nil {
				newCtx.Log.Errorf("market end generate user notification error: %+v", err)
				return
			}
			notificationEntities = append(notificationEntities, &userBiz.UserNotificationEntity{
				UUID:     util.GenerateUUID(),
				UID:      uid,
				Type:     userBiz.NotificationTypeMarketDone,
				Category: uint8(userBiz.NotificationCategoryTrade),
				Status:   userBiz.NotificationStatusUnRead,
				BizJson:  json.RawMessage(bizJson),
			})
		}

		err = s.userHandler.BatchGenerateNewUserNotification(newCtx, notificationEntities)
		if err != nil {
			newCtx.Log.Errorf("market end generate user notification error: %+v", err)
			return
		}

	}(common.CloneBaseCtx(c, s.log))

	return &marketcenter.ProcessMarketAssertionResolvedEventResponse{}, nil
}

func (s *MarketService) GetMarketCategories(ctx context.Context, req *marketcenter.GetMarketCategoriesRequest) (*marketcenter.GetMarketCategoriesResponse, error) {
	c := common.NewBaseCtx(ctx, s.log)

	categoryList, total, err := s.marketHandler.GetCategoriesFromS3(c, uint8(req.BaseTokenType))
	if err != nil {
		return nil, err
	}

	return &marketcenter.GetMarketCategoriesResponse{
		Total: uint32(total),
		Categories: func() []*marketcenter.GetMarketCategoriesResponse_Category {
			categories := make([]*marketcenter.GetMarketCategoriesResponse_Category, 0, len(categoryList))
			for _, category := range categoryList {
				categories = append(categories, &marketcenter.GetMarketCategoriesResponse_Category{
					Name:   category.Name,
					Weight: category.Weight,
					Id:     category.Id,
				})
			}
			return categories
		}(),
	}, nil
}

func (s *MarketService) GetBanners(ctx context.Context, req *marketcenter.GetBannersRequest) (*marketcenter.GetBannersResponse, error) {
	c := common.NewBaseCtx(ctx, s.log)

	banners, _, err := s.marketHandler.GetBannersFromS3(c, uint8(req.BaseTokenType))
	if err != nil {
		return nil, err
	}

	rsp := &marketcenter.GetBannersResponse{}
	for _, banner := range banners {
		rsp.Banners = append(rsp.Banners, &marketcenter.GetBannersResponse_Banner{
			Url:    banner.Url,
			Image:  banner.Image,
			Weight: banner.Weight,
			Type:   banner.Type,
			Id:     banner.Id,
		})
	}
	return rsp, nil
}

func (s *MarketService) GetSections(ctx context.Context, req *marketcenter.GetSectionsRequest) (*marketcenter.GetSectionsResponse, error) {
	c := common.NewBaseCtx(ctx, s.log)

	sections, _, err := s.marketHandler.GetSectionsFromS3(c, uint8(req.BaseTokenType))
	if err != nil {
		return nil, err
	}

	rsp := &marketcenter.GetSectionsResponse{}
	for _, section := range sections {
		rspSections := &marketcenter.GetSectionsResponse_Section{
			Title:  section.Title,
			Color:  section.Color,
			Type:   section.Type,
			Weight: section.Weight,
			Id:     section.Id,
			Predictions: func() []*marketcenter.GetSectionsResponse_Section_Prediction {
				predictions := make([]*marketcenter.GetSectionsResponse_Section_Prediction, 0, len(section.Predictions))
				for _, prediction := range section.Predictions {
					predictions = append(predictions, &marketcenter.GetSectionsResponse_Section_Prediction{
						Prediction: prediction.Prediction,
						Weight:     prediction.Weight,
						Id:         prediction.Id,
					})
				}
				return predictions
			}(),
		}
		rsp.Sections = append(rsp.Sections, rspSections)
	}
	return rsp, nil
}

func (s *MarketService) UpdateMarketInfo(ctx context.Context, req *marketcenter.UpdateMarketInfoRequest) (*marketcenter.UpdateMarketInfoResponse, error) {
	c := common.NewBaseCtx(ctx, s.log)

	err := s.marketHandler.UpdateMarketInfoByS3Data(c, req.MarketAddress)
	if err != nil {
		return nil, err
	}

	return &marketcenter.UpdateMarketInfoResponse{}, nil

}

func (s *MarketService) GetUserTransactions(ctx context.Context, req *marketcenter.GetUserTransactionsRequest) (*marketcenter.GetUserTransactionsResponse, error) {
	c := common.NewBaseCtx(ctx, s.log)

	if req.Page <= 0 {
		req.Page = 1
	}
	if req.PageSize <= 0 {
		req.PageSize = 10
	} else if req.PageSize > 100 {
		req.PageSize = 100
	}

	transactions, total, err := s.assetHandler.GetUserTransactions(c, &assetBiz.SendTxQuery{
		UID:           req.Uid,
		BaseTokenType: uint8(req.BaseTokenType),
		BaseQuery: base.BaseQuery{
			Limit:  int32(req.PageSize),
			Offset: int32((req.Page - 1) * req.PageSize),
			Order:  "id desc",
		},
	})
	if err != nil {
		return nil, err
	}

	return &marketcenter.GetUserTransactionsResponse{
		Total:        uint32(total),
		Transactions: transactions,
	}, nil
}

func (s *MarketService) GetLeaderboard(ctx context.Context, req *marketcenter.GetLeaderboardRequest) (*marketcenter.GetLeaderboardResponse, error) {
	c := common.NewBaseCtx(ctx, s.log)

	// 参数验证和默认值设置
	if req.Page <= 0 {
		req.Page = 1
	}
	if req.PageSize <= 0 {
		req.PageSize = 10
	} else if req.PageSize > 100 {
		req.PageSize = 100
	}

	// 构建排行榜key
	leaderboardKey, err := s.buildLeaderboardKey(req.TimeInterval, req.BaseTokenType, req.SortType)
	if err != nil {
		return nil, err
	}

	c.Log.Infof("GetLeaderboard query: key=%s, page=%d, pageSize=%d", leaderboardKey, req.Page, req.PageSize)

	// 计算分页参数
	start := int64((req.Page - 1) * req.PageSize)
	stop := start + int64(req.PageSize) - 1

	// 获取排行榜数据（按分数从高到低）
	entries, err := s.assetHandler.GetLeaderboardEntries(c, leaderboardKey, start, stop)
	if err != nil {
		c.Log.Errorf("GetLeaderboard GetLeaderboardEntries error: %v", err)
		return nil, err
	}

	// 获取总数
	total, err := s.assetHandler.GetLeaderboardTotal(c, leaderboardKey, "-inf", "+inf")
	if err != nil {
		c.Log.Errorf("GetLeaderboard GetLeaderboardTotal error: %v", err)
		return nil, err
	}

	// 构建响应
	rsp := &marketcenter.GetLeaderboardResponse{
		Total:   uint32(total),
		Entries: make([]*marketcenter.GetLeaderboardResponse_Entry, 0, len(entries)),
	}

	// 获取小数位数
	var decimalValue uint32
	if req.BaseTokenType == marketcenter.BaseTokenType_BASE_TOKEN_TYPE_POINTS {
		decimalValue = s.confCustom.AssetTokens.Points.Decimals
	} else if req.BaseTokenType == marketcenter.BaseTokenType_BASE_TOKEN_TYPE_USDC {
		decimalValue = s.confCustom.AssetTokens.Usdc.Decimals
	} else {
		decimalValue = 6 // 默认值
	}

	for i, entry := range entries {
		scoreDecimal := decimal.NewFromFloat(entry.Score)
		rank := uint32(start + int64(i) + 1)
		rsp.Entries = append(rsp.Entries, &marketcenter.GetLeaderboardResponse_Entry{
			Uid:     entry.Member.(string),
			Score:   scoreDecimal.String(),
			Decimal: decimalValue,
			Rank:    rank,
		})
	}

	// 如果传了uid，查询用户自己的排行榜条目
	if req.Uid != "" {
		userRank, err := s.assetHandler.GetUserRank(c, leaderboardKey, req.Uid)
		if err != nil {
			c.Log.Errorf("GetLeaderboard GetUserRank error: %v", err)
			return nil, err
		}

		var userScore float64
		if userRank >= 0 {
			// 用户在排行榜中，获取其分数
			userScore, err = s.assetHandler.GetUserScore(c, leaderboardKey, req.Uid)
			if err != nil {
				c.Log.Errorf("GetLeaderboard GetUserScore error: %v", err)
				return nil, err
			}
		} else {
			// 用户不在排行榜中
			userRank = total
			userScore = 0
			if req.SortType == marketcenter.GetLeaderboardRequest_SORT_TYPE_PNL {
				userRank, err = s.assetHandler.GetLeaderboardTotal(c, leaderboardKey, "(0", "+inf")
				if err != nil {
					c.Log.Errorf("GetLeaderboard GetLeaderboardTotal error: %v", err)
					return nil, err
				}

			}
		}

		userScoreDecimal := decimal.NewFromFloat(userScore)
		userEntry := &marketcenter.GetLeaderboardResponse_Entry{
			Uid:     req.Uid,
			Score:   userScoreDecimal.String(),
			Decimal: decimalValue,
			Rank:    uint32(userRank + 1),
		}
		rsp.UserOwnEntry = userEntry
	}

	return rsp, nil
}

// buildLeaderboardKey 构建排行榜key
func (s *MarketService) buildLeaderboardKey(timeInterval marketcenter.GetLeaderboardRequest_TimeInterval, baseTokenType marketcenter.BaseTokenType, sortType marketcenter.GetLeaderboardRequest_SortType) (string, error) {
	baseTokenTypeUint := uint8(baseTokenType)

	// 根据排序类型选择对应的排行榜
	var leaderboardType string
	switch sortType {
	case marketcenter.GetLeaderboardRequest_SORT_TYPE_VOLUME:
		leaderboardType = "volume"
	case marketcenter.GetLeaderboardRequest_SORT_TYPE_TRADE_COUNT:
		leaderboardType = "trades"
	case marketcenter.GetLeaderboardRequest_SORT_TYPE_PNL:
		leaderboardType = "pnl"
	default:
		return "", errors.New(int(marketcenter.ErrorCode_PARAM), "PARAM_ERROR", "invalid sort type")
	}

	// 根据时间间隔构建key
	switch timeInterval {
	case marketcenter.GetLeaderboardRequest_TIME_INTERVAL_DAY:
		dayStr := util.GetDayStartTimeStr(time.Now().Unix())
		return fmt.Sprintf(assetBiz.LeaderboardKey, baseTokenTypeUint, leaderboardType, "daily-"+dayStr), nil
	case marketcenter.GetLeaderboardRequest_TIME_INTERVAL_WEEK:
		weekStr := util.GetWeekStartTimeStr(time.Now().Unix())
		return fmt.Sprintf(assetBiz.LeaderboardKey, baseTokenTypeUint, leaderboardType, "weekly-"+weekStr), nil
	case marketcenter.GetLeaderboardRequest_TIME_INTERVAL_MONTH:
		monthStr := util.GetMonthStartTimeStr(time.Now().Unix())
		return fmt.Sprintf(assetBiz.LeaderboardKey, baseTokenTypeUint, leaderboardType, "monthly-"+monthStr), nil
	case marketcenter.GetLeaderboardRequest_TIME_INTERVAL_ALL:
		return fmt.Sprintf(assetBiz.LeaderboardKey, baseTokenTypeUint, leaderboardType, "all-time"), nil
	default:
		return "", errors.New(int(marketcenter.ErrorCode_PARAM), "PARAM_ERROR", "invalid time interval")
	}
}

// ==================== CTF Event gRPC Handlers ====================

func (s *MarketService) GetEvent(ctx context.Context, req *marketcenter.GetEventRequest) (*marketcenter.GetEventResponse, error) {
	c := common.NewBaseCtx(ctx, s.log)

	query := &marketBiz.PredictionEventQuery{}
	if req.Id != "" {
		// 纯数字 → 按数据库 ID 查询；否则按 on-chain event_id 查询
		if dbId, parseErr := strconv.ParseUint(req.Id, 10, 64); parseErr == nil {
			query.DbId = uint(dbId)
		} else {
			query.EventId = req.Id
		}
	}

	event, err := s.marketHandler.GetEvent(c, query)
	if err != nil {
		return nil, err
	}

	return &marketcenter.GetEventResponse{
		Event: s.eventEntityToProto(event),
	}, nil
}

func (s *MarketService) ListEvents(ctx context.Context, req *marketcenter.ListEventsRequest) (*marketcenter.ListEventsResponse, error) {
	c := common.NewBaseCtx(ctx, s.log)

	query := &marketBiz.PredictionEventQuery{}
	if req.Status != marketcenter.EventStatus_EVENT_STATUS_UNSPECIFIED {
		query.Status = uint8(req.Status)
	}
	if req.Page > 0 {
		query.Offset = int32((req.Page - 1) * req.PageSize)
	}
	if req.PageSize > 0 {
		query.Limit = int32(req.PageSize)
	}

	// 排序
	switch req.SortType {
	case marketcenter.ListEventsRequest_SORT_TYPE_LATEST:
		query.Order = "created_at DESC"
	case marketcenter.ListEventsRequest_SORT_TYPE_VOLUME:
		query.Order = "created_at DESC" // TODO: 未来可聚合子市场 volume
	default:
		query.Order = "created_at DESC"
	}

	events, total, err := s.marketHandler.ListEvents(c, query)
	if err != nil {
		return nil, err
	}

	protoEvents := make([]*marketcenter.PredictionEvent, 0, len(events))
	for _, e := range events {
		protoEvents = append(protoEvents, s.eventEntityToProto(e))
	}

	return &marketcenter.ListEventsResponse{
		Total:  uint32(total),
		Events: protoEvents,
	}, nil
}

func (s *MarketService) CreateEvent(ctx context.Context, req *marketcenter.CreateEventRequest) (*marketcenter.CreateEventResponse, error) {
	c := common.NewBaseCtx(ctx, s.log)

	entity := &marketBiz.PredictionEventEntity{
		EventId:          req.EventId,
		Title:            req.Title,
		OutcomeSlotCount: req.OutcomeSlotCount,
		Collateral:       req.Collateral,
		MetadataHash:     req.MetadataHash,
		Status:           marketBiz.EventStatusActive,
	}

	if err := s.marketHandler.CreateEvent(c, entity); err != nil {
		return nil, err
	}

	return &marketcenter.CreateEventResponse{
		Id: fmt.Sprintf("%d", entity.Id),
	}, nil
}

// eventEntityToProto converts a PredictionEventEntity to proto PredictionEvent
func (s *MarketService) eventEntityToProto(entity *marketBiz.PredictionEventEntity) *marketcenter.PredictionEvent {
	pe := &marketcenter.PredictionEvent{
		Id:               fmt.Sprintf("%d", entity.Id),
		EventId:          entity.EventId,
		Title:            entity.Title,
		OutcomeSlotCount: entity.OutcomeSlotCount,
		Collateral:       entity.Collateral,
		Status:           marketcenter.EventStatus(entity.Status),
		MetadataHash:     entity.MetadataHash,
		CreatedAt:        uint32(entity.CreatedAt.Unix()),
		UpdatedAt:        uint32(entity.UpdatedAt.Unix()),
	}

	if entity.Markets != nil {
		markets := make([]*marketcenter.PredictionEvent_Market, 0, len(entity.Markets))
		for _, m := range entity.Markets {
			protoMarket := &marketcenter.PredictionEvent_Market{
				Address:          m.Address,
				Name:             m.Name,
				PicUrl:           m.PicUrl,
				Description:      m.Description,
				Status:           uint32(m.Status),
				ParticipantsCount: uint32(m.ParticipantsCount),
				Volume:           m.Volume.String(),
				Decimal:          uint32(6),
				CreatedAt:        uint32(m.CreatedAt.Unix()),
				Deadline:         uint32(m.Deadline),
				Result:           m.Result,
				ConditionId:      m.ConditionId,
				QuestionId:       m.QuestionId,
			}

			if m.Options != nil {
				options := make([]*marketcenter.PredictionEvent_Market_Option, 0, len(m.Options))
				for _, opt := range m.Options {
					protoOpt := &marketcenter.PredictionEvent_Market_Option{
						Address:     opt.Address,
						Name:        opt.Name,
						Symbol:      opt.Symbol,
						PicUrl:      opt.PicUrl,
						Decimal:     uint32(opt.Decimal),
						Index:       opt.Index,
						Description: opt.Description,
						PositionId:  opt.PositionId,
					}
					if opt.OptionTokenPrice != nil {
						protoOpt.Price = opt.OptionTokenPrice.Price.String()
					}
					options = append(options, protoOpt)
				}
				protoMarket.Options = options
			}

			markets = append(markets, protoMarket)
		}
		pe.Markets = markets
	}

	return pe
}

// ============================================
// CTF APMM 交易桩 (TRADING_NOT_READY)
// ============================================

func (s *MarketService) CTFSwap(ctx context.Context, req *marketcenter.CTFSwapRequest) (*marketcenter.CTFSwapResponse, error) {
	return nil, errors.New(int(marketcenterPb.ErrorCode_TRADING_NOT_READY), "CTF Trading Not Ready", "CTF trading is not yet available")
}

func (s *MarketService) CTFDepositLiquidity(ctx context.Context, req *marketcenter.CTFDepositLiquidityRequest) (*marketcenter.CTFDepositLiquidityResponse, error) {
	return nil, errors.New(int(marketcenterPb.ErrorCode_TRADING_NOT_READY), "CTF Trading Not Ready", "CTF trading is not yet available")
}

func (s *MarketService) CTFWithdrawLiquidity(ctx context.Context, req *marketcenter.CTFWithdrawLiquidityRequest) (*marketcenter.CTFWithdrawLiquidityResponse, error) {
	return nil, errors.New(int(marketcenterPb.ErrorCode_TRADING_NOT_READY), "CTF Trading Not Ready", "CTF trading is not yet available")
}

func (s *MarketService) CTFRedeemPosition(ctx context.Context, req *marketcenter.CTFRedeemPositionRequest) (*marketcenter.CTFRedeemPositionResponse, error) {
	return nil, errors.New(int(marketcenterPb.ErrorCode_TRADING_NOT_READY), "CTF Trading Not Ready", "CTF trading is not yet available")
}

func (s *MarketService) CTFGetSwapPrice(ctx context.Context, req *marketcenter.CTFGetSwapPriceRequest) (*marketcenter.CTFGetSwapPriceResponse, error) {
	return nil, errors.New(int(marketcenterPb.ErrorCode_TRADING_NOT_READY), "CTF Trading Not Ready", "CTF trading is not yet available")
}

// ============================================
// CTF 链上事件处理桩 (log + return OK)
// ============================================

func (s *MarketService) ProcessCTFConditionEvent(ctx context.Context, req *marketcenter.ProcessCTFConditionEventRequest) (*marketcenter.ProcessCTFConditionEventResponse, error) {
	c := common.NewBaseCtx(ctx, s.log)
	c.Log.Infof("ProcessCTFConditionEvent stub: type=%v conditionId=%s txHash=%s", req.EventType, req.ConditionId, req.TxHash)
	return &marketcenter.ProcessCTFConditionEventResponse{}, nil
}

func (s *MarketService) ProcessCTFPositionEvent(ctx context.Context, req *marketcenter.ProcessCTFPositionEventRequest) (*marketcenter.ProcessCTFPositionEventResponse, error) {
	c := common.NewBaseCtx(ctx, s.log)
	c.Log.Infof("ProcessCTFPositionEvent stub: type=%v stakeholder=%s conditionId=%s txHash=%s", req.EventType, req.Stakeholder, req.ConditionId, req.TxHash)
	return &marketcenter.ProcessCTFPositionEventResponse{}, nil
}

func (s *MarketService) ProcessCTFTransferEvent(ctx context.Context, req *marketcenter.ProcessCTFTransferEventRequest) (*marketcenter.ProcessCTFTransferEventResponse, error) {
	c := common.NewBaseCtx(ctx, s.log)
	c.Log.Infof("ProcessCTFTransferEvent stub: type=%v from=%s to=%s txHash=%s", req.EventType, req.From, req.To, req.TxHash)
	return &marketcenter.ProcessCTFTransferEventResponse{}, nil
}

func (s *MarketService) ProcessCTFMarketResolvedEvent(ctx context.Context, req *marketcenter.ProcessCTFMarketResolvedEventRequest) (*marketcenter.ProcessCTFMarketResolvedEventResponse, error) {
	c := common.NewBaseCtx(ctx, s.log)
	c.Log.Infof("ProcessCTFMarketResolvedEvent stub: market=%s txHash=%s", req.MarketAddress, req.TxHash)
	return &marketcenter.ProcessCTFMarketResolvedEventResponse{}, nil
}

func (s *MarketService) ProcessCTFLiquidityEvent(ctx context.Context, req *marketcenter.ProcessCTFLiquidityEventRequest) (*marketcenter.ProcessCTFLiquidityEventResponse, error) {
	c := common.NewBaseCtx(ctx, s.log)
	c.Log.Infof("ProcessCTFLiquidityEvent stub: type=%v market=%s user=%s txHash=%s", req.EventType, req.MarketAddress, req.UserAddress, req.TxHash)
	return &marketcenter.ProcessCTFLiquidityEventResponse{}, nil
}
