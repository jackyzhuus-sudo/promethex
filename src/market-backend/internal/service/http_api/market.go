package http_api

import (
	"context"
	"fmt"
	"market-backend/internal/pkg"
	"market-backend/internal/pkg/util"
	marketcenterpb "market-proto/proto/market-service/marketcenter/v1"
	usercenterpb "market-proto/proto/market-service/usercenter/v1"

	apipb "market-proto/proto/market-backend/v1"

	"github.com/go-kratos/kratos/v2/errors"
)

func (s *HttpApiService) GetHotMarkets(ctx context.Context, req *apipb.GetHotMarketsRequest) (*apipb.GetHotMarketsReply, error) {
	c := util.NewBaseCtx(ctx, s.logger)
	hotMarketsRsp, err := s.data.RpcClient.MarketcenterClient.GetHotMarkets(ctx, &marketcenterpb.GetHotMarketsRequest{
		BaseTokenAddress: util.GetBaseTokenAddressFromCtx(ctx),
	})
	if err != nil {
		c.Log.Errorf("rpc GetHotMarkets failed, err: [%+v]", err)
		return nil, err
	}
	marketList := make([]*apipb.GetHotMarketsReply_MarketInfo, 0)
	for _, market := range hotMarketsRsp.Markets {
		marketInfo := &apipb.GetHotMarketsReply_MarketInfo{
			MarketName:        market.Name,
			MarketAddress:     market.Address,
			MarketPicUrl:      market.PicUrl,
			MarketDescription: market.Description,
			MarketStatus:      market.Status,
			BaseTokenAddress: market.BaseTokenAddress,
		}
		marketList = append(marketList, marketInfo)
	}
	return &apipb.GetHotMarketsReply{
		MarketList: marketList,
	}, nil
}

func (s *HttpApiService) GetFollowMarkets(ctx context.Context, req *apipb.GetFollowMarketsRequest) (*apipb.GetFollowMarketsReply, error) {

	c := util.NewBaseCtx(ctx, s.logger)

	uid := util.GetUidFromCtx(ctx)
	if uid == "" {
		return nil, pkg.ErrParam
	}
	followedMarketsRsp, err := s.data.RpcClient.MarketcenterClient.GetFollowedMarkets(ctx, &marketcenterpb.GetFollowedMarketsRequest{
		Uid:           uid,
		Page:          req.Page,
		PageSize:      req.PageSize,
		BaseTokenAddress: util.GetBaseTokenAddressFromCtx(ctx),
	})
	if err != nil {
		c.Log.Errorf("rpc GetFollowedMarkets failed, err: [%+v]", err)
		return nil, err
	}
	marketList := make([]*apipb.GetFollowMarketsReply_MarketInfo, 0)
	for _, market := range followedMarketsRsp.Markets {
		marketInfo := &apipb.GetFollowMarketsReply_MarketInfo{
			MarketAddress:     market.Address,
			MarketName:        market.Name,
			MarketPicUrl:      market.PicUrl,
			MarketDescription: market.Description,
			MarketStatus:      market.Status,
			Deadline:          market.Deadline,
			BaseTokenAddress: market.BaseTokenAddress,
		}
		marketList = append(marketList, marketInfo)
	}
	return &apipb.GetFollowMarketsReply{
		Total:      followedMarketsRsp.Total,
		MarketList: marketList,
	}, nil
}

func (s *HttpApiService) GetHoldingPositionsMarket(ctx context.Context, req *apipb.GetHoldingPositionsMarketRequest) (*apipb.GetHoldingPositionsMarketReply, error) {
	c := util.NewBaseCtx(ctx, s.logger)
	holdingPositionsMarketRsp, err := s.data.RpcClient.MarketcenterClient.GetHoldingPositionsMarkets(ctx, &marketcenterpb.GetHoldingPositionsMarketsRequest{
		BaseTokenAddress: util.GetBaseTokenAddressFromCtx(ctx),
		Uid:           req.Uid,
		Page:          req.Page,
		PageSize:      req.PageSize,
	})
	if err != nil {
		c.Log.Errorf("rpc GetHoldingPositionsMarket failed, err: [%+v]", err)
		return nil, err
	}
	marketList := make([]*apipb.GetHoldingPositionsMarketReply_Market, 0)
	for _, market := range holdingPositionsMarketRsp.Markets {
		marketInfo := &apipb.GetHoldingPositionsMarketReply_Market{
			Name:                    market.Name,
			Address:                 market.Address,
			PicUrl:                  market.PicUrl,
			Description:             market.Description,
			Status:                  market.Status,
			MarketVolume:            market.MarketVolume,
			MarketParticipantsCount: market.MarketParticipantsCount,
			BaseTokenAddress: market.BaseTokenAddress,
			UserMarketTotalValue:    market.UserMarketTotalValue,
			Positions: func() []*apipb.GetHoldingPositionsMarketReply_Market_Position {
				positions := make([]*apipb.GetHoldingPositionsMarketReply_Market_Position, 0)
				for _, position := range market.Positions {
					positions = append(positions, &apipb.GetHoldingPositionsMarketReply_Market_Position{
						TokenAddress: position.TokenAddress,
						TokenName:    position.TokenName,
						Description:  position.TokenDescription,
						Decimal:      position.Decimal,
						TokenSymbol:  position.TokenSymbol,
						TokenPicUrl:  position.TokenPicUrl,
						Balance:      position.Balance,
					})
				}
				return positions
			}(),
		}
		marketList = append(marketList, marketInfo)
	}
	return &apipb.GetHoldingPositionsMarketReply{
		Total:      holdingPositionsMarketRsp.Total,
		TotalValue: holdingPositionsMarketRsp.TotalValue,
		Markets:    marketList,
	}, nil
}

func (s *HttpApiService) FollowMarket(ctx context.Context, req *apipb.FollowMarketRequest) (*apipb.FollowMarketReply, error) {
	c := util.NewBaseCtx(ctx, s.logger)
	uid := util.GetUidFromCtx(ctx)
	if uid == "" {
		return nil, pkg.ErrParam
	}

	_, err := s.data.RpcClient.MarketcenterClient.UpdateUserMarketFollowStatus(ctx, &marketcenterpb.UpdateUserMarketFollowStatusRequest{
		Uid:           uid,
		MarketAddress: req.MarketAddress,
		Status:        marketcenterpb.UpdateUserMarketFollowStatusRequest_STATUS_FOLLOW,
	})
	if err != nil {
		c.Log.Errorf("rpc UpdateUserMarketFollowStatus failed, err: [%+v]", err)
		return nil, err
	}

	return &apipb.FollowMarketReply{}, nil
}

func (s *HttpApiService) UnfollowMarket(ctx context.Context, req *apipb.UnfollowMarketRequest) (*apipb.UnfollowMarketReply, error) {
	c := util.NewBaseCtx(ctx, s.logger)
	uid := util.GetUidFromCtx(ctx)
	if uid == "" {
		return nil, pkg.ErrParam
	}

	_, err := s.data.RpcClient.MarketcenterClient.UpdateUserMarketFollowStatus(ctx, &marketcenterpb.UpdateUserMarketFollowStatusRequest{
		Uid:           uid,
		MarketAddress: req.MarketAddress,
		Status:        marketcenterpb.UpdateUserMarketFollowStatusRequest_STATUS_UNFOLLOW,
	})
	if err != nil {
		c.Log.Errorf("rpc UpdateUserMarketFollowStatus failed, err: [%+v]", err)
		return nil, err
	}

	return &apipb.UnfollowMarketReply{}, nil
}

func (s *HttpApiService) GetPaymasterData(ctx context.Context, req *apipb.GetPaymasterDataRequest) (*apipb.GetPaymasterDataReply, error) {
	c := util.NewBaseCtx(ctx, s.logger)
	uid := util.GetUidFromCtx(ctx)
	if uid == "" {
		return nil, pkg.ErrParam
	}
	paymasterDataRsp, err := s.data.RpcClient.MarketcenterClient.GetPayMasterData(ctx, &marketcenterpb.GetPayMasterDataRequest{
		UserOperation: &marketcenterpb.UserOperation{
			Sender:                        req.UserOperation.Sender,
			Nonce:                         req.UserOperation.Nonce,
			InitCode:                      req.UserOperation.InitCode,
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
	})
	if err != nil {
		c.Log.Errorf("rpc GetPaymasterData failed, err: [%+v]", err)
		return nil, err
	}

	return &apipb.GetPaymasterDataReply{
		Paymaster:                     paymasterDataRsp.Paymaster,
		PaymasterData:                 paymasterDataRsp.PaymasterData,
		PaymasterVerificationGasLimit: paymasterDataRsp.PaymasterVerificationGasLimit,
		PreVerificationGas:            paymasterDataRsp.PreVerificationGas,
		VerificationGasLimit:          paymasterDataRsp.VerificationGasLimit,
		CallGasLimit:                  paymasterDataRsp.CallGasLimit,
	}, nil
}

// TODO 限流 用户 + 接口维度
func (s *HttpApiService) PlaceOrder(ctx context.Context, req *apipb.PlaceOrderRequest) (*apipb.PlaceOrderReply, error) {
	c := util.NewBaseCtx(ctx, s.logger)
	uid := util.GetUidFromCtx(ctx)
	if uid == "" {
		return nil, pkg.ErrParam
	}

	placeOrderRsp, err := s.data.RpcClient.MarketcenterClient.PlaceOrder(ctx, &marketcenterpb.PlaceOrderRequest{
		Uid:              uid,
		MarketAddress:    req.MarketAddress,
		OptionAddress:    req.OptionAddress,
		Side:             marketcenterpb.PlaceOrderRequest_Side(req.Side),
		Price:            req.Price,
		Amount:           req.Amount,
		MinReceiveAmount: req.MinReceiveAmount,
		Deadline:         req.Deadline,
		UserOperation: &marketcenterpb.UserOperation{
			Sender:                        req.UserOperation.Sender,
			Nonce:                         req.UserOperation.Nonce,
			InitCode:                      req.UserOperation.InitCode,
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
	})
	if err != nil {
		c.Log.Errorf("rpc PlaceOrder failed, err: [%+v]", err)
		return nil, err
	}

	return &apipb.PlaceOrderReply{
		OpHash:    placeOrderRsp.OpHash,
		OrderUuid: placeOrderRsp.OrderUuid,
	}, nil
}

func (s *HttpApiService) ClaimMarketResult(ctx context.Context, req *apipb.ClaimMarketResultRequest) (*apipb.ClaimMarketResultReply, error) {
	c := util.NewBaseCtx(ctx, s.logger)
	uid := util.GetUidFromCtx(ctx)
	if uid == "" {
		return nil, pkg.ErrParam
	}

	claimMarketResultRsp, err := s.data.RpcClient.MarketcenterClient.ClaimMarketResult(ctx, &marketcenterpb.ClaimMarketResultRequest{
		Uid:           uid,
		MarketAddress: req.MarketAddress,
		OptionAddress: req.OptionAddress,
		UserOperation: &marketcenterpb.UserOperation{
			Sender:                        req.UserOperation.Sender,
			Nonce:                         req.UserOperation.Nonce,
			InitCode:                      req.UserOperation.InitCode,
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
	})
	if err != nil {
		c.Log.Errorf("rpc ClaimMarketResult failed, err: [%+v]", err)
		return nil, err
	}

	return &apipb.ClaimMarketResultReply{
		OpHash: claimMarketResultRsp.OpHash,
	}, nil
}

func (s *HttpApiService) TransferBaseToken(ctx context.Context, req *apipb.TransferBaseTokenRequest) (*apipb.TransferBaseTokenReply, error) {
	c := util.NewBaseCtx(ctx, s.logger)
	uid := util.GetUidFromCtx(ctx)
	if uid == "" {
		return nil, pkg.ErrParam
	}

	TransferBaseTokenRsp, err := s.data.RpcClient.MarketcenterClient.TransferBaseToken(ctx, &marketcenterpb.TransferBaseTokenRequest{
		Uid:           uid,
		BaseTokenAddress: util.GetBaseTokenAddressFromCtx(ctx),
		Amount:        req.Amount,
		ToAddress:     req.ToAddress, // 接收地址
		UserOperation: &marketcenterpb.UserOperation{
			Sender:                        req.UserOperation.Sender,
			Nonce:                         req.UserOperation.Nonce,
			InitCode:                      req.UserOperation.InitCode,
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
	})
	if err != nil {
		c.Log.Errorf("rpc TransferBaseToken failed, err: [%+v]", err)
		return nil, err
	}

	return &apipb.TransferBaseTokenReply{
		OpHash: TransferBaseTokenRsp.OpHash,
	}, nil
}

func (s *HttpApiService) GetMarketTrades(ctx context.Context, req *apipb.GetMarketTradesRequest) (*apipb.GetMarketTradesReply, error) {
	c := util.NewBaseCtx(ctx, s.logger)
	rsp := &apipb.GetMarketTradesReply{
		Total:  0,
		Orders: make([]*apipb.GetMarketTradesReply_Order, 0),
	}

	GetMarketTradesRsp, err := s.data.RpcClient.MarketcenterClient.GetMarketTrades(ctx, &marketcenterpb.GetMarketTradesRequest{
		Address:  req.MarketAddress,
		Page:     req.Page,
		PageSize: req.PageSize,
	})
	if err != nil {
		c.Log.Errorf("rpc GetMarketTrades failed, err: [%+v]", err)
		return nil, err
	}

	if len(GetMarketTradesRsp.Orders) == 0 {
		return rsp, nil
	}
	uidList := make([]string, 0)
	for _, order := range GetMarketTradesRsp.Orders {
		uidList = append(uidList, order.Uid)
	}
	uidList = util.RemoveDuplicate(uidList)
	userInfos, err := s.data.RpcClient.UsercenterClient.GetUserInfosByUids(ctx, &usercenterpb.GetUserInfosByUidsRequest{
		Uids: uidList,
	})
	if err != nil {
		c.Log.Errorf("rpc GetUserInfosByUids failed, err: [%+v]", err)
		return nil, err
	}
	uidToUserInfoMap := make(map[string]*usercenterpb.GetUserInfosByUidsReply_UserInfo)
	for _, userInfo := range userInfos.UserInfos {
		uidToUserInfoMap[userInfo.Uid] = userInfo
	}

	for _, order := range GetMarketTradesRsp.Orders {
		orderInfo := &apipb.GetMarketTradesReply_Order{
			Uuid:          order.Uuid,
			Uid:           order.Uid,
			Side:          order.Side,
			Amount:        order.Amount,
			ReceiveAmount: order.ReceiveAmount,
			Timestamp:     order.Timestamp,
			DealPrice:     order.DealPrice,
		}
		if order.Option != nil {
			orderInfo.Option = &apipb.GetMarketTradesReply_Order_Option{
				Address:     order.Option.Address,
				Name:        order.Option.Name,
				Description: order.Option.Description,
				Decimal:     order.Option.Decimal,
				Symbol:      order.Option.Symbol,
				PicUrl:      order.Option.PicUrl,
			}
		}
		userInfo, ok := uidToUserInfoMap[order.Uid]
		if ok {
			orderInfo.UserName = userInfo.Name
			orderInfo.UserAvatarUrl = userInfo.Avatar
		}
		rsp.Orders = append(rsp.Orders, orderInfo)
	}

	rsp.Total = GetMarketTradesRsp.Total
	return rsp, nil
}

func (s *HttpApiService) GetUserTrades(ctx context.Context, req *apipb.GetUserTradesRequest) (*apipb.GetUserTradesReply, error) {
	c := util.NewBaseCtx(ctx, s.logger)
	uid := req.Uid
	if uid == "" {
		uid = util.GetUidFromCtx(ctx)
	}
	GetUserTradesRsp, err := s.data.RpcClient.MarketcenterClient.GetUserTrades(ctx, &marketcenterpb.GetUserTradesRequest{
		Uid:           uid,
		MarketAddress: req.MarketAddress,
		Page:          req.Page,
		PageSize:      req.PageSize,
		BaseTokenAddress: util.GetBaseTokenAddressFromCtx(ctx),
	})
	if err != nil {
		c.Log.Errorf("rpc GetUserTrades failed, err: [%+v]", err)
		return nil, err
	}

	rspTrades := make([]*apipb.GetUserTradesReply_Order, 0)
	for _, trade := range GetUserTradesRsp.Orders {
		one := &apipb.GetUserTradesReply_Order{
			Uuid:          trade.Uuid,
			Uid:           trade.Uid,
			Side:          trade.Side,
			Amount:        trade.Amount,
			ReceiveAmount: trade.ReceiveAmount,
			Timestamp:     trade.Timestamp,
			DealPrice:     trade.DealPrice,
			BaseTokenAddress: trade.BaseTokenAddress,
		}
		if trade.Option != nil {
			one.Option = &apipb.GetUserTradesReply_Order_Option{
				Address:           trade.Option.Address,
				Name:              trade.Option.Name,
				Description:       trade.Option.Description,
				Decimal:           trade.Option.Decimal,
				Symbol:            trade.Option.Symbol,
				PicUrl:            trade.Option.PicUrl,
				MarketAddress:     trade.Option.MarketAddress,
				MarketName:        trade.Option.MarketName,
				MarketPicUrl:      trade.Option.MarketPicUrl,
				MarketDescription: trade.Option.MarketDescription,
				ParticipantsCount: trade.Option.MarketParticipantsCount,
			}
		}
		rspTrades = append(rspTrades, one)
	}

	return &apipb.GetUserTradesReply{
		Total:  GetUserTradesRsp.Total,
		Orders: rspTrades,
	}, nil
}

func (s *HttpApiService) GetMarketDetail(ctx context.Context, req *apipb.GetMarketDetailRequest) (*apipb.GetMarketDetailReply, error) {

	c := util.NewBaseCtx(ctx, s.logger)

	GetMarketDetailRsp, err := s.data.RpcClient.MarketcenterClient.GetMarketDetail(ctx, &marketcenterpb.GetMarketDetailRequest{
		Address: req.MarketAddress,
		Uid:     util.GetUidFromCtx(ctx),
	})
	if err != nil {
		c.Log.Errorf("rpc GetMarketDetail failed, err: [%+v]", err)
		return nil, err
	}

	if GetMarketDetailRsp.Address == "" {
		return nil, errors.NotFound("NOT_FOUND", "market not found")
	}

	return &apipb.GetMarketDetailReply{
		MarketAddress:       GetMarketDetailRsp.Address,
		MarketName:          GetMarketDetailRsp.Name,
		MarketPicUrl:        GetMarketDetailRsp.PicUrl,
		MarketDescription:   GetMarketDetailRsp.Description,
		Volume:              GetMarketDetailRsp.Volume,
		Decimal:             uint32(GetMarketDetailRsp.Decimal),
		ParticipantsCount:   uint32(GetMarketDetailRsp.ParticipantsCount),
		ResultOptionAddress: GetMarketDetailRsp.ResultOptionAddress,
		Rule:                GetMarketDetailRsp.Rules,
		Deadline:            uint64(GetMarketDetailRsp.Deadline),
		RuleFileKey:         GetMarketDetailRsp.RulesFileUrl,
		MarketStatus:        apipb.GetMarketDetailReply_MaketStatus(GetMarketDetailRsp.Status),
		IsFollowed:          apipb.IsFollowed(GetMarketDetailRsp.IsFollowed),
		IsClaim:             apipb.IsClaim(GetMarketDetailRsp.IsClaim),
		BaseTokenAddress:    GetMarketDetailRsp.BaseTokenAddress,
		EventId:             GetMarketDetailRsp.EventId,
		ConditionId:         GetMarketDetailRsp.ConditionId,
		QuestionId:          GetMarketDetailRsp.QuestionId,
		OutcomeSlotCount:    GetMarketDetailRsp.OutcomeSlotCount,
		OptionList: func() []*apipb.GetMarketDetailReply_OptionInfo {
			optionList := make([]*apipb.GetMarketDetailReply_OptionInfo, 0)
			for _, option := range GetMarketDetailRsp.Options {
				optionList = append(optionList, &apipb.GetMarketDetailReply_OptionInfo{
					OptionAddress: option.Address,
					OptionName:    option.Name,
					OptionSymbol:  option.Symbol,
					OptionPicUrl:  option.PicUrl,
					Price:         option.Price,
					Decimal:       option.Decimal,
					Description:   option.Description,
					PositionId:    option.PositionId,
				})
			}
			return optionList
		}(),
	}, nil
}

func (s *HttpApiService) GetUserPositions(ctx context.Context, req *apipb.GetUserPositionsRequest) (*apipb.GetUserPositionsReply, error) {
	c := util.NewBaseCtx(ctx, s.logger)

	uid := req.Uid
	if uid == "" {
		uid = util.GetUidFromCtx(ctx)
	}

	GetUserPositionsRsp, err := s.data.RpcClient.MarketcenterClient.GetUserPositions(ctx, &marketcenterpb.GetUserPositionsRequest{
		Uid:           uid,
		MarketAddress: req.MarketAddress,
		BaseTokenAddress: util.GetBaseTokenAddressFromCtx(ctx),
		Page:          req.Page,
		PageSize:      req.PageSize,
		OptionAddress: req.OptionAddress,
	})
	if err != nil {
		c.Log.Errorf("rpc GetUserPositions failed, err: [%+v]", err)
		return nil, err
	}
	rspPositions := make([]*apipb.GetUserPositionsReply_Position, 0)
	for _, position := range GetUserPositionsRsp.Positions {
		rspPositions = append(rspPositions, &apipb.GetUserPositionsReply_Position{
			MarketAddress:     position.MarketAddress,
			MarketName:        position.MarketName,
			MarketDescription: position.MarketDescription,
			MarketPicUrl:      position.MarketPicUrl,
			OptionAddress:     position.OptionAddress,
			BaseTokenAddress: position.BaseTokenAddress,
			OptionName:        position.OptionName,
			OptionDecimal:     position.OptionDecimal,
			OptionSymbol:      position.OptionSymbol,
			OptionPicUrl:      position.OptionPicUrl,
			OptionDescription: position.OptionDescription,
			Balance:           position.Balance,
			EntryPrice:        position.EntryPrice,
			MarketPirce:       position.MarketPirce,
			BetValue:          position.BetValue,
			CurrentValue:      position.CurrentValue,
			Pnl:               position.Pnl,
			ToWin:             position.ToWin,
			Deadline:          position.Deadline,
			Status:            position.Status,
			IsClaimed:         position.IsClaimed,
		})
	}
	return &apipb.GetUserPositionsReply{
		Total:     GetUserPositionsRsp.Total,
		Positions: rspPositions,
	}, nil
}

func (s *HttpApiService) GetUserAssetHistory(ctx context.Context, req *apipb.GetUserAssetHistoryRequest) (*apipb.GetUserAssetHistoryReply, error) {
	c := util.NewBaseCtx(ctx, s.logger)
	GetUserAssetHistoryRsp, err := s.data.RpcClient.MarketcenterClient.GetUserAssetHistory(ctx, &marketcenterpb.GetUserAssetHistoryRequest{
		Uid:           req.Uid,
		BaseTokenAddress: util.GetBaseTokenAddressFromCtx(ctx),
		TimeInterval:  marketcenterpb.GetUserAssetHistoryRequest_TimeInterval(req.TimeInterval),
	})
	if err != nil {
		c.Log.Errorf("rpc GetUserAssetHistory failed, err: [%+v]", err)
		return nil, err
	}

	rspSnapshots := make([]*apipb.GetUserAssetHistoryReply_OneSnapshot, 0)
	for _, snapshot := range GetUserAssetHistoryRsp.Snapshots {
		rspSnapshots = append(rspSnapshots, &apipb.GetUserAssetHistoryReply_OneSnapshot{
			Value:     snapshot.Value,
			Balance:   snapshot.Balance,
			Portfolio: snapshot.Portfolio,
			Pnl:       snapshot.Pnl,
			Timestamp: snapshot.Timestamp,
		})
	}
	return &apipb.GetUserAssetHistoryReply{
		Total:         GetUserAssetHistoryRsp.Total,
		Snapshots:     rspSnapshots,
		BaseTokenAddress: GetUserAssetHistoryRsp.BaseTokenAddress,
		Decimal:       uint32(GetUserAssetHistoryRsp.Decimal),
	}, nil
}
func (s *HttpApiService) GetMarketOptionPriceHistory(ctx context.Context, req *apipb.GetMarketOptionPriceHistoryRequest) (*apipb.GetMarketOptionPriceHistoryReply, error) {
	c := util.NewBaseCtx(ctx, s.logger)
	GetMarketOptionPriceHistoryRsp, err := s.data.RpcClient.MarketcenterClient.GetMarketOptionPriceHistory(ctx, &marketcenterpb.GetMarketOptionPriceHistoryRequest{
		MarketAddress: req.MarketAddress,
		TimeInterval:  marketcenterpb.GetMarketOptionPriceHistoryRequest_TimeInterval(req.TimeInterval),
	})
	if err != nil {
		c.Log.Errorf("rpc GetMarketOptionPriceHistory failed, err: [%+v]", err)
		return nil, err
	}

	rspSnapshots := make([]*apipb.GetMarketOptionPriceHistoryReply_OneSnapshot, 0)
	for _, snapshot := range GetMarketOptionPriceHistoryRsp.Snapshots {
		rspSnapshots = append(rspSnapshots, &apipb.GetMarketOptionPriceHistoryReply_OneSnapshot{
			Timestamp: snapshot.Timestamp,
			TokenPrices: func() []*apipb.GetMarketOptionPriceHistoryReply_OneSnapshot_TokenPrice {
				tokenPrices := make([]*apipb.GetMarketOptionPriceHistoryReply_OneSnapshot_TokenPrice, 0)
				for _, tokenPrice := range snapshot.TokenPrices {
					tokenPrices = append(tokenPrices, &apipb.GetMarketOptionPriceHistoryReply_OneSnapshot_TokenPrice{
						TokenAddress: tokenPrice.TokenAddress,
						Price:        tokenPrice.Price,
						Decimal:      tokenPrice.Decimal,
					})
				}
				return tokenPrices
			}(),
		})
	}
	return &apipb.GetMarketOptionPriceHistoryReply{
		Total:     GetMarketOptionPriceHistoryRsp.Total,
		Snapshots: rspSnapshots,
	}, nil
}

func (s *HttpApiService) GetMarkets(ctx context.Context, req *apipb.GetMarketsRequest) (*apipb.GetMarketsReply, error) {
	c := util.NewBaseCtx(ctx, s.logger)

	uid := req.Uid
	if uid == "" {
		uid = util.GetUidFromCtx(ctx)
	}

	rsp := &apipb.GetMarketsReply{
		TotalCount: 0,
		Markets:    make([]*apipb.GetMarketsReply_Market, 0),
	}

	// 1. 分页查询市场
	GetMarketsRsp, err := s.data.RpcClient.MarketcenterClient.GetMarketsAndOptionsInfo(ctx, &marketcenterpb.GetMarketsAndOptionsInfoRequest{
		Uid:               uid,
		Tag:               req.Tag,
		SortType:          marketcenterpb.GetMarketsAndOptionsInfoRequest_SortType(req.SortType),
		HotWords:          req.HotWords,
		BaseTokenAddress:  util.GetBaseTokenAddressFromCtx(ctx),
		Page:              req.Page,
		PageSize:          req.PageSize,
		Status:            1,
		IsShow:            marketcenterpb.MarketIsShow_MARKET_IS_SHOW_SHOW,
		CategoryId:        req.CategoryId,
		SortAsc:           req.SortAsc,
		Followed:          req.Followed,
		MinVolumeTrending: req.MinVolumeTrending,
		SoonExpired:       req.SoonExpired,
		NewMarket:         req.NewMarket,
	})

	if err != nil {
		c.Log.Errorf("rpc GetMarketsAndOptionsInfo failed, err: [%+v]", err)
		return nil, err
	}

	rsp.TotalCount = GetMarketsRsp.Total

	optionAddressToOptionInfoMap := make(map[string]*marketcenterpb.GetMarketsAndOptionsInfoResponse_Market_Option)
	marketAddresses := make([]string, 0)
	for _, market := range GetMarketsRsp.Markets {
		marketAddresses = append(marketAddresses, market.Address)
		for _, option := range market.Options {
			optionAddressToOptionInfoMap[option.Address] = option
		}
	}

	// 2. 查询每个市场的一个帖子
	marketPostAndPublisherRsp := &usercenterpb.BatchGetMarketPostAndPublisherResponse{Posts: make([]*usercenterpb.BatchGetMarketPostAndPublisherResponse_Post, 0)}
	markAddressToPostMap := make(map[string]*usercenterpb.BatchGetMarketPostAndPublisherResponse_Post)

	// key: uid:marketAddress
	uidAndMarketAddressToUserPositionMap := make(map[string]*marketcenterpb.BatchGetMarketUsersPositionsResponse_UserMarketPosition)

	if len(marketAddresses) > 0 {

		marketPostAndPublisherRsp, err = s.data.RpcClient.UsercenterClient.BatchGetMarketPostAndPublisher(ctx, &usercenterpb.BatchGetMarketPostAndPublisherRequest{
			Uid:             req.Uid,
			MarketAddresses: marketAddresses,
		})
		if err != nil {
			c.Log.Errorf("rpc BatchGetMarketPostAndPublisher failed, err: [%+v]", err)
			return nil, err
		}

		marketAndUser := make([]*marketcenterpb.BatchGetMarketUsersPositionsRequest_MarketAndUser, 0)
		for _, post := range marketPostAndPublisherRsp.Posts {
			markAddressToPostMap[post.MarketAddress] = post

			marketAndUser = append(marketAndUser, &marketcenterpb.BatchGetMarketUsersPositionsRequest_MarketAndUser{
				MarketAddress: post.MarketAddress,
				Uid:           post.Uid,
			})
		}

		// 3. 查询帖子发布者们的持仓信息
		if len(marketAndUser) > 0 {
			marketUsersPositionsRsp, err := s.data.RpcClient.MarketcenterClient.BatchGetMarketUsersPositions(ctx, &marketcenterpb.BatchGetMarketUsersPositionsRequest{
				MarketAndUsers: marketAndUser,
			})
			if err != nil {
				c.Log.Errorf("rpc BatchGetMarketUsersPositions failed, err: [%+v]", err)
				return nil, err
			}

			for _, userPosition := range marketUsersPositionsRsp.UserMarketPositions {
				key := fmt.Sprintf("%s:%s", userPosition.Uid, userPosition.MarketAddress)
				uidAndMarketAddressToUserPositionMap[key] = userPosition
			}

		}

	}

	for _, market := range GetMarketsRsp.Markets {
		marketInfo := &apipb.GetMarketsReply_Market{
			MarketAddress:     market.Address,
			MarketName:        market.Name,
			MarketPicUrl:      market.PicUrl,
			MarketDescription: market.Description,
			MarketStatus:      market.Status,
			MarketVolume:      market.Volume,
			ParticipantsCount: market.ParticipantsCount,
			IsFollowed:        apipb.IsFollowed(market.IsFollowed),
			MarketDecimal:     uint32(market.Decimal),
			Result:            market.Result,
			CreatedAt:         uint32(market.CreatedAt),
			Deadline:          uint32(market.Deadline),
			Options:           make([]*apipb.GetMarketsReply_Market_Option, 0),
			BaseTokenAddress: market.BaseTokenAddress,
			EventId:           market.EventId,
		}

		for _, option := range market.Options {
			marketInfo.Options = append(marketInfo.Options, &apipb.GetMarketsReply_Market_Option{
				TokenAddress:     option.Address,
				TokenName:        option.Name,
				TokenPicUrl:      option.PicUrl,
				TokenPrice:       option.Price,
				Decimal:          option.Decimal,
				TokenDescription: option.Description,
				PositionId:       option.PositionId,
			})
		}

		if post, ok := markAddressToPostMap[market.Address]; ok {
			marketInfo.Post = &apipb.GetMarketsReply_Market_Post{
				Uuid:          post.PostUuid,
				Uid:           post.Uid,
				UserName:      post.UserName,
				UserAvatarUrl: post.UserAvatarUrl,
				Title:         post.Title,
				Content:       post.Content,
				LikeCount:     uint32(post.LikeCount),
				CommentCount:  uint32(post.CommentCount),
				Timestamp:     uint64(post.CreatedAt),
				Positions:     make([]*apipb.GetMarketsReply_Market_Post_Position, 0),
				IsLike:        apipb.GetMarketsReply_IsLike(post.IsLike),
				Id:            int64(post.Id),
			}

			key := fmt.Sprintf("%s:%s", post.Uid, market.Address)
			if userPosition, ok := uidAndMarketAddressToUserPositionMap[key]; ok {
				for _, position := range userPosition.Positions {
					oneRspPosition := &apipb.GetMarketsReply_Market_Post_Position{
						TokenAddress: position.OptionAddress,
						Balance:      position.Amount,
						Decimal:      position.Decimal,
					}
					if option, ok := optionAddressToOptionInfoMap[position.OptionAddress]; ok {
						oneRspPosition.TokenName = option.Name
						oneRspPosition.TokenPicUrl = option.PicUrl
						oneRspPosition.TokenDescription = option.Description
					}
					marketInfo.Post.Positions = append(marketInfo.Post.Positions, oneRspPosition)
				}
			}

		}

		rsp.Markets = append(rsp.Markets, marketInfo)
	}

	return rsp, nil
}

func (s *HttpApiService) GetTags(ctx context.Context, req *apipb.GetTagsRequest) (*apipb.GetTagsReply, error) {
	c := util.NewBaseCtx(ctx, s.logger)

	GetMarketTagsRsp, err := s.data.RpcClient.MarketcenterClient.GetMarketTags(ctx, &marketcenterpb.GetMarketTagsRequest{
		Page:     req.Page,
		PageSize: req.PageSize,
	})
	if err != nil {
		c.Log.Errorf("rpc GetMarketTags failed, err: [%+v]", err)
		return nil, err
	}

	return &apipb.GetTagsReply{
		Total: GetMarketTagsRsp.Total,
		Tags:  GetMarketTagsRsp.Tags,
	}, nil
}

func (s *HttpApiService) GetUserAssetInfo(ctx context.Context, req *apipb.GetUserAssetInfoRequest) (*apipb.GetUserAssetInfoReply, error) {

	c := util.NewBaseCtx(ctx, s.logger)

	GetUserAssetInfoRsp, err := s.data.RpcClient.MarketcenterClient.GetUserLatestAssetValue(ctx, &marketcenterpb.GetUserLatestAssetValueRequest{
		Uid:           req.Uid,
		BaseTokenAddress: util.GetBaseTokenAddressFromCtx(ctx),
	})
	if err != nil {
		c.Log.Errorf("rpc GetUserLatestAssetValue failed, err: [%+v]", err)
		return nil, err
	}

	return &apipb.GetUserAssetInfoReply{
		Balance:   GetUserAssetInfoRsp.Balance,
		Portfolio: GetUserAssetInfoRsp.Portfolio,
		Pnl:       GetUserAssetInfoRsp.Pnl,
		Value:     GetUserAssetInfoRsp.Value,
		Decimal:   uint32(GetUserAssetInfoRsp.Decimal),
		Volume:    GetUserAssetInfoRsp.Volume,
		PnlRank:   GetUserAssetInfoRsp.PnlRank,
	}, nil
}

func (s *HttpApiService) GetCategories(ctx context.Context, req *apipb.GetCategoriesRequest) (*apipb.GetCategoriesReply, error) {
	c := util.NewBaseCtx(ctx, s.logger)

	GetCategoriesRsp, err := s.data.RpcClient.MarketcenterClient.GetMarketCategories(ctx, &marketcenterpb.GetMarketCategoriesRequest{
		BaseTokenAddress: util.GetBaseTokenAddressFromCtx(ctx),
	})
	if err != nil {
		c.Log.Errorf("rpc GetMarketCategories failed, err: [%+v]", err)
		return nil, err
	}

	return &apipb.GetCategoriesReply{
		Total: GetCategoriesRsp.Total,
		Categories: func() []*apipb.GetCategoriesReply_Category {
			categories := make([]*apipb.GetCategoriesReply_Category, 0, len(GetCategoriesRsp.Categories))
			for _, category := range GetCategoriesRsp.Categories {
				categories = append(categories, &apipb.GetCategoriesReply_Category{
					Id:     category.Id,
					Name:   category.Name,
					Weight: category.Weight,
				})
			}
			return categories
		}(),
	}, nil
}

func (s *HttpApiService) GetUserTransactions(ctx context.Context, req *apipb.GetUserTransactionsRequest) (*apipb.GetUserTransactionsReply, error) {
	c := util.NewBaseCtx(ctx, s.logger)

	uid := util.GetUidFromCtx(ctx)
	if uid == "" {
		return nil, pkg.ErrParam
	}

	GetUserTransactionsRsp, err := s.data.RpcClient.MarketcenterClient.GetUserTransactions(ctx, &marketcenterpb.GetUserTransactionsRequest{
		Uid:           uid,
		Page:          req.Page,
		PageSize:      req.PageSize,
		BaseTokenAddress: util.GetBaseTokenAddressFromCtx(ctx),
	})
	if err != nil {
		c.Log.Errorf("rpc GetUserTransactions failed, err: [%+v]", err)
		return nil, err
	}

	transactions := make([]*apipb.GetUserTransactionsReply_Transaction, 0, len(GetUserTransactionsRsp.Transactions))
	for _, transaction := range GetUserTransactionsRsp.Transactions {
		tx := &apipb.GetUserTransactionsReply_Transaction{
			Uid:           transaction.Uid,
			Amount:        transaction.Amount,
			Timestamp:     transaction.Timestamp,
			BaseTokenAddress: transaction.BaseTokenAddress,
			TokenAddress:  transaction.TokenAddress,
			Side:          uint32(transaction.Side),
			Decimal:       uint32(transaction.Decimal),
			Type:          apipb.TxType(transaction.Type),
			Status:        uint32(transaction.Status),
			BizDataJson:   string(transaction.BizData),
			TxHash:        transaction.TxHash,
		}
		transactions = append(transactions, tx)
	}

	return &apipb.GetUserTransactionsReply{
		Total:        uint32(GetUserTransactionsRsp.Total),
		Transactions: transactions,
	}, nil
}

func (s *HttpApiService) GetBanners(ctx context.Context, req *apipb.GetBannersRequest) (*apipb.GetBannersReply, error) {
	c := util.NewBaseCtx(ctx, s.logger)

	GetBannersRsp, err := s.data.RpcClient.MarketcenterClient.GetBanners(ctx, &marketcenterpb.GetBannersRequest{
		BaseTokenAddress: util.GetBaseTokenAddressFromCtx(ctx),
	})
	if err != nil {
		c.Log.Errorf("rpc GetBanners failed, err: [%+v]", err)
		return nil, err
	}

	rsp := &apipb.GetBannersReply{}
	for _, banner := range GetBannersRsp.Banners {
		rsp.Banners = append(rsp.Banners, &apipb.GetBannersReply_Banner{
			Id:     banner.Id,
			Weight: banner.Weight,
			Image:  banner.Image,
			Url:    banner.Url,
			Type:   int32(banner.Type),
		})
	}

	return rsp, nil
}

func (s *HttpApiService) GetSections(ctx context.Context, req *apipb.GetSectionsRequest) (*apipb.GetSectionsReply, error) {
	c := util.NewBaseCtx(ctx, s.logger)

	GetSectionsRsp, err := s.data.RpcClient.MarketcenterClient.GetSections(ctx, &marketcenterpb.GetSectionsRequest{
		BaseTokenAddress: util.GetBaseTokenAddressFromCtx(ctx),
	})
	if err != nil {
		c.Log.Errorf("rpc GetSections failed, err: [%+v]", err)
		return nil, err
	}

	rsp := &apipb.GetSectionsReply{}
	for _, section := range GetSectionsRsp.Sections {
		oneSection := &apipb.GetSectionsReply_Section{
			Weight:      section.Weight,
			Color:       section.Color,
			Title:       section.Title,
			Id:          section.Id,
			Type:        int32(section.Type),
			Predictions: make([]*apipb.GetSectionsReply_Section_Prediction, 0),
		}

		for _, prediction := range section.Predictions {
			oneSection.Predictions = append(oneSection.Predictions, &apipb.GetSectionsReply_Section_Prediction{
				Id:         prediction.Id,
				Weight:     prediction.Weight,
				Prediction: prediction.Prediction,
			})
		}
		rsp.Sections = append(rsp.Sections, oneSection)
	}

	return rsp, nil
}

func (s *HttpApiService) GetLeaderboard(ctx context.Context, req *apipb.GetLeaderboardRequest) (*apipb.GetLeaderboardResponse, error) {
	c := util.NewBaseCtx(ctx, s.logger)
	rsp := &apipb.GetLeaderboardResponse{
		Total:   0,
		Entries: make([]*apipb.GetLeaderboardResponse_Entry, 0),
	}

	uid := req.Uid
	GetLeaderboardRsp, err := s.data.RpcClient.MarketcenterClient.GetLeaderboard(ctx, &marketcenterpb.GetLeaderboardRequest{
		BaseTokenAddress: util.GetBaseTokenAddressFromCtx(ctx),
		Uid:           req.Uid,
		Page:          req.Page,
		PageSize:      req.PageSize,
		TimeInterval:  marketcenterpb.GetLeaderboardRequest_TimeInterval(req.TimeInterval),
		SortType:      marketcenterpb.GetLeaderboardRequest_SortType(req.SortType),
	})
	if err != nil {
		c.Log.Errorf("rpc GetLeaderboard failed, err: [%+v]", err)
		return nil, err
	}

	if len(GetLeaderboardRsp.Entries) == 0 {
		return rsp, nil
	}

	uids := make([]string, 0, len(GetLeaderboardRsp.Entries))
	for _, entry := range GetLeaderboardRsp.Entries {
		uids = append(uids, entry.Uid)
	}
	if uid != "" {
		uids = append(uids, uid)
	}

	userInfoList, err := s.data.RpcClient.UsercenterClient.GetUserInfosByUids(ctx, &usercenterpb.GetUserInfosByUidsRequest{
		Uids: uids,
	})
	if err != nil {
		c.Log.Errorf("rpc GetUsersInfoByAddresses failed, err: [%+v]", err)
		return nil, err
	}

	userInfoMap := make(map[string]*usercenterpb.GetUserInfosByUidsReply_UserInfo)
	for _, userInfo := range userInfoList.UserInfos {
		userInfoMap[userInfo.Uid] = userInfo
	}

	for _, entry := range GetLeaderboardRsp.Entries {
		oneEntry := &apipb.GetLeaderboardResponse_Entry{
			Uid:     entry.Uid,
			Score:   entry.Score,
			Decimal: entry.Decimal,
			Rank:    entry.Rank,
		}
		if userInfo, ok := userInfoMap[entry.Uid]; ok {
			oneEntry.Name = userInfo.Name
			oneEntry.Avatar = userInfo.Avatar
		}
		rsp.Entries = append(rsp.Entries, oneEntry)
	}

	if GetLeaderboardRsp.UserOwnEntry != nil {
		rsp.UserOwnEntry = &apipb.GetLeaderboardResponse_Entry{
			Uid:     GetLeaderboardRsp.UserOwnEntry.Uid,
			Score:   GetLeaderboardRsp.UserOwnEntry.Score,
			Decimal: GetLeaderboardRsp.UserOwnEntry.Decimal,
			Rank:    GetLeaderboardRsp.UserOwnEntry.Rank,
		}
		if userOwnInfo, ok := userInfoMap[uid]; ok {
			rsp.UserOwnEntry.Name = userOwnInfo.Name
			rsp.UserOwnEntry.Avatar = userOwnInfo.Avatar
		}
	}

	rsp.Total = uint32(GetLeaderboardRsp.Total)

	return rsp, nil
}

func (s *HttpApiService) GetEventDetail(ctx context.Context, req *apipb.GetEventDetailRequest) (*apipb.GetEventDetailReply, error) {
	c := util.NewBaseCtx(ctx, s.logger)

	getEventRsp, err := s.data.RpcClient.MarketcenterClient.GetEvent(ctx, &marketcenterpb.GetEventRequest{
		Id: req.Id,
	})
	if err != nil {
		c.Log.Errorf("rpc GetEvent failed, err: [%+v]", err)
		return nil, err
	}

	event := getEventRsp.Event
	if event == nil {
		return nil, errors.NotFound("NOT_FOUND", "event not found")
	}

	// Id 优先使用 upstream 返回的 db id，fallback 到链上 eventId
	eventId := event.Id
	if eventId == "" {
		eventId = event.EventId
	}

	eventStatus := apipb.EventStatus(event.Status)
	if eventStatus < apipb.EventStatus_EVENT_STATUS_UNSPECIFIED || eventStatus > apipb.EventStatus_EVENT_STATUS_FROZEN {
		eventStatus = apipb.EventStatus_EVENT_STATUS_UNSPECIFIED
	}

	rsp := &apipb.GetEventDetailReply{
		Id:               eventId,
		EventId:          event.EventId,
		Title:            event.Title,
		OutcomeSlotCount: event.OutcomeSlotCount,
		Collateral:       event.Collateral,
		Status:           eventStatus,
		MetadataHash:     event.MetadataHash,
		CreatedAt:        event.CreatedAt,
		UpdatedAt:        event.UpdatedAt,
		Markets:          make([]*apipb.GetEventDetailReply_Market, 0, len(event.Markets)),
	}

	for _, market := range event.Markets {
		m := &apipb.GetEventDetailReply_Market{
			Address:           market.Address,
			Name:              market.Name,
			PicUrl:            market.PicUrl,
			Description:       market.Description,
			Status:            market.Status,
			ParticipantsCount: market.ParticipantsCount,
			Volume:            market.Volume,
			Decimal:           market.Decimal,
			CreatedAt:         market.CreatedAt,
			Deadline:          market.Deadline,
			Result:            market.Result,
			ConditionId:       market.ConditionId,
			QuestionId:        market.QuestionId,
			Options:           make([]*apipb.GetEventDetailReply_Market_Option, 0, len(market.Options)),
		}
		for _, option := range market.Options {
			m.Options = append(m.Options, &apipb.GetEventDetailReply_Market_Option{
				Address:    option.Address,
				Name:       option.Name,
				Symbol:     option.Symbol,
				PicUrl:     option.PicUrl,
				Decimal:    option.Decimal,
				Index:      option.Index,
				Price:      option.Price,
				Description: option.Description,
				PositionId: option.PositionId,
			})
		}
		rsp.Markets = append(rsp.Markets, m)
	}

	return rsp, nil
}

func (s *HttpApiService) GetEvents(ctx context.Context, req *apipb.GetEventsRequest) (*apipb.GetEventsReply, error) {
	c := util.NewBaseCtx(ctx, s.logger)

	listEventsRsp, err := s.data.RpcClient.MarketcenterClient.ListEvents(ctx, &marketcenterpb.ListEventsRequest{
		Status:   marketcenterpb.EventStatus(req.Status),
		SortType: marketcenterpb.ListEventsRequest_SortType(req.SortType),
		Page:     req.Page,
		PageSize: req.PageSize,
	})
	if err != nil {
		c.Log.Errorf("rpc ListEvents failed, err: [%+v]", err)
		return nil, err
	}

	rsp := &apipb.GetEventsReply{
		Total:  listEventsRsp.Total,
		Events: make([]*apipb.GetEventsReply_EventSummary, 0, len(listEventsRsp.Events)),
	}

	for _, event := range listEventsRsp.Events {
		evtStatus := apipb.EventStatus(event.Status)
		if evtStatus < apipb.EventStatus_EVENT_STATUS_UNSPECIFIED || evtStatus > apipb.EventStatus_EVENT_STATUS_FROZEN {
			evtStatus = apipb.EventStatus_EVENT_STATUS_UNSPECIFIED
		}
		summary := &apipb.GetEventsReply_EventSummary{
			Id:               event.Id,
			EventId:          event.EventId,
			Title:            event.Title,
			OutcomeSlotCount: event.OutcomeSlotCount,
			Status:           evtStatus,
			CreatedAt:        event.CreatedAt,
			Markets:          make([]*apipb.GetEventsReply_EventSummary_Market, 0, len(event.Markets)),
		}
		for _, market := range event.Markets {
			summary.Markets = append(summary.Markets, &apipb.GetEventsReply_EventSummary_Market{
				Address:           market.Address,
				Name:              market.Name,
				PicUrl:            market.PicUrl,
				Status:            market.Status,
				Volume:            market.Volume,
				ParticipantsCount: market.ParticipantsCount,
			})
		}
		rsp.Events = append(rsp.Events, summary)
	}

	return rsp, nil
}

// ============================================
// CTF APMM 交易接口
// ============================================

func (s *HttpApiService) Swap(ctx context.Context, req *apipb.SwapRequest) (*apipb.SwapReply, error) {
	c := util.NewBaseCtx(ctx, s.logger)
	uid := util.GetUidFromCtx(ctx)
	if uid == "" {
		return nil, pkg.ErrParam
	}

	swapRsp, err := s.data.RpcClient.MarketcenterClient.CTFSwap(ctx, &marketcenterpb.CTFSwapRequest{
		Uid:              uid,
		MarketAddress:    req.MarketAddress,
		OptionIndex:      req.OptionIndex,
		Amount:           req.Amount,
		MinReceiveAmount: req.MinReceiveAmount,
		Side:             req.Side,
		UserOperation: &marketcenterpb.UserOperation{
			Sender:                        req.UserOperation.Sender,
			Nonce:                         req.UserOperation.Nonce,
			InitCode:                      req.UserOperation.InitCode,
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
	})
	if err != nil {
		c.Log.Errorf("rpc CTFSwap failed, err: [%+v]", err)
		return nil, err
	}

	return &apipb.SwapReply{
		OpHash:    swapRsp.OpHash,
		OrderUuid: swapRsp.OrderUuid,
	}, nil
}

func (s *HttpApiService) DepositLiquidity(ctx context.Context, req *apipb.DepositLiquidityRequest) (*apipb.DepositLiquidityReply, error) {
	c := util.NewBaseCtx(ctx, s.logger)
	uid := util.GetUidFromCtx(ctx)
	if uid == "" {
		return nil, pkg.ErrParam
	}

	depositRsp, err := s.data.RpcClient.MarketcenterClient.CTFDepositLiquidity(ctx, &marketcenterpb.CTFDepositLiquidityRequest{
		Uid:           uid,
		MarketAddress: req.MarketAddress,
		Amount:        req.Amount,
		UserOperation: &marketcenterpb.UserOperation{
			Sender:                        req.UserOperation.Sender,
			Nonce:                         req.UserOperation.Nonce,
			InitCode:                      req.UserOperation.InitCode,
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
	})
	if err != nil {
		c.Log.Errorf("rpc CTFDepositLiquidity failed, err: [%+v]", err)
		return nil, err
	}

	return &apipb.DepositLiquidityReply{
		OpHash: depositRsp.OpHash,
	}, nil
}

func (s *HttpApiService) WithdrawLiquidity(ctx context.Context, req *apipb.WithdrawLiquidityRequest) (*apipb.WithdrawLiquidityReply, error) {
	c := util.NewBaseCtx(ctx, s.logger)
	uid := util.GetUidFromCtx(ctx)
	if uid == "" {
		return nil, pkg.ErrParam
	}

	withdrawRsp, err := s.data.RpcClient.MarketcenterClient.CTFWithdrawLiquidity(ctx, &marketcenterpb.CTFWithdrawLiquidityRequest{
		Uid:           uid,
		MarketAddress: req.MarketAddress,
		LpAmount:      req.LpAmount,
		UserOperation: &marketcenterpb.UserOperation{
			Sender:                        req.UserOperation.Sender,
			Nonce:                         req.UserOperation.Nonce,
			InitCode:                      req.UserOperation.InitCode,
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
	})
	if err != nil {
		c.Log.Errorf("rpc CTFWithdrawLiquidity failed, err: [%+v]", err)
		return nil, err
	}

	return &apipb.WithdrawLiquidityReply{
		OpHash: withdrawRsp.OpHash,
	}, nil
}

func (s *HttpApiService) RedeemPosition(ctx context.Context, req *apipb.RedeemPositionRequest) (*apipb.RedeemPositionReply, error) {
	c := util.NewBaseCtx(ctx, s.logger)
	uid := util.GetUidFromCtx(ctx)
	if uid == "" {
		return nil, pkg.ErrParam
	}

	redeemRsp, err := s.data.RpcClient.MarketcenterClient.CTFRedeemPosition(ctx, &marketcenterpb.CTFRedeemPositionRequest{
		Uid:           uid,
		MarketAddress: req.MarketAddress,
		ConditionId:   req.ConditionId,
		IndexSets:     req.IndexSets,
		UserOperation: &marketcenterpb.UserOperation{
			Sender:                        req.UserOperation.Sender,
			Nonce:                         req.UserOperation.Nonce,
			InitCode:                      req.UserOperation.InitCode,
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
	})
	if err != nil {
		c.Log.Errorf("rpc CTFRedeemPosition failed, err: [%+v]", err)
		return nil, err
	}

	return &apipb.RedeemPositionReply{
		OpHash: redeemRsp.OpHash,
	}, nil
}

func (s *HttpApiService) GetSwapPrice(ctx context.Context, req *apipb.GetSwapPriceRequest) (*apipb.GetSwapPriceReply, error) {
	c := util.NewBaseCtx(ctx, s.logger)

	priceRsp, err := s.data.RpcClient.MarketcenterClient.CTFGetSwapPrice(ctx, &marketcenterpb.CTFGetSwapPriceRequest{
		MarketAddress: req.MarketAddress,
		OptionIndex:   req.OptionIndex,
		Amount:        req.Amount,
		Side:          req.Side,
	})
	if err != nil {
		c.Log.Errorf("rpc CTFGetSwapPrice failed, err: [%+v]", err)
		return nil, err
	}

	return &apipb.GetSwapPriceReply{
		ExpectedAmount: priceRsp.ExpectedAmount,
		PriceImpact:    priceRsp.PriceImpact,
		Fee:            priceRsp.Fee,
	}, nil
}


