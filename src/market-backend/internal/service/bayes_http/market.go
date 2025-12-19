package bayes_http

import (
	"context"
	"fmt"
	"market-backend/internal/pkg"
	"market-backend/internal/pkg/util"
	marketcenterpb "market-proto/proto/market-service/marketcenter/v1"
	usercenterpb "market-proto/proto/market-service/usercenter/v1"

	bayespb "market-proto/proto/market-backend/v1"

	"github.com/go-kratos/kratos/v2/errors"
)

func (s *BayesHttpService) GetHotMarkets(ctx context.Context, req *bayespb.GetHotMarketsRequest) (*bayespb.GetHotMarketsReply, error) {
	c := util.NewBaseCtx(ctx, s.logger)
	hotMarketsRsp, err := s.data.RpcClient.MarketcenterClient.GetHotMarkets(ctx, &marketcenterpb.GetHotMarketsRequest{
		BaseTokenType: marketcenterpb.BaseTokenType(util.GetBaseTokenFromCtx(ctx)),
	})
	if err != nil {
		c.Log.Errorf("rpc GetHotMarkets failed, err: [%+v]", err)
		return nil, err
	}
	marketList := make([]*bayespb.GetHotMarketsReply_MarketInfo, 0)
	for _, market := range hotMarketsRsp.Markets {
		marketInfo := &bayespb.GetHotMarketsReply_MarketInfo{
			MarketName:        market.Name,
			MarketAddress:     market.Address,
			MarketPicUrl:      market.PicUrl,
			MarketDescription: market.Description,
			MarketStatus:      market.Status,
			BaseTokenType:     bayespb.BaseTokenType(market.BaseTokenType),
		}
		marketList = append(marketList, marketInfo)
	}
	return &bayespb.GetHotMarketsReply{
		MarketList: marketList,
	}, nil
}

func (s *BayesHttpService) GetFollowMarkets(ctx context.Context, req *bayespb.GetFollowMarketsRequest) (*bayespb.GetFollowMarketsReply, error) {

	c := util.NewBaseCtx(ctx, s.logger)

	uid := util.GetUidFromCtx(ctx)
	if uid == "" {
		return nil, pkg.ErrParam
	}
	followedMarketsRsp, err := s.data.RpcClient.MarketcenterClient.GetFollowedMarkets(ctx, &marketcenterpb.GetFollowedMarketsRequest{
		Uid:           uid,
		Page:          req.Page,
		PageSize:      req.PageSize,
		BaseTokenType: marketcenterpb.BaseTokenType(util.GetBaseTokenFromCtx(ctx)),
	})
	if err != nil {
		c.Log.Errorf("rpc GetFollowedMarkets failed, err: [%+v]", err)
		return nil, err
	}
	marketList := make([]*bayespb.GetFollowMarketsReply_MarketInfo, 0)
	for _, market := range followedMarketsRsp.Markets {
		marketInfo := &bayespb.GetFollowMarketsReply_MarketInfo{
			MarketAddress:     market.Address,
			MarketName:        market.Name,
			MarketPicUrl:      market.PicUrl,
			MarketDescription: market.Description,
			MarketStatus:      market.Status,
			Deadline:          market.Deadline,
			BaseTokenType:     bayespb.BaseTokenType(market.BaseTokenType),
		}
		marketList = append(marketList, marketInfo)
	}
	return &bayespb.GetFollowMarketsReply{
		Total:      followedMarketsRsp.Total,
		MarketList: marketList,
	}, nil
}

func (s *BayesHttpService) GetHoldingPositionsMarket(ctx context.Context, req *bayespb.GetHoldingPositionsMarketRequest) (*bayespb.GetHoldingPositionsMarketReply, error) {
	c := util.NewBaseCtx(ctx, s.logger)
	holdingPositionsMarketRsp, err := s.data.RpcClient.MarketcenterClient.GetHoldingPositionsMarkets(ctx, &marketcenterpb.GetHoldingPositionsMarketsRequest{
		BaseTokenType: marketcenterpb.BaseTokenType(util.GetBaseTokenFromCtx(ctx)),
		Uid:           req.Uid,
		Page:          req.Page,
		PageSize:      req.PageSize,
	})
	if err != nil {
		c.Log.Errorf("rpc GetHoldingPositionsMarket failed, err: [%+v]", err)
		return nil, err
	}
	marketList := make([]*bayespb.GetHoldingPositionsMarketReply_Market, 0)
	for _, market := range holdingPositionsMarketRsp.Markets {
		marketInfo := &bayespb.GetHoldingPositionsMarketReply_Market{
			Name:                    market.Name,
			Address:                 market.Address,
			PicUrl:                  market.PicUrl,
			Description:             market.Description,
			Status:                  market.Status,
			MarketVolume:            market.MarketVolume,
			MarketParticipantsCount: market.MarketParticipantsCount,
			BaseTokenType:           bayespb.BaseTokenType(market.BaseTokenType),
			UserMarketTotalValue:    market.UserMarketTotalValue,
			Positions: func() []*bayespb.GetHoldingPositionsMarketReply_Market_Position {
				positions := make([]*bayespb.GetHoldingPositionsMarketReply_Market_Position, 0)
				for _, position := range market.Positions {
					positions = append(positions, &bayespb.GetHoldingPositionsMarketReply_Market_Position{
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
	return &bayespb.GetHoldingPositionsMarketReply{
		Total:      holdingPositionsMarketRsp.Total,
		TotalValue: holdingPositionsMarketRsp.TotalValue,
		Markets:    marketList,
	}, nil
}

func (s *BayesHttpService) FollowMarket(ctx context.Context, req *bayespb.FollowMarketRequest) (*bayespb.FollowMarketReply, error) {
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

	return &bayespb.FollowMarketReply{}, nil
}

func (s *BayesHttpService) UnfollowMarket(ctx context.Context, req *bayespb.UnfollowMarketRequest) (*bayespb.UnfollowMarketReply, error) {
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

	return &bayespb.UnfollowMarketReply{}, nil
}

func (s *BayesHttpService) GetPaymasterData(ctx context.Context, req *bayespb.GetPaymasterDataRequest) (*bayespb.GetPaymasterDataReply, error) {
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

	return &bayespb.GetPaymasterDataReply{
		Paymaster:                     paymasterDataRsp.Paymaster,
		PaymasterData:                 paymasterDataRsp.PaymasterData,
		PaymasterVerificationGasLimit: paymasterDataRsp.PaymasterVerificationGasLimit,
		PreVerificationGas:            paymasterDataRsp.PreVerificationGas,
		VerificationGasLimit:          paymasterDataRsp.VerificationGasLimit,
		CallGasLimit:                  paymasterDataRsp.CallGasLimit,
	}, nil
}

// TODO 限流 用户 + 接口维度
func (s *BayesHttpService) PlaceOrder(ctx context.Context, req *bayespb.PlaceOrderRequest) (*bayespb.PlaceOrderReply, error) {
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

	return &bayespb.PlaceOrderReply{
		OpHash:    placeOrderRsp.OpHash,
		OrderUuid: placeOrderRsp.OrderUuid,
	}, nil
}

func (s *BayesHttpService) ClaimMarketResult(ctx context.Context, req *bayespb.ClaimMarketResultRequest) (*bayespb.ClaimMarketResultReply, error) {
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

	return &bayespb.ClaimMarketResultReply{
		OpHash: claimMarketResultRsp.OpHash,
	}, nil
}

func (s *BayesHttpService) TransferBaseToken(ctx context.Context, req *bayespb.TransferBaseTokenRequest) (*bayespb.TransferBaseTokenReply, error) {
	c := util.NewBaseCtx(ctx, s.logger)
	uid := util.GetUidFromCtx(ctx)
	if uid == "" {
		return nil, pkg.ErrParam
	}

	TransferBaseTokenRsp, err := s.data.RpcClient.MarketcenterClient.TransferBaseToken(ctx, &marketcenterpb.TransferBaseTokenRequest{
		Uid:           uid,
		BaseTokenType: marketcenterpb.BaseTokenType(util.GetBaseTokenFromCtx(ctx)),
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

	return &bayespb.TransferBaseTokenReply{
		OpHash: TransferBaseTokenRsp.OpHash,
	}, nil
}

func (s *BayesHttpService) GetMarketTrades(ctx context.Context, req *bayespb.GetMarketTradesRequest) (*bayespb.GetMarketTradesReply, error) {
	c := util.NewBaseCtx(ctx, s.logger)
	rsp := &bayespb.GetMarketTradesReply{
		Total:  0,
		Orders: make([]*bayespb.GetMarketTradesReply_Order, 0),
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
		orderInfo := &bayespb.GetMarketTradesReply_Order{
			Uuid:          order.Uuid,
			Uid:           order.Uid,
			Side:          order.Side,
			Amount:        order.Amount,
			ReceiveAmount: order.ReceiveAmount,
			Timestamp:     order.Timestamp,
			DealPrice:     order.DealPrice,
		}
		if order.Option != nil {
			orderInfo.Option = &bayespb.GetMarketTradesReply_Order_Option{
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

func (s *BayesHttpService) GetUserTrades(ctx context.Context, req *bayespb.GetUserTradesRequest) (*bayespb.GetUserTradesReply, error) {
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
		BaseTokenType: marketcenterpb.BaseTokenType(util.GetBaseTokenFromCtx(ctx)),
	})
	if err != nil {
		c.Log.Errorf("rpc GetUserTrades failed, err: [%+v]", err)
		return nil, err
	}

	rspTrades := make([]*bayespb.GetUserTradesReply_Order, 0)
	for _, trade := range GetUserTradesRsp.Orders {
		one := &bayespb.GetUserTradesReply_Order{
			Uuid:          trade.Uuid,
			Uid:           trade.Uid,
			Side:          trade.Side,
			Amount:        trade.Amount,
			ReceiveAmount: trade.ReceiveAmount,
			Timestamp:     trade.Timestamp,
			DealPrice:     trade.DealPrice,
			BaseTokenType: bayespb.BaseTokenType(trade.BaseTokenType),
		}
		if trade.Option != nil {
			one.Option = &bayespb.GetUserTradesReply_Order_Option{
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

	return &bayespb.GetUserTradesReply{
		Total:  GetUserTradesRsp.Total,
		Orders: rspTrades,
	}, nil
}

func (s *BayesHttpService) GetMarketDetail(ctx context.Context, req *bayespb.GetMarketDetailRequest) (*bayespb.GetMarketDetailReply, error) {

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

	return &bayespb.GetMarketDetailReply{
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
		MarketStatus:        bayespb.GetMarketDetailReply_MaketStatus(GetMarketDetailRsp.Status),
		IsFollowed:          bayespb.IsFollowed(GetMarketDetailRsp.IsFollowed),
		IsClaim:             bayespb.IsClaim(GetMarketDetailRsp.IsClaim),
		BaseTokenType:       bayespb.BaseTokenType(GetMarketDetailRsp.BaseTokenType),
		BaseTokenAddress:    GetMarketDetailRsp.BaseTokenAddress,
		OptionList: func() []*bayespb.GetMarketDetailReply_OptionInfo {
			optionList := make([]*bayespb.GetMarketDetailReply_OptionInfo, 0)
			for _, option := range GetMarketDetailRsp.Options {
				optionList = append(optionList, &bayespb.GetMarketDetailReply_OptionInfo{
					OptionAddress: option.Address,
					OptionName:    option.Name,
					OptionSymbol:  option.Symbol,
					OptionPicUrl:  option.PicUrl,
					Price:         option.Price,
					Decimal:       option.Decimal,
					Description:   option.Description,
				})
			}
			return optionList
		}(),
	}, nil
}

func (s *BayesHttpService) GetUserPositions(ctx context.Context, req *bayespb.GetUserPositionsRequest) (*bayespb.GetUserPositionsReply, error) {
	c := util.NewBaseCtx(ctx, s.logger)

	uid := req.Uid
	if uid == "" {
		uid = util.GetUidFromCtx(ctx)
	}

	GetUserPositionsRsp, err := s.data.RpcClient.MarketcenterClient.GetUserPositions(ctx, &marketcenterpb.GetUserPositionsRequest{
		Uid:           uid,
		MarketAddress: req.MarketAddress,
		BaseTokenType: marketcenterpb.BaseTokenType(util.GetBaseTokenFromCtx(ctx)),
		Page:          req.Page,
		PageSize:      req.PageSize,
		OptionAddress: req.OptionAddress,
	})
	if err != nil {
		c.Log.Errorf("rpc GetUserPositions failed, err: [%+v]", err)
		return nil, err
	}
	rspPositions := make([]*bayespb.GetUserPositionsReply_Position, 0)
	for _, position := range GetUserPositionsRsp.Positions {
		rspPositions = append(rspPositions, &bayespb.GetUserPositionsReply_Position{
			MarketAddress:     position.MarketAddress,
			MarketName:        position.MarketName,
			MarketDescription: position.MarketDescription,
			MarketPicUrl:      position.MarketPicUrl,
			OptionAddress:     position.OptionAddress,
			BaseTokenType:     bayespb.BaseTokenType(position.BaseTokenType),
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
	return &bayespb.GetUserPositionsReply{
		Total:     GetUserPositionsRsp.Total,
		Positions: rspPositions,
	}, nil
}

func (s *BayesHttpService) GetUserAssetHistory(ctx context.Context, req *bayespb.GetUserAssetHistoryRequest) (*bayespb.GetUserAssetHistoryReply, error) {
	c := util.NewBaseCtx(ctx, s.logger)
	GetUserAssetHistoryRsp, err := s.data.RpcClient.MarketcenterClient.GetUserAssetHistory(ctx, &marketcenterpb.GetUserAssetHistoryRequest{
		Uid:           req.Uid,
		BaseTokenType: marketcenterpb.BaseTokenType(util.GetBaseTokenFromCtx(ctx)),
		TimeInterval:  marketcenterpb.GetUserAssetHistoryRequest_TimeInterval(req.TimeInterval),
	})
	if err != nil {
		c.Log.Errorf("rpc GetUserAssetHistory failed, err: [%+v]", err)
		return nil, err
	}

	rspSnapshots := make([]*bayespb.GetUserAssetHistoryReply_OneSnapshot, 0)
	for _, snapshot := range GetUserAssetHistoryRsp.Snapshots {
		rspSnapshots = append(rspSnapshots, &bayespb.GetUserAssetHistoryReply_OneSnapshot{
			Value:     snapshot.Value,
			Balance:   snapshot.Balance,
			Portfolio: snapshot.Portfolio,
			Pnl:       snapshot.Pnl,
			Timestamp: snapshot.Timestamp,
		})
	}
	return &bayespb.GetUserAssetHistoryReply{
		Total:         GetUserAssetHistoryRsp.Total,
		Snapshots:     rspSnapshots,
		BaseTokenType: bayespb.BaseTokenType(GetUserAssetHistoryRsp.BaseTokenType),
		Decimal:       uint32(GetUserAssetHistoryRsp.Decimal),
	}, nil
}
func (s *BayesHttpService) GetMarketOptionPriceHistory(ctx context.Context, req *bayespb.GetMarketOptionPriceHistoryRequest) (*bayespb.GetMarketOptionPriceHistoryReply, error) {
	c := util.NewBaseCtx(ctx, s.logger)
	GetMarketOptionPriceHistoryRsp, err := s.data.RpcClient.MarketcenterClient.GetMarketOptionPriceHistory(ctx, &marketcenterpb.GetMarketOptionPriceHistoryRequest{
		MarketAddress: req.MarketAddress,
		TimeInterval:  marketcenterpb.GetMarketOptionPriceHistoryRequest_TimeInterval(req.TimeInterval),
	})
	if err != nil {
		c.Log.Errorf("rpc GetMarketOptionPriceHistory failed, err: [%+v]", err)
		return nil, err
	}

	rspSnapshots := make([]*bayespb.GetMarketOptionPriceHistoryReply_OneSnapshot, 0)
	for _, snapshot := range GetMarketOptionPriceHistoryRsp.Snapshots {
		rspSnapshots = append(rspSnapshots, &bayespb.GetMarketOptionPriceHistoryReply_OneSnapshot{
			Timestamp: snapshot.Timestamp,
			TokenPrices: func() []*bayespb.GetMarketOptionPriceHistoryReply_OneSnapshot_TokenPrice {
				tokenPrices := make([]*bayespb.GetMarketOptionPriceHistoryReply_OneSnapshot_TokenPrice, 0)
				for _, tokenPrice := range snapshot.TokenPrices {
					tokenPrices = append(tokenPrices, &bayespb.GetMarketOptionPriceHistoryReply_OneSnapshot_TokenPrice{
						TokenAddress: tokenPrice.TokenAddress,
						Price:        tokenPrice.Price,
						Decimal:      tokenPrice.Decimal,
					})
				}
				return tokenPrices
			}(),
		})
	}
	return &bayespb.GetMarketOptionPriceHistoryReply{
		Total:     GetMarketOptionPriceHistoryRsp.Total,
		Snapshots: rspSnapshots,
	}, nil
}

func (s *BayesHttpService) GetMarkets(ctx context.Context, req *bayespb.GetMarketsRequest) (*bayespb.GetMarketsReply, error) {
	c := util.NewBaseCtx(ctx, s.logger)

	uid := req.Uid
	if uid == "" {
		uid = util.GetUidFromCtx(ctx)
	}

	rsp := &bayespb.GetMarketsReply{
		TotalCount: 0,
		Markets:    make([]*bayespb.GetMarketsReply_Market, 0),
	}

	// 1. 分页查询市场
	GetMarketsRsp, err := s.data.RpcClient.MarketcenterClient.GetMarketsAndOptionsInfo(ctx, &marketcenterpb.GetMarketsAndOptionsInfoRequest{
		Uid:               uid,
		Tag:               req.Tag,
		SortType:          marketcenterpb.GetMarketsAndOptionsInfoRequest_SortType(req.SortType),
		HotWords:          req.HotWords,
		BaseTokenType:     marketcenterpb.BaseTokenType(util.GetBaseTokenFromCtx(ctx)),
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
		marketInfo := &bayespb.GetMarketsReply_Market{
			MarketAddress:     market.Address,
			MarketName:        market.Name,
			MarketPicUrl:      market.PicUrl,
			MarketDescription: market.Description,
			MarketStatus:      market.Status,
			MarketVolume:      market.Volume,
			ParticipantsCount: market.ParticipantsCount,
			IsFollowed:        bayespb.IsFollowed(market.IsFollowed),
			MarketDecimal:     uint32(market.Decimal),
			Result:            market.Result,
			CreatedAt:         uint32(market.CreatedAt),
			Deadline:          uint32(market.Deadline),
			Options:           make([]*bayespb.GetMarketsReply_Market_Option, 0),
			BaseTokenType:     uint32(market.BaseTokenType),
		}

		for _, option := range market.Options {
			marketInfo.Options = append(marketInfo.Options, &bayespb.GetMarketsReply_Market_Option{
				TokenAddress:     option.Address,
				TokenName:        option.Name,
				TokenPicUrl:      option.PicUrl,
				TokenPrice:       option.Price,
				Decimal:          option.Decimal,
				TokenDescription: option.Description,
			})
		}

		if post, ok := markAddressToPostMap[market.Address]; ok {
			marketInfo.Post = &bayespb.GetMarketsReply_Market_Post{
				Uuid:          post.PostUuid,
				Uid:           post.Uid,
				UserName:      post.UserName,
				UserAvatarUrl: post.UserAvatarUrl,
				Title:         post.Title,
				Content:       post.Content,
				LikeCount:     uint32(post.LikeCount),
				CommentCount:  uint32(post.CommentCount),
				Timestamp:     uint64(post.CreatedAt),
				Positions:     make([]*bayespb.GetMarketsReply_Market_Post_Position, 0),
				IsLike:        bayespb.GetMarketsReply_IsLike(post.IsLike),
				Id:            int64(post.Id),
			}

			key := fmt.Sprintf("%s:%s", post.Uid, market.Address)
			if userPosition, ok := uidAndMarketAddressToUserPositionMap[key]; ok {
				for _, position := range userPosition.Positions {
					oneRspPosition := &bayespb.GetMarketsReply_Market_Post_Position{
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

func (s *BayesHttpService) GetTags(ctx context.Context, req *bayespb.GetTagsRequest) (*bayespb.GetTagsReply, error) {
	c := util.NewBaseCtx(ctx, s.logger)

	GetMarketTagsRsp, err := s.data.RpcClient.MarketcenterClient.GetMarketTags(ctx, &marketcenterpb.GetMarketTagsRequest{
		Page:     req.Page,
		PageSize: req.PageSize,
	})
	if err != nil {
		c.Log.Errorf("rpc GetMarketTags failed, err: [%+v]", err)
		return nil, err
	}

	return &bayespb.GetTagsReply{
		Total: GetMarketTagsRsp.Total,
		Tags:  GetMarketTagsRsp.Tags,
	}, nil
}

func (s *BayesHttpService) GetUserAssetInfo(ctx context.Context, req *bayespb.GetUserAssetInfoRequest) (*bayespb.GetUserAssetInfoReply, error) {

	c := util.NewBaseCtx(ctx, s.logger)

	GetUserAssetInfoRsp, err := s.data.RpcClient.MarketcenterClient.GetUserLatestAssetValue(ctx, &marketcenterpb.GetUserLatestAssetValueRequest{
		Uid:           req.Uid,
		BaseTokenType: marketcenterpb.BaseTokenType(util.GetBaseTokenFromCtx(ctx)),
	})
	if err != nil {
		c.Log.Errorf("rpc GetUserLatestAssetValue failed, err: [%+v]", err)
		return nil, err
	}

	return &bayespb.GetUserAssetInfoReply{
		Balance:   GetUserAssetInfoRsp.Balance,
		Portfolio: GetUserAssetInfoRsp.Portfolio,
		Pnl:       GetUserAssetInfoRsp.Pnl,
		Value:     GetUserAssetInfoRsp.Value,
		Decimal:   uint32(GetUserAssetInfoRsp.Decimal),
		Volume:    GetUserAssetInfoRsp.Volume,
		PnlRank:   GetUserAssetInfoRsp.PnlRank,
	}, nil
}

func (s *BayesHttpService) GetCategories(ctx context.Context, req *bayespb.GetCategoriesRequest) (*bayespb.GetCategoriesReply, error) {
	c := util.NewBaseCtx(ctx, s.logger)

	GetCategoriesRsp, err := s.data.RpcClient.MarketcenterClient.GetMarketCategories(ctx, &marketcenterpb.GetMarketCategoriesRequest{
		BaseTokenType: marketcenterpb.BaseTokenType(util.GetBaseTokenFromCtx(ctx)),
	})
	if err != nil {
		c.Log.Errorf("rpc GetMarketCategories failed, err: [%+v]", err)
		return nil, err
	}

	return &bayespb.GetCategoriesReply{
		Total: GetCategoriesRsp.Total,
		Categories: func() []*bayespb.GetCategoriesReply_Category {
			categories := make([]*bayespb.GetCategoriesReply_Category, 0, len(GetCategoriesRsp.Categories))
			for _, category := range GetCategoriesRsp.Categories {
				categories = append(categories, &bayespb.GetCategoriesReply_Category{
					Id:     category.Id,
					Name:   category.Name,
					Weight: category.Weight,
				})
			}
			return categories
		}(),
	}, nil
}

func (s *BayesHttpService) GetUserTransactions(ctx context.Context, req *bayespb.GetUserTransactionsRequest) (*bayespb.GetUserTransactionsReply, error) {
	c := util.NewBaseCtx(ctx, s.logger)

	uid := util.GetUidFromCtx(ctx)
	if uid == "" {
		return nil, pkg.ErrParam
	}

	GetUserTransactionsRsp, err := s.data.RpcClient.MarketcenterClient.GetUserTransactions(ctx, &marketcenterpb.GetUserTransactionsRequest{
		Uid:           uid,
		Page:          req.Page,
		PageSize:      req.PageSize,
		BaseTokenType: marketcenterpb.BaseTokenType(util.GetBaseTokenFromCtx(ctx)),
	})
	if err != nil {
		c.Log.Errorf("rpc GetUserTransactions failed, err: [%+v]", err)
		return nil, err
	}

	transactions := make([]*bayespb.GetUserTransactionsReply_Transaction, 0, len(GetUserTransactionsRsp.Transactions))
	for _, transaction := range GetUserTransactionsRsp.Transactions {
		tx := &bayespb.GetUserTransactionsReply_Transaction{
			Uid:           transaction.Uid,
			Amount:        transaction.Amount,
			Timestamp:     transaction.Timestamp,
			BaseTokenType: bayespb.BaseTokenType(transaction.BaseTokenType),
			TokenAddress:  transaction.TokenAddress,
			Side:          uint32(transaction.Side),
			Decimal:       uint32(transaction.Decimal),
			Type:          bayespb.TxType(transaction.Type),
			Status:        uint32(transaction.Status),
			BizDataJson:   string(transaction.BizData),
			TxHash:        transaction.TxHash,
		}
		transactions = append(transactions, tx)
	}

	return &bayespb.GetUserTransactionsReply{
		Total:        uint32(GetUserTransactionsRsp.Total),
		Transactions: transactions,
	}, nil
}

func (s *BayesHttpService) GetBanners(ctx context.Context, req *bayespb.GetBannersRequest) (*bayespb.GetBannersReply, error) {
	c := util.NewBaseCtx(ctx, s.logger)

	GetBannersRsp, err := s.data.RpcClient.MarketcenterClient.GetBanners(ctx, &marketcenterpb.GetBannersRequest{
		BaseTokenType: marketcenterpb.BaseTokenType(util.GetBaseTokenFromCtx(ctx)),
	})
	if err != nil {
		c.Log.Errorf("rpc GetBanners failed, err: [%+v]", err)
		return nil, err
	}

	rsp := &bayespb.GetBannersReply{}
	for _, banner := range GetBannersRsp.Banners {
		rsp.Banners = append(rsp.Banners, &bayespb.GetBannersReply_Banner{
			Id:     banner.Id,
			Weight: banner.Weight,
			Image:  banner.Image,
			Url:    banner.Url,
			Type:   int32(banner.Type),
		})
	}

	return rsp, nil
}

func (s *BayesHttpService) GetSections(ctx context.Context, req *bayespb.GetSectionsRequest) (*bayespb.GetSectionsReply, error) {
	c := util.NewBaseCtx(ctx, s.logger)

	GetSectionsRsp, err := s.data.RpcClient.MarketcenterClient.GetSections(ctx, &marketcenterpb.GetSectionsRequest{
		BaseTokenType: marketcenterpb.BaseTokenType(util.GetBaseTokenFromCtx(ctx)),
	})
	if err != nil {
		c.Log.Errorf("rpc GetSections failed, err: [%+v]", err)
		return nil, err
	}

	rsp := &bayespb.GetSectionsReply{}
	for _, section := range GetSectionsRsp.Sections {
		oneSection := &bayespb.GetSectionsReply_Section{
			Weight:      section.Weight,
			Color:       section.Color,
			Title:       section.Title,
			Id:          section.Id,
			Type:        int32(section.Type),
			Predictions: make([]*bayespb.GetSectionsReply_Section_Prediction, 0),
		}

		for _, prediction := range section.Predictions {
			oneSection.Predictions = append(oneSection.Predictions, &bayespb.GetSectionsReply_Section_Prediction{
				Id:         prediction.Id,
				Weight:     prediction.Weight,
				Prediction: prediction.Prediction,
			})
		}
		rsp.Sections = append(rsp.Sections, oneSection)
	}

	return rsp, nil
}

func (s *BayesHttpService) GetLeaderboard(ctx context.Context, req *bayespb.GetLeaderboardRequest) (*bayespb.GetLeaderboardResponse, error) {
	c := util.NewBaseCtx(ctx, s.logger)
	rsp := &bayespb.GetLeaderboardResponse{
		Total:   0,
		Entries: make([]*bayespb.GetLeaderboardResponse_Entry, 0),
	}

	uid := req.Uid
	GetLeaderboardRsp, err := s.data.RpcClient.MarketcenterClient.GetLeaderboard(ctx, &marketcenterpb.GetLeaderboardRequest{
		BaseTokenType: marketcenterpb.BaseTokenType(util.GetBaseTokenFromCtx(ctx)),
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
		oneEntry := &bayespb.GetLeaderboardResponse_Entry{
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
		rsp.UserOwnEntry = &bayespb.GetLeaderboardResponse_Entry{
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
