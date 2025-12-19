package bayes_http

import (
	"context"
	"fmt"
	"market-backend/internal/pkg"
	"market-backend/internal/pkg/util"
	marketcenterpb "market-proto/proto/market-service/marketcenter/v1"
	usercenterpb "market-proto/proto/market-service/usercenter/v1"
	"strings"
	"time"

	bayespb "market-proto/proto/market-backend/v1"

	"github.com/go-kratos/kratos/v2/errors"
)

func (s *BayesHttpService) Login(ctx context.Context, req *bayespb.LoginRequest) (*bayespb.LoginReply, error) {
	c := util.NewBaseCtx(ctx, s.logger)
	did := util.GetDidFromCtx(ctx)
	if did == "" {
		return nil, pkg.ErrParam
	}

	lockKey := fmt.Sprintf("login-lock-%s", did)
	lockID, ok, err := s.data.AcquireLock(ctx, lockKey, 5*time.Second)
	if err != nil {
		return nil, pkg.ErrInternal.WithMetadata(map[string]string{"error": err.Error()})
	}
	if !ok {
		return nil, pkg.ErrWait.WithMetadata(map[string]string{"error": "lock is not acquired"})
	}
	defer s.data.ReleaseLock(ctx, lockKey, lockID)

	GetUserInfoRsp, err := s.data.RpcClient.UsercenterClient.GetUserInfo(ctx, &usercenterpb.GetUserInfoRequest{
		Issuer: did,
	})
	if err != nil {
		return nil, err
	}
	// 已注册 暂时不做什么 后续可以换token 记录用户每日登录任务等
	if GetUserInfoRsp.Uid != "" {
		return &bayespb.LoginReply{
			Uid:       GetUserInfoRsp.Uid,
			IsNewUser: false,
		}, nil
	}

	// 未注册 创建用户
	userInfoSimpleJson, err := s.data.GetUserInfoFromPrivy(c, did)
	if err != nil {
		return nil, err
	}
	linkedAccounts := userInfoSimpleJson.Get("linked_accounts")

	email := ""
	eoaAddress := ""
	address := ""
	for i := range linkedAccounts.MustArray() {
		linkedAccount := linkedAccounts.GetIndex(i)
		accountType := linkedAccount.Get("type").MustString()
		switch accountType {
		case "email":
			email = linkedAccount.Get("address").MustString()
		case "google_oauth":
			email = linkedAccount.Get("email").MustString()
		case "wallet":
			chainType := linkedAccount.Get("chain_type").MustString()
			if chainType == "ethereum" {
				eoaAddress = linkedAccount.Get("address").MustString()
			}
		// case "smart_wallet":
		// 	address = linkedAccount.Get("address").MustString()
		default:
		}
	}

	if email == "" || eoaAddress == "" {
		c.Log.Errorf("email or eoaAddress is empty")
		return nil, pkg.ErrParam
	}
	address = req.Address

	userInfoJson, err := userInfoSimpleJson.Encode()
	if err != nil {
		return nil, err
	}

	createUserResp, err := s.data.RpcClient.UsercenterClient.CreateUser(ctx, &usercenterpb.CreateUserRequest{
		Email:         email,
		Issuer:        did,
		EoaAddress:    eoaAddress,
		Address:       address,
		Name:          req.UserName,
		InvitedByCode: req.InviteByCode,
		Avatar:        req.AvatarUrl,
		PrivyUserInfo: userInfoJson,
		Source:        usercenterpb.CreateUserRequest_SOURCE_PRIVY,
	})
	if err != nil {
		c.Log.Errorf("rpc create user failed, err: [%+v]", err)
		return nil, err
	}

	return &bayespb.LoginReply{
		Uid:       createUserResp.Uid,
		IsNewUser: true,
	}, nil
}

func (s *BayesHttpService) SetAvatar(ctx context.Context, req *bayespb.SetAvatarRequest) (*bayespb.SetAvatarReply, error) {
	c := util.NewBaseCtx(ctx, s.logger)
	uid := util.GetUidFromCtx(ctx)
	if uid == "" {
		return nil, pkg.ErrParam
	}
	if req.AvatarUrl == "" {
		return nil, pkg.ErrParam
	}

	_, err := s.data.RpcClient.UsercenterClient.SetUserInfo(ctx, &usercenterpb.SetUserInfoRequest{
		Uid:    uid,
		Avatar: req.AvatarUrl,
	})
	if err != nil {
		c.Log.Errorf("rpc set user info failed, err: [%+v]", err)
		return nil, err
	}

	return &bayespb.SetAvatarReply{}, nil
}

func (s *BayesHttpService) SetName(ctx context.Context, req *bayespb.SetNameRequest) (*bayespb.SetNameReply, error) {
	c := util.NewBaseCtx(ctx, s.logger)
	uid := util.GetUidFromCtx(ctx)
	if uid == "" {
		return nil, pkg.ErrParam
	}
	if req.Name == "" && req.InviteByCode == "" {
		return nil, pkg.ErrParam
	}

	_, err := s.data.RpcClient.UsercenterClient.SetUserInfo(ctx, &usercenterpb.SetUserInfoRequest{
		Uid:          uid,
		Name:         req.Name,
		InviteByCode: req.InviteByCode,
	})
	if err != nil {
		c.Log.Errorf("rpc set user info failed, err: [%+v]", err)
		return nil, err
	}

	return &bayespb.SetNameReply{}, nil
}

func (s *BayesHttpService) SetDescription(ctx context.Context, req *bayespb.SetDescriptionRequest) (*bayespb.SetDescriptionReply, error) {
	c := util.NewBaseCtx(ctx, s.logger)
	uid := util.GetUidFromCtx(ctx)
	if uid == "" {
		return nil, pkg.ErrParam
	}
	if req.Description == "" {
		return nil, pkg.ErrParam
	}

	_, err := s.data.RpcClient.UsercenterClient.SetUserInfo(ctx, &usercenterpb.SetUserInfoRequest{
		Uid:  uid,
		Desc: req.Description,
	})
	if err != nil {
		c.Log.Errorf("rpc set user info failed, err: [%+v]", err)
		return nil, err
	}

	return &bayespb.SetDescriptionReply{}, nil
}

func (s *BayesHttpService) UploadFile(ctx context.Context, req *bayespb.UploadFileRequest) (*bayespb.UploadFileReply, error) {
	c := util.NewBaseCtx(ctx, s.logger)
	c.Log.Debugf("upload file start")
	fileData := req.File

	uploadFileToBizBucketS3Resp, err := s.data.RpcClient.UsercenterClient.UploadFileToBizBucketS3(ctx, &usercenterpb.UploadFileToBizBucketS3Request{
		FileData: fileData,
		// 包含除了市场和选项信息的所有业务 暂时都用avatar
		Biz: "avatar",
	})
	if err != nil {
		c.Log.Errorf("upload file to biz bucket s3 failed, err: [%+v]", err)
		return nil, err
	}

	return &bayespb.UploadFileReply{
		FileKey: uploadFileToBizBucketS3Resp.FileUrl,
	}, nil
}

func (s *BayesHttpService) DownloadFile(ctx context.Context, req *bayespb.DownloadFileRequest) (*bayespb.DownloadFileReply, error) {
	c := util.NewBaseCtx(ctx, s.logger)
	c.Log.Debugf("download file start")
	fileKey := req.FileKey

	downloadFileFromBizBucketS3Resp, err := s.data.RpcClient.UsercenterClient.DownloadFileFromBizBucketS3(ctx, &usercenterpb.DownloadFileFromBizBucketS3Request{
		FileUrl: fileKey,
	})
	if err != nil {
		c.Log.Errorf("download file from biz bucket s3 failed, err: [%+v]", err)
		return nil, err
	}

	return &bayespb.DownloadFileReply{
		FileData:    downloadFileFromBizBucketS3Resp.FileData,
		ContentType: downloadFileFromBizBucketS3Resp.ContentType,
	}, nil
}

func (s *BayesHttpService) FollowUser(ctx context.Context, req *bayespb.FollowUserRequest) (*bayespb.FollowUserReply, error) {
	c := util.NewBaseCtx(ctx, s.logger)
	uid := util.GetUidFromCtx(ctx)
	if uid == "" {
		return nil, pkg.ErrParam
	}

	_, err := s.data.RpcClient.UsercenterClient.UpdateFollowStatus(ctx, &usercenterpb.UpdateFollowStatusRequest{
		Uid:       uid,
		FollowUid: req.FollowUid,
		Status:    usercenterpb.UpdateFollowStatusRequest_STATUS_FOLLOW,
	})
	if err != nil {
		c.Log.Errorf("rpc update follow status failed, err: [%+v]", err)
		return nil, err
	}

	return &bayespb.FollowUserReply{}, nil
}

func (s *BayesHttpService) UnfollowUser(ctx context.Context, req *bayespb.UnfollowUserRequest) (*bayespb.UnfollowUserReply, error) {
	c := util.NewBaseCtx(ctx, s.logger)
	uid := util.GetUidFromCtx(ctx)
	if uid == "" {
		return nil, pkg.ErrParam
	}

	_, err := s.data.RpcClient.UsercenterClient.UpdateFollowStatus(ctx, &usercenterpb.UpdateFollowStatusRequest{
		Uid:       uid,
		FollowUid: req.UnfollowUid,
		Status:    usercenterpb.UpdateFollowStatusRequest_STATUS_UNFOLLOW,
	})
	if err != nil {
		c.Log.Errorf("rpc update follow status failed, err: [%+v]", err)
		return nil, err
	}

	return &bayespb.UnfollowUserReply{}, nil
}

func (s *BayesHttpService) GetUserBaseInfo(ctx context.Context, req *bayespb.GetUserBaseInfoRequest) (*bayespb.GetUserBaseInfoReply, error) {
	c := util.NewBaseCtx(ctx, s.logger)
	uid := util.GetUidFromCtx(ctx)
	GetUserBaseInfoRsp, err := s.data.RpcClient.UsercenterClient.GetUserInfo(ctx, &usercenterpb.GetUserInfoRequest{
		Uid:        req.Uid,
		Email:      req.Email,
		EoaAddress: req.EoaAddress,
		Address:    req.Address,
		Issuer:     req.Issuer,
		Name:       req.Name,
		InviteCode: req.InviteCode,
		Viewer:     uid,
	})
	if err != nil {
		c.Log.Errorf("rpc get user base info failed, err: [%+v]", err)
		return nil, err
	}

	if GetUserBaseInfoRsp.Uid == "" {
		return nil, errors.NotFound("NOT_FOUND", "user not found")
	}

	rsp := &bayespb.GetUserBaseInfoReply{
		Uid:        GetUserBaseInfoRsp.Uid,
		Email:      GetUserBaseInfoRsp.Email,
		EoaAddress: GetUserBaseInfoRsp.EoaAddress,
		Address:    GetUserBaseInfoRsp.Address,
		Issuer:     GetUserBaseInfoRsp.Issuer,
		Avatar:     GetUserBaseInfoRsp.Avatar,
		Name:       GetUserBaseInfoRsp.Name,
		Desc:       GetUserBaseInfoRsp.Desc,
		InviteCode: GetUserBaseInfoRsp.InviteCode,
		InviterUid: GetUserBaseInfoRsp.InviterUid,
		CreatedAt:  GetUserBaseInfoRsp.CreatedAt,

		PostCount:    GetUserBaseInfoRsp.PostCount,
		CommentCount: GetUserBaseInfoRsp.CommentCount,
		LikeCount:    GetUserBaseInfoRsp.LikeCount,
		IsFollowed:   bayespb.IsFollowed(GetUserBaseInfoRsp.IsFollow),

		FollowerCount:  GetUserBaseInfoRsp.FollowerCount,
		FollowingCount: GetUserBaseInfoRsp.FollowingCount,

		InviteByCode:    GetUserBaseInfoRsp.InviteByCode,
		InviterUserName: GetUserBaseInfoRsp.InviterUserName,
		InviteAt:        GetUserBaseInfoRsp.InviteAt,
	}

	return rsp, nil
}

func (s *BayesHttpService) Search(ctx context.Context, req *bayespb.SearchRequest) (*bayespb.SearchReply, error) {
	c := util.NewBaseCtx(ctx, s.logger)
	req.Keyword = strings.TrimSpace(req.Keyword)
	if req.Keyword == "" {
		return nil, pkg.ErrParam
	}

	reply := &bayespb.SearchReply{
		Users:   make([]*bayespb.SearchReply_User, 0),
		Markets: make([]*bayespb.SearchReply_Market, 0),
	}
	if req.SearchType == bayespb.SearchRequest_SEARCH_TYPE_UNSPECIFIED || req.SearchType == bayespb.SearchRequest_SEARCH_TYPE_MARKET {
		SearchMarketRsp, err := s.data.RpcClient.MarketcenterClient.SearchMarket(ctx, &marketcenterpb.SearchMarketRequest{
			Keyword:  req.Keyword,
			Page:     req.MarketPage,
			PageSize: req.MarketPageSize,
		})
		if err != nil {
			c.Log.Errorf("rpc search market failed, err: [%+v]", err)
			return nil, err
		}
		for _, market := range SearchMarketRsp.Markets {
			reply.Markets = append(reply.Markets, &bayespb.SearchReply_Market{
				Address:           market.Address,
				Name:              market.Name,
				PicUrl:            market.PicUrl,
				Description:       market.Description,
				Status:            market.Status,
				ParticipantsCount: market.ParticipantsCount,
				Volume:            market.Volume,
				Deadline:          uint64(market.Deadline),
			})
		}
		reply.MarketTotal = SearchMarketRsp.Total
	}
	if req.SearchType == bayespb.SearchRequest_SEARCH_TYPE_UNSPECIFIED || req.SearchType == bayespb.SearchRequest_SEARCH_TYPE_USER {
		SearchUserRsp, err := s.data.RpcClient.UsercenterClient.SearchUser(ctx, &usercenterpb.SearchUserRequest{
			Keyword:  req.Keyword,
			Page:     req.UserPage,
			PageSize: req.UserPageSize,
		})
		if err != nil {
			c.Log.Errorf("rpc search user failed, err: [%+v]", err)
			return nil, err
		}
		for _, user := range SearchUserRsp.Users {
			reply.Users = append(reply.Users, &bayespb.SearchReply_User{
				Uid:        user.Uid,
				Name:       user.Name,
				Avatar:     user.Avatar,
				Desc:       user.Desc,
				Address:    user.Address,
				EoaAddress: user.EoaAddress,
			})
		}
		reply.UserTotal = SearchUserRsp.Total
	}
	return reply, nil
}

func (s *BayesHttpService) GetUserNotifications(ctx context.Context, req *bayespb.GetUserNotificationsRequest) (*bayespb.GetUserNotificationsReply, error) {
	c := util.NewBaseCtx(ctx, s.logger)
	uid := util.GetUidFromCtx(ctx)
	if uid == "" {
		return nil, pkg.ErrParam
	}

	rsp := &bayespb.GetUserNotificationsReply{
		Notifications: make([]*bayespb.GetUserNotificationsReply_Notification, 0),
	}
	GetUserNotificationsRsp, err := s.data.RpcClient.UsercenterClient.GetUserNotifications(ctx, &usercenterpb.GetUserNotificationsRequest{
		Uid:           uid,
		Category:      req.Category,
		Status:        req.Status,
		Type:          req.Type,
		Page:          req.Page,
		PageSize:      req.PageSize,
		BaseTokenType: usercenterpb.BaseTokenType(util.GetBaseTokenFromCtx(ctx)),
	})
	if err != nil {
		c.Log.Errorf("rpc get user notifications failed, err: [%+v]", err)
		return nil, err
	}

	for _, notification := range GetUserNotificationsRsp.Notifications {
		rsp.Notifications = append(rsp.Notifications, &bayespb.GetUserNotificationsReply_Notification{
			Uid:       notification.Uid,
			Uuid:      notification.Uuid,
			Type:      notification.Type,
			Category:  notification.Category,
			Status:    notification.Status,
			BizJson:   notification.BizJson,
			CreatedAt: notification.CreatedAt,
		})
	}
	rsp.Total = GetUserNotificationsRsp.Total
	return rsp, nil
}

func (s *BayesHttpService) MarkNotificationsAsRead(ctx context.Context, req *bayespb.MarkNotificationsAsReadRequest) (*bayespb.MarkNotificationsAsReadReply, error) {
	c := util.NewBaseCtx(ctx, s.logger)
	uid := util.GetUidFromCtx(ctx)
	if uid == "" {
		return nil, pkg.ErrParam
	}
	_, err := s.data.RpcClient.UsercenterClient.MarkNotificationsAsRead(ctx, &usercenterpb.MarkNotificationsAsReadRequest{
		Uid:               uid,
		NotificationUuids: req.NotificationUuids,
	})
	if err != nil {
		c.Log.Errorf("rpc mark notifications as read failed, err: [%+v]", err)
		return nil, err
	}
	return &bayespb.MarkNotificationsAsReadReply{}, nil
}

func (s *BayesHttpService) GetInviteUserList(ctx context.Context, req *bayespb.GetInviteUserListRequest) (*bayespb.GetInviteUserListReply, error) {
	c := util.NewBaseCtx(ctx, s.logger)
	uid := util.GetUidFromCtx(ctx)
	if uid == "" {
		return nil, pkg.ErrParam
	}
	GetInviteUserListRsp, err := s.data.RpcClient.UsercenterClient.GetInviteUserList(ctx, &usercenterpb.GetInviteUserListRequest{
		Uid:      uid,
		Page:     req.Page,
		PageSize: req.PageSize,
	})
	if err != nil {
		c.Log.Errorf("rpc get invite user list failed, err: [%+v]", err)
		return nil, err
	}
	rsp := &bayespb.GetInviteUserListReply{
		Users: make([]*bayespb.GetInviteUserListReply_User, 0),
	}
	for _, user := range GetInviteUserListRsp.Users {
		rsp.Users = append(rsp.Users, &bayespb.GetInviteUserListReply_User{
			Uid:          user.Uid,
			Name:         user.Name,
			Avatar:       user.Avatar,
			Desc:         user.Desc,
			Email:        user.Email,
			CreatedAt:    user.CreatedAt,
			InviteAt:     user.InviteAt,
			RewardPoints: uint32(user.RewardPoints),
		})
	}
	rsp.Total = GetInviteUserListRsp.Total
	rsp.TotalRewardPoints = GetInviteUserListRsp.TotalRewardPoints
	return rsp, nil
}

func (s *BayesHttpService) GetBaseTokenConfig(ctx context.Context, req *bayespb.GetBaseTokenConfigRequest) (*bayespb.GetBaseTokenConfigReply, error) {

	rsp := &bayespb.GetBaseTokenConfigReply{
		BaseTokens: make([]*bayespb.GetBaseTokenConfigReply_BaseToken, 0),
	}
	rsp.BaseTokens = append(rsp.BaseTokens, &bayespb.GetBaseTokenConfigReply_BaseToken{
		BaseTokenType:     1,
		BaseTokenName:     s.custom.AssetTokens.Points.Name,
		BaseTokenSymbol:   s.custom.AssetTokens.Points.Symbol,
		BaseTokenAddress:  s.custom.AssetTokens.Points.Address,
		BaseTokenDecimals: s.custom.AssetTokens.Points.Decimals,
	})
	rsp.BaseTokens = append(rsp.BaseTokens, &bayespb.GetBaseTokenConfigReply_BaseToken{
		BaseTokenType:     2,
		BaseTokenName:     s.custom.AssetTokens.Usdc.Name,
		BaseTokenSymbol:   s.custom.AssetTokens.Usdc.Symbol,
		BaseTokenAddress:  s.custom.AssetTokens.Usdc.Address,
		BaseTokenDecimals: s.custom.AssetTokens.Usdc.Decimals,
	})
	return rsp, nil
}
