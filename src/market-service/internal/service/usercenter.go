package service

import (
	"context"
	"encoding/json"
	"fmt"
	usercenter "market-proto/proto/market-service/usercenter/v1"
	assetBiz "market-service/internal/biz/asset"
	"market-service/internal/biz/base"
	communityBiz "market-service/internal/biz/community"
	marketBiz "market-service/internal/biz/market"
	taskBiz "market-service/internal/biz/task"
	userBiz "market-service/internal/biz/user"
	"market-service/internal/pkg/alarm"
	"market-service/internal/pkg/common"
	"market-service/internal/pkg/util"
	"runtime/debug"
	"time"

	"github.com/go-kratos/kratos/v2/errors"
)

func (s *MarketService) CreateUser(ctx context.Context, req *usercenter.CreateUserRequest) (*usercenter.CreateUserReply, error) {
	c := common.NewBaseCtx(ctx, s.log)
	userEntity, err := s.userHandler.CreateUser(c, &userBiz.UserEntity{
		Email:         req.Email,
		EoaAddress:    req.EoaAddress,
		Address:       req.Address,
		Issuer:        req.Issuer,
		InviteCode:    req.InvitedByCode,
		Name:          req.Name,
		Description:   req.Desc,
		Avatar:        req.Avatar,
		PrivyUserInfo: json.RawMessage(req.PrivyUserInfo),
		Source:        userBiz.UserSourcePrivy,
	})
	if err != nil {
		return nil, err
	}

	go func(newCtx common.Ctx) {
		defer func() {
			if err := recover(); err != nil {
				newCtx.Log.Errorf("async ProduceNewUserStreamMsg panic err: %+v, stack: %+v", err, string(debug.Stack()))
				alarm.Lark.Send(fmt.Sprintf("async ProduceNewUserStreamMsg panic err: %+v, stack: %+v", err, string(debug.Stack())))
			}
		}()

		msgId, err := s.userHandler.ProduceNewUserStreamMsg(newCtx, userEntity)
		if err != nil {
			alarm.Lark.Send(fmt.Sprintf("async ProduceNewUserStreamMsg to new user [%s] error: %+v", userEntity.UID, err))
			newCtx.Log.Errorf("async ProduceNewUserStreamMsg error: %+v", err)
			return
		}
		newCtx.Log.Infof("async ProduceNewUserStreamMsg success, msgId: %s", msgId)
	}(common.CloneBaseCtx(c, s.log))

	return &usercenter.CreateUserReply{Uid: userEntity.UID}, nil
}

func (s *MarketService) GetUserInfo(ctx context.Context, req *usercenter.GetUserInfoRequest) (*usercenter.GetUserInfoReply, error) {
	c := common.NewBaseCtx(ctx, s.log)
	userEntity, err := s.userHandler.GetUserInfo(c, &userBiz.UserQuery{
		Address:    req.Address,
		Email:      req.Email,
		UID:        req.Uid,
		Issuer:     req.Issuer,
		Name:       req.Name,
		InviteCode: req.InviteCode,
		EoaAddress: req.EoaAddress,
	})
	if err != nil {
		return nil, err
	}
	if userEntity == nil || userEntity.UID == "" {
		return &usercenter.GetUserInfoReply{}, nil
	}
	userCommunityInfo, err := s.communityHandler.GetUserCommunityInfo(c, &communityBiz.UserCommunityInfoQuery{
		UID: userEntity.UID,
	})
	if err != nil {
		return nil, err
	}
	isFollowed := usercenter.IsFollowedUser_IS_FOLLOWED_NO
	if req.Viewer != "" {
		isFollow, err := s.userHandler.IsFollowed(c, req.Viewer, userEntity.UID)
		if err != nil {
			return nil, err
		}
		if isFollow {
			isFollowed = usercenter.IsFollowedUser_IS_FOLLOWED_YES
		}
	}

	inviteByCode := ""
	inviterUserName := ""
	inviterUid := ""
	if userEntity.InviterUID != "" && userEntity.InviterUID != "0" {
		inviter, err := s.userHandler.GetUserInfo(c, &userBiz.UserQuery{
			UID: userEntity.InviterUID,
		})
		if err != nil {
			return nil, err
		}
		if inviter == nil || inviter.UID == "" {
			c.Log.Errorf("inviter not found, uid: %s", userEntity.InviterUID)
			return nil, errors.New(int(usercenter.ErrorCode_PARAM), "PARAM_ERROR", "inviter not found")
		}
		inviteByCode = inviter.InviteCode
		inviterUserName = inviter.Name
		inviterUid = inviter.UID
	}

	return &usercenter.GetUserInfoReply{
		Uid:             userEntity.UID,
		Email:           userEntity.Email,
		Address:         userEntity.Address,
		EoaAddress:      userEntity.EoaAddress,
		Issuer:          userEntity.Issuer,
		Name:            userEntity.Name,
		Avatar:          userEntity.Avatar,
		Desc:            userEntity.Description,
		InviteCode:      userEntity.InviteCode,
		InviterUid:      inviterUid,
		CreatedAt:       userEntity.CreatedAt.Unix(),
		PostCount:       userCommunityInfo.PostCount,
		CommentCount:    userCommunityInfo.CommentCount,
		LikeCount:       userCommunityInfo.LikeCount,
		IsFollow:        isFollowed,
		FollowerCount:   userEntity.FollowerCount,
		FollowingCount:  userEntity.FollowCount,
		InviteByCode:    inviteByCode,
		InviterUserName: inviterUserName,
		InviteAt:        uint32(userEntity.InviteAt),
	}, nil
}

func (s *MarketService) SetUserInfo(ctx context.Context, req *usercenter.SetUserInfoRequest) (*usercenter.SetUserInfoReply, error) {
	c := common.NewBaseCtx(ctx, s.log)
	inviter, err := s.userHandler.UpdateUser(c, &userBiz.UserEntity{
		UID:         req.Uid,
		Name:        req.Name,
		Description: req.Desc,
		Avatar:      req.Avatar,
	}, req.InviteByCode)
	if err != nil {
		return nil, err
	}

	if inviter == nil {
		return &usercenter.SetUserInfoReply{}, nil
	}

	go func(newCtx common.Ctx) {
		defer func() {
			if err := recover(); err != nil {
				newCtx.Log.Errorf("async produce bind invite relation stream msg panic err: %+v, stack: %+v", err, string(debug.Stack()))
				alarm.Lark.Send(fmt.Sprintf("async produce bind invite relation stream msg panic err: %+v, stack: %+v", err, string(debug.Stack())))
			}
		}()

		bindInviteRelationStreamMsg := &userBiz.BindInviteRelationStreamMsg{
			InviterUID: inviter.UID,
			InviteeUID: req.Uid,
			Timestamp:  time.Now().Unix(),
		}
		msgId, err := s.userHandler.ProduceBindInviteRelationStreamMsg(newCtx, bindInviteRelationStreamMsg)
		if err != nil {
			newCtx.Log.Errorf("async produce bind invite relation stream msg error: %+v", err)
			alarm.Lark.Send(fmt.Sprintf("async produce bind invite relation stream msg error: %+v", err))
			return
		}
		newCtx.Log.Infof("async produce bind invite relation stream msg success, msgId: %s", msgId)
	}(common.CloneBaseCtx(c, s.log))

	return &usercenter.SetUserInfoReply{}, nil
}

func (s *MarketService) PublishPost(ctx context.Context, req *usercenter.PublishPostRequest) (*usercenter.PublishPostReply, error) {
	c := common.NewBaseCtx(ctx, s.log)
	marketEntity, err := s.marketHandler.GetMarket(c, &marketBiz.MarketQuery{
		Address: req.MarketAddress,
	})
	if err != nil {
		return nil, err
	}
	if marketEntity == nil || marketEntity.Address == "" {
		return nil, marketBiz.ErrMarketNotFound.WithCause(fmt.Errorf("market not found"))
	}

	postUuid, err := s.communityHandler.PublishPost(c, &communityBiz.PostEntity{
		UID:           req.Uid,
		MarketAddress: marketEntity.Address,
		Title:         req.Title,
		Content:       req.Content,
	})
	if err != nil {
		return nil, err
	}
	return &usercenter.PublishPostReply{PostUuid: postUuid}, nil

}

func (s *MarketService) PublishComment(ctx context.Context, req *usercenter.PublishCommentRequest) (*usercenter.PublishCommentReply, error) {
	c := common.NewBaseCtx(ctx, s.log)

	commentUuid, notifyUid, marketAddress, err := s.communityHandler.PublishComment(c, &communityBiz.CommentEntity{
		UID:           req.Uid,
		MarketAddress: req.MarketAddress,
		PostUUID:      req.PostUuid,
		RootUUID:      req.RootUuid,
		ParentUUID:    req.ParentUuid,
		Content:       req.Content,
	})
	if err != nil {
		return nil, err
	}

	go func(newCtx common.Ctx) {
		defer func() {
			if err := recover(); err != nil {
				newCtx.Log.Errorf("async generate comment notification error: %+v, stack: %s", err, string(debug.Stack()))
			}
		}()
		userEntity, err := s.userHandler.GetUserInfo(newCtx, &userBiz.UserQuery{
			UID: req.Uid,
		})
		if err != nil {
			newCtx.Log.Errorf("async generate comment notification GetUserInfo error: %+v", err)
			return
		}
		if userEntity == nil || userEntity.UID == "" {
			newCtx.Log.Errorf("async generate comment notification GetUserInfo error: userEntity is nil")
			return
		}

		commentEntity, err := s.communityHandler.GetComment(newCtx, &communityBiz.CommentQuery{
			UUID: commentUuid,
		})
		if err != nil {
			newCtx.Log.Errorf("async generate comment notification GetPost error: %+v", err)
			return
		}
		if commentEntity == nil || commentEntity.UUID == "" {
			newCtx.Log.Errorf("async generate comment notification GetComment error: commentEntity is nil")
			return
		}

		postEntity, err := s.communityHandler.GetPost(newCtx, &communityBiz.PostQuery{
			UUID: req.PostUuid,
		})
		if err != nil {
			newCtx.Log.Errorf("async generate comment notification GetPost error: %+v", err)
			return
		}
		if postEntity == nil || postEntity.UUID == "" {
			newCtx.Log.Errorf("async generate comment notification GetPost error: postEntity is nil")
			return
		}

		marketEntity, err := s.marketHandler.GetMarket(newCtx, &marketBiz.MarketQuery{
			Address: marketAddress,
		})
		if err != nil {
			newCtx.Log.Errorf("async generate comment notification GetMarket error: %+v", err)
			return
		}
		if marketEntity == nil || marketEntity.Address == "" {
			newCtx.Log.Errorf("async generate comment notification GetMarket error: marketEntity is nil")
			return
		}

		bizData, err := json.Marshal(&userBiz.ReceiveCommentNotificationEntity{
			PostUUID: req.PostUuid,
			PostId:   uint(postEntity.Id),

			CommentId:      uint64(commentEntity.Id),
			CommentUUID:    commentUuid,
			CommentContent: req.Content,

			CommentUID:    userEntity.UID,
			CommentName:   userEntity.Name,
			CommentAvatar: userEntity.Avatar,

			CommentType: func() uint8 {
				if req.ParentUuid == "" && req.RootUuid == "" {
					return userBiz.CommentTypeRoot
				}
				return userBiz.CommentTypeReply
			}(),
			MarketAddress: marketAddress,
		})

		if err != nil {
			newCtx.Log.Errorf("async generate comment notification Marshal error: %+v", err)
			return
		}
		err = s.userHandler.GenerateNewUserNotification(newCtx, &userBiz.UserNotificationEntity{
			UUID:          util.GenerateUUID(),
			UID:           notifyUid,
			BizJson:       bizData,
			Type:          userBiz.NotificationTypeReceiveComment,
			Category:      uint8(userBiz.NotificationCategoryCommunity),
			Status:        userBiz.NotificationStatusUnRead,
			BaseTokenType: uint8(marketEntity.TokenType),
		})
		if err != nil {
			newCtx.Log.Errorf("async generate comment notification GenerateNewUserNotification error: %+v", err)
		}
	}(common.CloneBaseCtx(c, s.log))

	return &usercenter.PublishCommentReply{CommentUuid: commentUuid}, nil
}

func (s *MarketService) UpdateLikeContentStatus(ctx context.Context, req *usercenter.UpdateLikeContentStatusRequest) (*usercenter.UpdateLikeContentStatusReply, error) {
	c := common.NewBaseCtx(ctx, s.log)

	notifyUid, err := s.communityHandler.UpdateLikeContentStatus(c, &communityBiz.UserLikeEntity{
		UID:         req.Uid,
		ContentUUID: req.ContentUuid,
		Type:        uint8(req.ContentType),
		Status:      uint8(req.Status),
	})
	if err != nil {
		return nil, err
	}

	if req.Status == usercenter.UpdateLikeContentStatusRequest_Status(usercenter.IsLike_IS_LIKE_YES) && req.ContentType == usercenter.UpdateLikeContentStatusRequest_CONTENT_TYPE_POST {
		go func(newCtx common.Ctx) {
			defer func() {
				if err := recover(); err != nil {
					newCtx.Log.Errorf("async generate receive like notification panic err: %+v, stack: %+v", err, string(debug.Stack()))
				}
			}()

			userEntity, err := s.userHandler.GetUserInfo(newCtx, &userBiz.UserQuery{
				UID: req.Uid,
			})
			if err != nil {
				newCtx.Log.Errorf("async generate receive like notification GetUserInfo error: %+v", err)
				return
			}
			if userEntity == nil || userEntity.UID == "" {
				newCtx.Log.Errorf("async generate receive like notification GetUserInfo error: userEntity is nil")
				return
			}

			postEntity, err := s.communityHandler.GetPost(newCtx, &communityBiz.PostQuery{
				UUID: req.ContentUuid,
			})
			if err != nil {
				newCtx.Log.Errorf("async generate receive like notification GetPost error: %+v", err)
				return
			}
			if postEntity == nil || postEntity.UUID == "" {
				newCtx.Log.Errorf("async generate receive like notification GetPost error: postEntity is nil")
				return
			}

			bizData, err := json.Marshal(&userBiz.ReceiveLikeNotificationEntity{
				PostUUID:       req.ContentUuid,
				PostId:         uint(postEntity.Id),
				MarketAddress:  postEntity.MarketAddress,
				NewLikeCount:   1,
				LikeUserUID:    userEntity.UID,
				LikeUserName:   userEntity.Name,
				LikeUserAvatar: userEntity.Avatar,
			})
			if err != nil {
				newCtx.Log.Errorf("async generate receive like notification Marshal error: %+v", err)
				return
			}
			err = s.userHandler.CreateOrUpdateUserPostLikeNotification(newCtx, &userBiz.UserNotificationEntity{
				UUID:     util.GenerateUUID(),
				UID:      notifyUid,
				BizJson:  bizData,
				Type:     userBiz.NotificationTypeReceiveLike,
				Category: uint8(userBiz.NotificationCategoryCommunity),
				Status:   userBiz.NotificationStatusUnRead,
			})
			if err != nil {
				newCtx.Log.Errorf("async generate receive like notification CreateOrUpdateUserPostLikeNotification error: %+v", err)
			}
		}(common.CloneBaseCtx(c, s.log))
	}
	return &usercenter.UpdateLikeContentStatusReply{}, nil
}

func (s *MarketService) UploadFileToBizBucketS3(ctx context.Context, req *usercenter.UploadFileToBizBucketS3Request) (*usercenter.UploadFileToBizBucketS3Reply, error) {
	c := common.NewBaseCtx(ctx, s.log)

	key, err := s.userHandler.UploadFileToBizBucketS3(c, req.FileData, req.Biz)
	if err != nil {
		return nil, err
	}
	return &usercenter.UploadFileToBizBucketS3Reply{FileUrl: key}, nil
}

func (s *MarketService) DownloadFileFromBizBucketS3(ctx context.Context, req *usercenter.DownloadFileFromBizBucketS3Request) (*usercenter.DownloadFileFromBizBucketS3Reply, error) {
	c := common.NewBaseCtx(ctx, s.log)

	fileData, contentType, err := s.userHandler.DownloadFileFromS3(c, req.FileUrl)
	if err != nil {
		return nil, err
	}
	return &usercenter.DownloadFileFromBizBucketS3Reply{FileData: fileData, ContentType: contentType}, nil
}

func (s *MarketService) GetUserInfosByUids(ctx context.Context, req *usercenter.GetUserInfosByUidsRequest) (*usercenter.GetUserInfosByUidsReply, error) {
	c := common.NewBaseCtx(ctx, s.log)

	userEntities, err := s.userHandler.GetUsersInfo(c, &userBiz.UserQuery{
		UIDList: req.Uids,
	})
	if err != nil {
		return nil, err
	}

	rsp := &usercenter.GetUserInfosByUidsReply{}
	for _, userEntity := range userEntities {
		rsp.UserInfos = append(rsp.UserInfos, &usercenter.GetUserInfosByUidsReply_UserInfo{
			Uid:        userEntity.UID,
			Email:      userEntity.Email,
			Address:    userEntity.Address,
			EoaAddress: userEntity.EoaAddress,
			Issuer:     userEntity.Issuer,
			Name:       userEntity.Name,
			Avatar:     userEntity.Avatar,
			Desc:       userEntity.Description,
			InviteCode: userEntity.InviteCode,
			InviterUid: userEntity.InviterUID,
			CreatedAt:  userEntity.CreatedAt.Unix(),
			UpdatedAt:  userEntity.UpdatedAt.Unix(),
		})
	}
	return rsp, nil
}

func (s *MarketService) GetMarketPostsAndPublishers(ctx context.Context, req *usercenter.GetMarketPostsAndPublishersRequest) (*usercenter.GetMarketPostsAndPublishersResponse, error) {
	c := common.NewBaseCtx(ctx, s.log)

	if req.Page <= 0 {
		req.Page = 1
	}
	if req.PageSize <= 0 {
		req.PageSize = 10
	} else if req.PageSize > 100 {
		req.PageSize = 100
	}

	rsp := &usercenter.GetMarketPostsAndPublishersResponse{
		Total: 0,
		Posts: make([]*usercenter.GetMarketPostsAndPublishersResponse_Post, 0),
	}

	query := &communityBiz.PostQuery{
		MarketAddress: req.MarketAddress,
		Status:        communityBiz.PostStatusNormal,
		BaseQuery: base.BaseQuery{
			Order: "id desc",
			Limit: int32(req.PageSize),
		},
	}
	if req.LastId > 0 {
		query.IdLessThan = int64(req.LastId)
	} else {
		query.Offset = int32((req.Page - 1) * req.PageSize)
	}

	postEntities, total, err := s.communityHandler.GetPostsWithTotal(c, query)
	if err != nil {
		return nil, err
	}
	if len(postEntities) == 0 {
		return rsp, nil
	}

	postUuids := make([]string, 0, len(postEntities))
	uidList := make([]string, 0, len(postEntities))
	for _, postEntity := range postEntities {
		postUuids = append(postUuids, postEntity.UUID)
		uidList = append(uidList, postEntity.UID)
	}

	userEntities, err := s.userHandler.GetUsersInfo(c, &userBiz.UserQuery{
		UIDList: uidList,
	})
	if err != nil {
		return nil, err
	}

	userPostLikeMap := make(map[string]bool)
	if req.Uid != "" {
		userLikeEntities, err := s.communityHandler.GetUserLikes(c, &communityBiz.UserLikeQuery{
			UID:             req.Uid,
			Type:            communityBiz.UserLikeTypePost,
			ContentUUIDList: postUuids,
			Status:          communityBiz.UserLikeStatusNormal,
		})
		if err != nil {
			return nil, err
		}
		for _, userLikeEntity := range userLikeEntities {
			userPostLikeMap[userLikeEntity.ContentUUID] = true
		}
	}

	uidToUserMap := make(map[string]*userBiz.UserEntity)
	for _, userEntity := range userEntities {
		uidToUserMap[userEntity.UID] = userEntity
	}
	for _, postEntity := range postEntities {
		postInfo := &usercenter.GetMarketPostsAndPublishersResponse_Post{
			Uid:           postEntity.UID,
			Uuid:          postEntity.UUID,
			MarketAddress: postEntity.MarketAddress,
			Title:         postEntity.Title,
			Content:       postEntity.Content,
			LikeCount:     int64(postEntity.LikeCount),
			CommentCount:  int64(postEntity.CommentCount),
			ViewCount:     int64(postEntity.ViewCount),
			CreatedAt:     postEntity.CreatedAt.Unix(),
			IsLike:        uint32(usercenter.IsLike_IS_LIKE_NO),
			Id:            int64(postEntity.Id),
		}
		if userEntity, ok := uidToUserMap[postEntity.UID]; ok {
			postInfo.UserName = userEntity.Name
			postInfo.UserAvatarUrl = userEntity.Avatar
		}

		if userPostLikeMap[postEntity.UUID] {
			postInfo.IsLike = uint32(usercenter.IsLike_IS_LIKE_YES)
		}
		rsp.Posts = append(rsp.Posts, postInfo)
	}
	rsp.Total = total
	return rsp, nil
}

func (s *MarketService) BatchGetCommentReplysAndUsers(ctx context.Context, req *usercenter.BatchGetCommentReplysAndUsersRequest) (*usercenter.BatchGetCommentReplysAndUsersResponse, error) {
	c := common.NewBaseCtx(ctx, s.log)

	rsp := &usercenter.BatchGetCommentReplysAndUsersResponse{
		CommentAndReplys: make([]*usercenter.BatchGetCommentReplysAndUsersResponse_CommentAndReplys, 0),
	}

	replys, err := s.communityHandler.GetNewReplysForEachComment(c, req.RootUuids, int(req.ReplyPageSize), req.Uid)
	if err != nil {
		return nil, err
	}

	if len(replys) == 0 {
		return rsp, nil
	}

	replyCountsMap, err := s.communityHandler.GetCommentCountsByRootUUIDs(c, req.RootUuids)
	if err != nil {
		return nil, err
	}

	uidList := make([]string, 0, len(replys))
	for _, reply := range replys {
		uidList = append(uidList, reply.UID)
		if reply.ParentUserUID != "" {
			uidList = append(uidList, reply.ParentUserUID)
		}
	}
	uidList = util.RemoveDuplicate(uidList)
	userEntities, err := s.userHandler.GetUsersInfo(c, &userBiz.UserQuery{
		UIDList: uidList,
	})
	if err != nil {
		return nil, err
	}

	uidToUserMap := make(map[string]*userBiz.UserEntity)
	for _, userEntity := range userEntities {
		uidToUserMap[userEntity.UID] = userEntity
	}

	rootUuidToReplysMap := make(map[string]*usercenter.BatchGetCommentReplysAndUsersResponse_CommentAndReplys)
	for _, rootUuid := range req.RootUuids {
		rootUuidToReplysMap[rootUuid] = &usercenter.BatchGetCommentReplysAndUsersResponse_CommentAndReplys{
			RootUuid: rootUuid,
			Replies:  make([]*usercenter.BatchGetCommentReplysAndUsersResponse_CommentAndReplys_Reply, 0),
		}

		if replyCount, ok := replyCountsMap[rootUuid]; ok {
			rootUuidToReplysMap[rootUuid].Total = int64(replyCount)
		}

	}
	for _, reply := range replys {
		replyInfo := &usercenter.BatchGetCommentReplysAndUsersResponse_CommentAndReplys_Reply{
			Uid:           reply.UID,
			Uuid:          reply.UUID,
			Content:       reply.Content,
			LikeCount:     int64(reply.LikeCount),
			CreatedAt:     reply.CreatedAt.Unix(),
			IsLike:        usercenter.IsLike(reply.IsLike),
			ParentUuid:    reply.ParentUUID,
			RootUuid:      reply.RootUUID,
			MarketAddress: reply.MarketAddress,
			Id:            int64(reply.Id),
		}

		if userEntity, ok := uidToUserMap[reply.UID]; ok {
			replyInfo.UserName = userEntity.Name
			replyInfo.UserAvatarUrl = userEntity.Avatar
		}

		if parentUserEntity, ok := uidToUserMap[reply.ParentUserUID]; ok {
			replyInfo.ParentUserName = parentUserEntity.Name
			replyInfo.ParentUserAvatarUrl = parentUserEntity.Avatar
		}

		rootUuidToReplysMap[reply.RootUUID].Replies = append(rootUuidToReplysMap[reply.RootUUID].Replies, replyInfo)
	}

	for _, rootUuid := range req.RootUuids {
		if commentAndReplys, ok := rootUuidToReplysMap[rootUuid]; ok {
			rsp.CommentAndReplys = append(rsp.CommentAndReplys, commentAndReplys)
		}
	}

	return rsp, nil
}

func (s *MarketService) GetComments(ctx context.Context, req *usercenter.GetCommentsRequest) (*usercenter.GetCommentsResponse, error) {
	c := common.NewBaseCtx(ctx, s.log)

	if req.LastId <= 0 && req.Page <= 0 {
		req.Page = 1
	}
	if req.PageSize <= 0 {
		req.PageSize = 10
	} else if req.PageSize > 100 {
		req.PageSize = 100
	}

	query := &communityBiz.CommentQuery{
		PostUUID: req.PostUuid,
		Status:   communityBiz.CommentStatusNormal,
		BaseQuery: base.BaseQuery{
			Order: "id desc",
			Limit: int32(req.PageSize),
		},
	}
	if req.LastId > 0 {
		query.IdLessThan = req.LastId
	} else {
		query.Offset = int32((req.Page - 1) * req.PageSize)
	}

	// 如果root_uuid不为空，则只查回复评论
	if req.RootUuid != "" {
		query.RootUUID = req.RootUuid
	} else { // root_uuid=="" 只查根评论
		query.ParentUUIDIsNil = true
		query.RootUUIDIsNil = true
	}

	commentEntities, total, err := s.communityHandler.GetCommentsWithTotal(c, query)
	if err != nil {
		return nil, err
	}
	if len(commentEntities) == 0 {
		return &usercenter.GetCommentsResponse{}, nil
	}

	// 查点赞信息
	userLikeMap := make(map[string]bool)
	if req.Uid != "" {
		commentIdList := make([]string, 0, len(commentEntities))
		for _, commentEntity := range commentEntities {
			commentIdList = append(commentIdList, commentEntity.UUID)
		}

		userLikeEntities, err := s.communityHandler.GetUserLikes(c, &communityBiz.UserLikeQuery{
			UID:             req.Uid,
			Type:            communityBiz.UserLikeTypeComment,
			ContentUUIDList: commentIdList,
			Status:          communityBiz.UserLikeStatusNormal,
		})
		if err != nil {
			return nil, err
		}
		for _, userLikeEntity := range userLikeEntities {
			userLikeMap[userLikeEntity.ContentUUID] = true
		}
	}

	// 查用户信息
	uidList := make([]string, 0)
	for _, commentEntity := range commentEntities {
		uidList = append(uidList, commentEntity.UID)
		if commentEntity.ParentUserUID != "" {
			uidList = append(uidList, commentEntity.ParentUserUID)
		}
	}
	uidList = util.RemoveDuplicate(uidList)
	if req.Uid != "" {
		uidList = append(uidList, req.Uid)
	}
	userEntities, err := s.userHandler.GetUsersInfo(c, &userBiz.UserQuery{
		UIDList: uidList,
	})
	if err != nil {
		return nil, err
	}

	uidToUserMap := make(map[string]*userBiz.UserEntity)
	for _, userEntity := range userEntities {
		uidToUserMap[userEntity.UID] = userEntity
	}

	rspCommentList := make([]*usercenter.GetCommentsResponse_Comment, 0, len(commentEntities))
	for _, commentEntity := range commentEntities {
		commentInfo := &usercenter.GetCommentsResponse_Comment{
			Id:            int64(commentEntity.Id),
			Uuid:          commentEntity.UUID,
			Uid:           commentEntity.UID,
			ParentUuid:    commentEntity.ParentUUID,
			RootUuid:      commentEntity.RootUUID,
			Content:       commentEntity.Content,
			LikeCount:     int64(commentEntity.LikeCount),
			CreatedAt:     commentEntity.CreatedAt.Unix(),
			IsLike:        usercenter.IsLike_IS_LIKE_NO,
			MarketAddress: commentEntity.MarketAddress,
		}

		if commentEntity.ParentUserUID != "" {
			if userEntity, ok := uidToUserMap[commentEntity.ParentUserUID]; ok {
				commentInfo.ParentUserName = userEntity.Name
				commentInfo.ParentUserAvatarUrl = userEntity.Avatar
			}
		}

		if userEntity, ok := uidToUserMap[commentEntity.UID]; ok {
			commentInfo.UserName = userEntity.Name
			commentInfo.UserAvatarUrl = userEntity.Avatar
		}

		if userLikeMap[commentEntity.UUID] {
			commentInfo.IsLike = usercenter.IsLike_IS_LIKE_YES
		}
		rspCommentList = append(rspCommentList, commentInfo)
	}
	return &usercenter.GetCommentsResponse{
		Total:    total,
		Comments: rspCommentList,
	}, nil
}

func (s *MarketService) UpdateFollowStatus(ctx context.Context, req *usercenter.UpdateFollowStatusRequest) (*usercenter.UpdateFollowStatusReply, error) {
	c := common.NewBaseCtx(ctx, s.log)
	err := s.userHandler.UpdateUserFollowStatus(c, &userBiz.UserFollowEntity{
		UID:       req.Uid,
		FollowUID: req.FollowUid,
		Status:    uint8(req.Status),
	})
	if err != nil {
		return nil, err
	}

	/*go func(newCtx common.Ctx) {
		defer func() {
			if err := recover(); err != nil {
				newCtx.Log.Errorf("async generate follow notification panic err: %+v, stack: %+v", err, string(debug.Stack()))
			}
		}()

		userEntity, err := s.userHandler.GetUserInfo(newCtx, &userBiz.UserQuery{
			UID: req.Uid,
		})
		if err != nil {
			newCtx.Log.Errorf("async generate follow notification GetUserInfo error: %+v", err)
			return
		}
		if userEntity == nil || userEntity.UID == "" {
			newCtx.Log.Errorf("async generate follow notification GetUserInfo error: userEntity is nil")
			return
		}
		bizData, err := json.Marshal(&userBiz.ReceiveFollowNotificationEntity{
			FollowerUID:    req.Uid,
			FollowerName:   userEntity.Name,
			FollowerAvatar: userEntity.Avatar,
		})
		if err != nil {
			newCtx.Log.Errorf("async generate follow notification Marshal error: %+v", err)
			return
		}
		err = s.userHandler.GenerateNewUserNotification(newCtx, &userBiz.UserNotificationEntity{
			UUID:     util.GenerateUUID(),
			UID:      req.FollowUid,
			BizJson:  bizData,
			Type:     userBiz.NotificationTypeReceiveFollow,
			Category: uint8(userBiz.NotificationCategoryCommunity),
			Status:   userBiz.NotificationStatusUnRead,
		})
		if err != nil {
			newCtx.Log.Errorf("async generate follow notification GenerateNewUserNotification error: %+v", err)
		}
	}(common.CloneBaseCtx(c, s.log))*/
	return &usercenter.UpdateFollowStatusReply{}, nil
}

func (s *MarketService) SearchUser(ctx context.Context, req *usercenter.SearchUserRequest) (*usercenter.SearchUserResponse, error) {
	c := common.NewBaseCtx(ctx, s.log)

	if req.Page <= 0 {
		req.Page = 1
	}
	if req.PageSize <= 0 {
		req.PageSize = 10
	} else if req.PageSize > 100 {
		req.PageSize = 100
	}

	userEntities, total, err := s.userHandler.SearchUser(c, &userBiz.UserQuery{
		Search: req.Keyword,
		BaseQuery: base.BaseQuery{
			Order:  "id desc",
			Limit:  int32(req.PageSize),
			Offset: int32((req.Page - 1) * req.PageSize),
		},
	})
	if err != nil {
		return nil, err
	}

	rsp := &usercenter.SearchUserResponse{
		Total: uint32(total),
	}
	for _, userEntity := range userEntities {
		rsp.Users = append(rsp.Users, &usercenter.SearchUserResponse_User{
			Uid:        userEntity.UID,
			Address:    userEntity.Address,
			Name:       userEntity.Name,
			Avatar:     userEntity.Avatar,
			Desc:       userEntity.Description,
			EoaAddress: userEntity.EoaAddress,
		})
	}
	return rsp, nil
}

func (s *MarketService) GetUsersInfoByAddresses(ctx context.Context, req *usercenter.GetUsersInfoByAddressesRequest) (*usercenter.GetUsersInfoByAddressesReply, error) {
	c := common.NewBaseCtx(ctx, s.log)

	userEntities, err := s.userHandler.GetUsersInfo(c, &userBiz.UserQuery{
		AddressList: req.Addresses,
	})
	if err != nil {
		return nil, err
	}

	rsp := &usercenter.GetUsersInfoByAddressesReply{}
	for _, userEntity := range userEntities {
		rsp.Users = append(rsp.Users, &usercenter.GetUsersInfoByAddressesReply_User{
			Uid:        userEntity.UID,
			Name:       userEntity.Name,
			Avatar:     userEntity.Avatar,
			Desc:       userEntity.Description,
			Address:    userEntity.Address,
			EoaAddress: userEntity.EoaAddress,
			Issuer:     userEntity.Issuer,
		})
	}
	return rsp, nil
}

func (s *MarketService) BatchGetMarketPostAndPublisher(ctx context.Context, req *usercenter.BatchGetMarketPostAndPublisherRequest) (*usercenter.BatchGetMarketPostAndPublisherResponse, error) {
	c := common.NewBaseCtx(ctx, s.log)

	rsp := &usercenter.BatchGetMarketPostAndPublisherResponse{
		Posts: make([]*usercenter.BatchGetMarketPostAndPublisherResponse_Post, 0),
	}

	postEntities, err := s.communityHandler.GetTopOnePostForEachMarketAddress(c, req.MarketAddresses, req.Uid)
	if err != nil {
		return nil, err
	}
	if len(postEntities) == 0 {
		return rsp, nil
	}

	publisherUids := make([]string, 0, len(postEntities))
	for _, postEntity := range postEntities {
		publisherUids = append(publisherUids, postEntity.UID)
	}

	userEntities, err := s.userHandler.GetUsersInfo(c, &userBiz.UserQuery{
		UIDList: publisherUids,
	})
	if err != nil {
		return nil, err
	}

	uidToUserinfoMap := make(map[string]*userBiz.UserEntity)
	for _, userEntity := range userEntities {
		uidToUserinfoMap[userEntity.UID] = userEntity
	}

	for _, postEntity := range postEntities {
		rspPost := &usercenter.BatchGetMarketPostAndPublisherResponse_Post{
			MarketAddress: postEntity.MarketAddress,
			Uid:           postEntity.UID,
			PostUuid:      postEntity.UUID,
			Title:         postEntity.Title,
			Content:       postEntity.Content,
			LikeCount:     int64(postEntity.LikeCount),
			CommentCount:  int64(postEntity.CommentCount),
			ViewCount:     int64(postEntity.ViewCount),
			CreatedAt:     postEntity.CreatedAt.Unix(),
			IsLike:        usercenter.IsLike(postEntity.IsLike),
			Id:            int64(postEntity.Id),
		}
		if userEntity, ok := uidToUserinfoMap[postEntity.UID]; ok {
			rspPost.UserName = userEntity.Name
			rspPost.UserAvatarUrl = userEntity.Avatar
		}
		rsp.Posts = append(rsp.Posts, rspPost)
	}
	return rsp, nil
}

func (s *MarketService) GetUserNotifications(ctx context.Context, req *usercenter.GetUserNotificationsRequest) (*usercenter.GetUserNotificationsResponse, error) {
	c := common.NewBaseCtx(ctx, s.log)

	if req.Page <= 0 {
		req.Page = 1
	}
	if req.PageSize <= 0 {
		req.PageSize = 10
	} else if req.PageSize > 100 {
		req.PageSize = 100
	}
	rsp := &usercenter.GetUserNotificationsResponse{
		Notifications: make([]*usercenter.GetUserNotificationsResponse_Notification, 0),
	}

	userNotifications, total, err := s.userHandler.GetUserNotificationsWithTotal(c, &userBiz.UserNotificationQuery{
		UID:           req.Uid,
		Category:      uint8(req.Category),
		Type:          uint8(req.Type),
		Status:        uint8(req.Status),
		BaseTokenType: uint8(req.BaseTokenType),
		BaseQuery: base.BaseQuery{
			Order:  "id desc",
			Limit:  int32(req.PageSize),
			Offset: int32((req.Page - 1) * req.PageSize),
		},
	})
	if err != nil {
		return nil, err
	}

	for _, userNotification := range userNotifications {
		rsp.Notifications = append(rsp.Notifications, &usercenter.GetUserNotificationsResponse_Notification{
			Uuid:      userNotification.UUID,
			Category:  uint32(userNotification.Category),
			Type:      uint32(userNotification.Type),
			Status:    uint32(userNotification.Status),
			BizJson:   string(userNotification.BizJson),
			CreatedAt: userNotification.CreatedAt.Unix(),
		})
	}
	rsp.Total = uint32(total)
	return rsp, nil
}

func (s *MarketService) MarkNotificationsAsRead(ctx context.Context, req *usercenter.MarkNotificationsAsReadRequest) (*usercenter.MarkNotificationsAsReadResponse, error) {
	c := common.NewBaseCtx(ctx, s.log)
	err := s.userHandler.UpdateNotificationsToRead(c, req.Uid, req.NotificationUuids)
	if err != nil {
		return nil, err
	}
	return &usercenter.MarkNotificationsAsReadResponse{}, nil
}

func (s *MarketService) GetInviteUserList(ctx context.Context, req *usercenter.GetInviteUserListRequest) (*usercenter.GetInviteUserListReply, error) {
	c := common.NewBaseCtx(ctx, s.log)
	rsp := &usercenter.GetInviteUserListReply{
		Users: make([]*usercenter.GetInviteUserListReply_User, 0),
	}

	if req.Page <= 0 {
		req.Page = 1
	}
	if req.PageSize <= 0 {
		req.PageSize = 10
	} else if req.PageSize > 100 {
		req.PageSize = 100
	}
	userEntities, total, err := s.userHandler.GetUsersWithTotal(c, &userBiz.UserQuery{
		InviterUID: req.Uid,
		BaseQuery: base.BaseQuery{
			Order:  "id desc",
			Limit:  int32(req.PageSize),
			Offset: int32((req.Page - 1) * req.PageSize),
		},
	})
	if err != nil {
		return nil, err
	}

	user, err := s.userHandler.GetUserInfo(c, &userBiz.UserQuery{
		UID: req.Uid,
	})
	if err != nil {
		return nil, err
	}
	if user == nil || user.UID == "" {
		return nil, errors.New(int(usercenter.ErrorCode_NOT_FOUND), "NOT_FOUND", "inviter not found")
	}

	for _, userEntity := range userEntities {
		rsp.Users = append(rsp.Users, &usercenter.GetInviteUserListReply_User{
			Uid:          userEntity.UID,
			Name:         userEntity.Name,
			Avatar:       userEntity.Avatar,
			Desc:         userEntity.Description,
			Email:        util.HideEmail(userEntity.Email),
			CreatedAt:    uint32(userEntity.CreatedAt.Unix()),
			InviteAt:     uint32(userEntity.InviteAt),
			RewardPoints: uint32(userEntity.ProvideInvitePoints),
		})
	}
	rsp.Total = uint32(total)
	rsp.TotalRewardPoints = uint32(user.EarnedInvitePoints)
	return rsp, nil
}

func (s *MarketService) GetTasks(ctx context.Context, req *usercenter.GetTasksRequest) (*usercenter.GetTasksResponse, error) {
	c := common.NewBaseCtx(ctx, s.log)

	taskEntities, total, err := s.taskHandler.GetTasksWithTotal(c, &taskBiz.TaskQuery{
		IsShow: uint8(req.IsShow),
	})
	if err != nil {
		return nil, err
	}
	if len(taskEntities) == 0 {
		return &usercenter.GetTasksResponse{
			Total: 0,
		}, nil
	}

	if req.Uid == "" {
		rspTasks := make([]*usercenter.GetTasksResponse_Task, 0)
		for _, taskEntity := range taskEntities {
			rspTask := &usercenter.GetTasksResponse_Task{
				TaskUuid:     taskEntity.UUID,
				TaskKey:      taskEntity.Key,
				Name:         taskEntity.Name,
				Desc:         taskEntity.Description,
				Type:         uint32(taskEntity.Type),
				PicUrl:       taskEntity.PicUrl,
				RewardPoints: uint32(taskEntity.Reward),
				JumpUrl:      taskEntity.JumpUrl,
				TxHash:       "",
				TxStatus:     0,
			}
			rspTasks = append(rspTasks, rspTask)
		}
		return &usercenter.GetTasksResponse{
			Total: uint32(total),
			Tasks: rspTasks,
		}, nil
	}

	userTaskEntities, err := s.taskHandler.GetUserTasksByTaskKeys(c, req.Uid, taskEntities)
	if err != nil {
		return nil, err
	}

	userTaskEntitiesMap := make(map[string]*taskBiz.UserTaskEntity)
	userTaskUuidList := make([]string, 0)
	for _, userTaskEntity := range userTaskEntities {
		userTaskEntitiesMap[userTaskEntity.TaskKey] = userTaskEntity
		userTaskUuidList = append(userTaskUuidList, userTaskEntity.UUID)
	}

	userMintPointsEntities, err := s.assetHandler.GetUserMintPoints(c, &assetBiz.UserMintPointsQuery{
		UserTaskUuids: userTaskUuidList,
	})
	if err != nil {
		return nil, err
	}

	userTaskUuidToUserMintPointsMap := make(map[string][]*assetBiz.UserMintPointsEntity)
	for _, userMintPointsEntity := range userMintPointsEntities {
		userTaskUuidToUserMintPointsMap[userMintPointsEntity.UserTaskUUID] = append(userTaskUuidToUserMintPointsMap[userMintPointsEntity.UserTaskUUID], userMintPointsEntity)
	}

	// 查询用户通过做任务得到的总积分
	taskRewardPoints, err := s.assetHandler.GetUserEarnedPoints(c, req.Uid, assetBiz.UserMintPointsSourceTaskClaim)
	if err != nil {
		return nil, err
	}

	// 将积分除以精度，得到真实的积分数量
	realPoints := taskRewardPoints.Shift(-int32(s.confCustom.AssetTokens.Points.Decimals))

	rspTasks := make([]*usercenter.GetTasksResponse_Task, 0)
	for _, taskEntity := range taskEntities {
		rspTask := &usercenter.GetTasksResponse_Task{
			TaskUuid:     taskEntity.UUID,
			TaskKey:      taskEntity.Key,
			Name:         taskEntity.Name,
			Desc:         taskEntity.Description,
			Type:         uint32(taskEntity.Type),
			PicUrl:       taskEntity.PicUrl,
			RewardPoints: uint32(taskEntity.Reward),
			JumpUrl:      taskEntity.JumpUrl,
			TxHash:       "",
			TxStatus:     0,
		}
		if userTaskEntity, ok := userTaskEntitiesMap[taskEntity.Key]; ok {
			rspTask.IsDone = true
			rspTask.IsClaimed = uint32(userTaskEntity.Claimed)
			rspTask.ClaimAt = uint32(userTaskEntity.ClaimedAt)

			if userMintPointsEntities, ok := userTaskUuidToUserMintPointsMap[userTaskEntity.UUID]; ok {
				// 按优先级选择记录：优先成功(1) > 发送中(3) > 失败(2)
				var record *assetBiz.UserMintPointsEntity
				for _, oneMintRecord := range userMintPointsEntities {
					if record == nil {
						record = oneMintRecord
						continue
					}

					if oneMintRecord.Status == assetBiz.UserMintPointsStatusSuccess {
						// 成功状态优先级最高
						record = oneMintRecord
						break
					} else if oneMintRecord.Status == assetBiz.UserMintPointsStatusPending && record.Status != assetBiz.UserMintPointsStatusSuccess {
						// 发送中优先于失败
						record = oneMintRecord
					}
				}

				if record != nil {
					rspTask.TxHash = record.TxHash
					rspTask.TxStatus = uint32(record.Status)
				}
			}
		}
		rspTasks = append(rspTasks, rspTask)
	}

	return &usercenter.GetTasksResponse{
		Total:             uint32(total),
		Tasks:             rspTasks,
		TotalRewardPoints: uint32(realPoints.IntPart()),
	}, nil
}

func (s *MarketService) ClaimTaskReward(ctx context.Context, req *usercenter.ClaimTaskRewardRequest) (*usercenter.ClaimTaskRewardResponse, error) {
	c := common.NewBaseCtx(ctx, s.log)

	msg, err := s.taskHandler.ClaimTaskReward(c, req.Uid, req.TaskKey)
	if err != nil {
		return nil, err
	}

	msgId, err := s.userHandler.ProduceClaimTaskRewardStreamMsg(c, msg)
	if err != nil {
		return nil, err
	}
	c.Log.Infof("ClaimTaskReward msgId: %s", msgId)

	return &usercenter.ClaimTaskRewardResponse{}, nil
}

func (s *MarketService) TaskDone(ctx context.Context, req *usercenter.TaskDoneRequest) (*usercenter.TaskDoneResponse, error) {
	c := common.NewBaseCtx(ctx, s.log)

	err := s.taskHandler.TaskDone(c, req.Uid, req.TaskKey)
	if err != nil {
		return nil, err
	}
	return &usercenter.TaskDoneResponse{}, nil
}
