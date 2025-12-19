package bayes_http

import (
	"context"
	"market-backend/internal/pkg"
	"market-backend/internal/pkg/util"
	bayespb "market-proto/proto/market-backend/v1"
	marketcenterpb "market-proto/proto/market-service/marketcenter/v1"
	usercenterpb "market-proto/proto/market-service/usercenter/v1"
)

func (s *BayesHttpService) PublishPost(ctx context.Context, req *bayespb.PublishPostRequest) (*bayespb.PublishPostReply, error) {
	c := util.NewBaseCtx(ctx, s.logger)
	uid := util.GetUidFromCtx(ctx)
	if uid == "" {
		return nil, pkg.ErrParam
	}

	PublishPostRsp, err := s.data.RpcClient.UsercenterClient.PublishPost(ctx, &usercenterpb.PublishPostRequest{
		Uid:           uid,
		MarketAddress: req.MarketAddress,
		Title:         req.Title,
		Content:       req.Content,
	})
	if err != nil {
		c.Log.Errorf("rpc PublishPost failed, err: [%+v]", err)
		return nil, err
	}

	return &bayespb.PublishPostReply{PostUuid: PublishPostRsp.PostUuid}, nil
}

func (s *BayesHttpService) PublishComment(ctx context.Context, req *bayespb.PublishCommentRequest) (*bayespb.PublishCommentReply, error) {
	c := util.NewBaseCtx(ctx, s.logger)
	uid := util.GetUidFromCtx(ctx)
	if uid == "" {
		return nil, pkg.ErrParam
	}

	PublishCommentRsp, err := s.data.RpcClient.UsercenterClient.PublishComment(ctx, &usercenterpb.PublishCommentRequest{
		Uid:        uid,
		PostUuid:   req.PostUuid,
		Content:    req.Content,
		RootUuid:   req.RootUuid,
		ParentUuid: req.ParentUuid,
	})
	if err != nil {
		c.Log.Errorf("rpc PublishComment failed, err: [%+v]", err)
		return nil, err
	}

	return &bayespb.PublishCommentReply{CommentUuid: PublishCommentRsp.CommentUuid}, nil
}

func (s *BayesHttpService) LikeContent(ctx context.Context, req *bayespb.LikeContentRequest) (*bayespb.LikeContentReply, error) {
	c := util.NewBaseCtx(ctx, s.logger)
	uid := util.GetUidFromCtx(ctx)
	if uid == "" {
		return nil, pkg.ErrParam
	}

	_, err := s.data.RpcClient.UsercenterClient.UpdateLikeContentStatus(ctx, &usercenterpb.UpdateLikeContentStatusRequest{
		Uid:         uid,
		ContentUuid: req.ContentUuid,
		ContentType: usercenterpb.UpdateLikeContentStatusRequest_ContentType(req.ContentType),
		Status:      usercenterpb.UpdateLikeContentStatusRequest_STATUS_LIKE,
	})
	if err != nil {
		c.Log.Errorf("rpc LikeContent failed, err: [%+v]", err)
		return nil, err
	}

	return &bayespb.LikeContentReply{}, nil
}

func (s *BayesHttpService) UnlikeContent(ctx context.Context, req *bayespb.UnlikeContentRequest) (*bayespb.UnlikeContentReply, error) {
	c := util.NewBaseCtx(ctx, s.logger)
	uid := util.GetUidFromCtx(ctx)
	if uid == "" {
		return nil, pkg.ErrParam
	}

	_, err := s.data.RpcClient.UsercenterClient.UpdateLikeContentStatus(ctx, &usercenterpb.UpdateLikeContentStatusRequest{
		Uid:         uid,
		ContentUuid: req.ContentUuid,
		ContentType: usercenterpb.UpdateLikeContentStatusRequest_ContentType(req.ContentType),
		Status:      usercenterpb.UpdateLikeContentStatusRequest_STATUS_UNLIKE,
	})
	if err != nil {
		c.Log.Errorf("rpc UnlikeContent failed, err: [%+v]", err)
		return nil, err
	}

	return &bayespb.UnlikeContentReply{}, nil
}

func (s *BayesHttpService) GetMarketPosts(ctx context.Context, req *bayespb.GetMarketPostsRequest) (*bayespb.GetMarketPostsReply, error) {
	c := util.NewBaseCtx(ctx, s.logger)
	GetMarketPostsAndPublishersRsp, err := s.data.RpcClient.UsercenterClient.GetMarketPostsAndPublishers(ctx, &usercenterpb.GetMarketPostsAndPublishersRequest{
		Uid:           util.GetUidFromCtx(ctx),
		MarketAddress: req.MarketAddress,
		Page:          int64(req.Page),
		PageSize:      int64(req.PageSize),
		LastId:        int32(req.LastId),
	})
	if err != nil {
		c.Log.Errorf("rpc GetMarketPosts failed, err: [%+v]", err)
		return nil, err
	}
	if len(GetMarketPostsAndPublishersRsp.Posts) == 0 {
		return &bayespb.GetMarketPostsReply{}, nil
	}

	uidList := make([]string, 0)
	for _, post := range GetMarketPostsAndPublishersRsp.Posts {
		uidList = append(uidList, post.Uid)
	}

	GetMarketUsersPositionsRsp, err := s.data.RpcClient.MarketcenterClient.GetMarketUsersPositions(ctx, &marketcenterpb.GetMarketUsersPositionsRequest{
		MarketAddress: req.MarketAddress,
		Uids:          uidList,
	})
	if err != nil {
		c.Log.Errorf("rpc GetMarketUsersPositions failed, err: [%+v]", err)
		return nil, err
	}

	uidToUserTokenBalancesMap := make(map[string][]*marketcenterpb.GetMarketUsersPositionsResponse_UserPosition_Position)
	for _, userPositions := range GetMarketUsersPositionsRsp.UserPositions {
		uidToUserTokenBalancesMap[userPositions.Uid] = userPositions.Positions
	}

	rsp := &bayespb.GetMarketPostsReply{Total: uint32(GetMarketPostsAndPublishersRsp.Total), Posts: make([]*bayespb.GetMarketPostsReply_Post, 0)}

	for _, post := range GetMarketPostsAndPublishersRsp.Posts {
		onePost := &bayespb.GetMarketPostsReply_Post{
			Uuid:          post.Uuid,
			Uid:           post.Uid,
			UserName:      post.UserName,
			UserAvatarUrl: post.UserAvatarUrl,
			Title:         post.Title,
			Content:       post.Content,
			LikeCount:     uint32(post.LikeCount),
			CommentCount:  uint32(post.CommentCount),
			Timestamp:     uint64(post.CreatedAt),
			IsLike:        uint32(post.IsLike),
			Id:            int64(post.Id),
		}
		if userTokenBalances, ok := uidToUserTokenBalancesMap[post.Uid]; ok {
			onePost.Positions = make([]*bayespb.GetMarketPostsReply_Post_Position, 0)
			for _, userTokenBalance := range userTokenBalances {
				onePost.Positions = append(onePost.Positions, &bayespb.GetMarketPostsReply_Post_Position{
					TokenAddress:     userTokenBalance.TokenAddress,
					TokenName:        userTokenBalance.TokenName,
					TokenPicUrl:      userTokenBalance.TokenPicUrl,
					Balance:          userTokenBalance.Balance,
					Decimal:          userTokenBalance.Decimal,
					TokenDescription: userTokenBalance.TokenDescription,
				})
			}
		}
		rsp.Posts = append(rsp.Posts, onePost)
	}
	return rsp, nil
}

func (s *BayesHttpService) GetComments(ctx context.Context, req *bayespb.GetCommentsRequest) (*bayespb.GetCommentsReply, error) {
	c := util.NewBaseCtx(ctx, s.logger)
	// 获取评论 用户信息
	GetCommentsRsp, err := s.data.RpcClient.UsercenterClient.GetComments(ctx, &usercenterpb.GetCommentsRequest{
		Uid:      util.GetUidFromCtx(ctx),
		PostUuid: req.PostUuid,
		RootUuid: req.RootUuid,
		Page:     int64(req.Page),
		PageSize: int64(req.PageSize),
		LastId:   req.LastId,
	})
	if err != nil {
		c.Log.Errorf("rpc GetComments failed, err: [%+v]", err)
		return nil, err
	}

	if len(GetCommentsRsp.Comments) == 0 {
		return &bayespb.GetCommentsReply{}, nil
	}

	rootUuids := make([]string, 0)
	for _, comment := range GetCommentsRsp.Comments {
		rootUuids = append(rootUuids, comment.Uuid)
	}

	commentUuidToReplysMap := make(map[string][]*usercenterpb.BatchGetCommentReplysAndUsersResponse_CommentAndReplys)
	// 如果是查看评论区 还要查下根评论下的前几个回复
	if req.RootUuid == "" {
		BatchGetCommentReplysAndUsersReply, err := s.data.RpcClient.UsercenterClient.BatchGetCommentReplysAndUsers(ctx, &usercenterpb.BatchGetCommentReplysAndUsersRequest{
			Uid:           util.GetUidFromCtx(ctx),
			RootUuids:     rootUuids,
			ReplyPage:     1,
			ReplyPageSize: int64(req.ReplyPageSize),
		})
		if err != nil {
			c.Log.Errorf("rpc BatchGetCommentReplysAndUsers failed, err: [%+v]", err)
			return nil, err
		}
		for _, commentAndReplys := range BatchGetCommentReplysAndUsersReply.CommentAndReplys {
			if commentUuidToReplysMap[commentAndReplys.RootUuid] == nil {
				commentUuidToReplysMap[commentAndReplys.RootUuid] = make([]*usercenterpb.BatchGetCommentReplysAndUsersResponse_CommentAndReplys, 0)
			}
			commentUuidToReplysMap[commentAndReplys.RootUuid] = append(commentUuidToReplysMap[commentAndReplys.RootUuid], commentAndReplys)
		}
	}

	marketAddress := GetCommentsRsp.Comments[0].MarketAddress
	// 获取用户持仓
	uidList := make([]string, 0)
	for _, comment := range GetCommentsRsp.Comments {
		uidList = append(uidList, comment.Uid)
	}
	uidList = util.RemoveDuplicate(uidList)
	GetMarketUsersPositionsRsp, err := s.data.RpcClient.MarketcenterClient.GetMarketUsersPositions(ctx, &marketcenterpb.GetMarketUsersPositionsRequest{
		MarketAddress: marketAddress,
		Uids:          uidList,
	})
	if err != nil {
		c.Log.Errorf("rpc GetMarketUsersPositions failed, err: [%+v]", err)
		return nil, err
	}

	uidToUserTokenBalancesMap := make(map[string][]*marketcenterpb.GetMarketUsersPositionsResponse_UserPosition_Position)
	for _, userPositions := range GetMarketUsersPositionsRsp.UserPositions {
		uidToUserTokenBalancesMap[userPositions.Uid] = userPositions.Positions
	}

	rspComments := make([]*bayespb.GetCommentsReply_Comment, 0)
	for _, comment := range GetCommentsRsp.Comments {
		oneComment := &bayespb.GetCommentsReply_Comment{
			Uid:                 comment.Uid,
			UserName:            comment.UserName,
			UserAvatarUrl:       comment.UserAvatarUrl,
			ParentUuid:          comment.ParentUuid,
			RootUuid:            comment.RootUuid,
			Content:             comment.Content,
			LikeCount:           int64(comment.LikeCount),
			Timestamp:           comment.CreatedAt,
			IsLike:              bayespb.GetCommentsReply_IsLike(comment.IsLike),
			ParentUserName:      comment.ParentUserName,
			ParentUserAvatarUrl: comment.ParentUserAvatarUrl,
			Replies:             make([]*bayespb.GetCommentsReply_Comment_Reply, 0),
			Id:                  int64(comment.Id),
			Uuid:                comment.Uuid,
		}

		if userTokenBalances, ok := uidToUserTokenBalancesMap[comment.Uid]; ok {
			oneComment.Positions = make([]*bayespb.GetCommentsReply_Comment_Position, 0)
			for _, userTokenBalance := range userTokenBalances {
				oneComment.Positions = append(oneComment.Positions, &bayespb.GetCommentsReply_Comment_Position{
					TokenAddress:     userTokenBalance.TokenAddress,
					TokenName:        userTokenBalance.TokenName,
					TokenPicUrl:      userTokenBalance.TokenPicUrl,
					Balance:          userTokenBalance.Balance,
					Decimal:          userTokenBalance.Decimal,
					MarketAddress:    marketAddress,
					TokenDescription: userTokenBalance.TokenDescription,
				})
			}
		}

		if commentAndReplys, ok := commentUuidToReplysMap[comment.Uuid]; ok {

			for _, oneCommmentAndReply := range commentAndReplys {
				oneComment.RepliesTotalCount = oneCommmentAndReply.Total
				for _, reply := range oneCommmentAndReply.Replies {
					oneComment.Replies = append(oneComment.Replies, &bayespb.GetCommentsReply_Comment_Reply{
						Uid:                 reply.Uid,
						UserName:            reply.UserName,
						UserAvatarUrl:       reply.UserAvatarUrl,
						Content:             reply.Content,
						LikeCount:           reply.LikeCount,
						IsLike:              bayespb.GetCommentsReply_IsLike(reply.IsLike),
						ParentUserName:      reply.ParentUserName,
						ParentUserAvatarUrl: reply.ParentUserAvatarUrl,
						CreatedAt:           reply.CreatedAt,
						MarketAddress:       reply.MarketAddress,
						RootUuid:            reply.RootUuid,
						ParentUuid:          reply.ParentUuid,
						Uuid:                reply.Uuid,
						Id:                  reply.Id,
					})
				}
			}
		}
		rspComments = append(rspComments, oneComment)
	}
	return &bayespb.GetCommentsReply{Total: int32(GetCommentsRsp.Total), Comments: rspComments}, nil
}
