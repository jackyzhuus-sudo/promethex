package community

import (
	"fmt"
	"market-service/internal/biz/base"
	"market-service/internal/pkg/common"
	"market-service/internal/pkg/util"
	"time"

	"github.com/go-kratos/kratos/v2/errors"

	usercenterPb "market-proto/proto/market-service/usercenter/v1"

	"github.com/go-kratos/kratos/v2/log"
	"gorm.io/gorm"
)

var (
	ErrPostTitleTooLong      = errors.New(int(usercenterPb.ErrorCode_POST_TITLE_TOO_LONG), "POST_TITLE_TOO_LONG", "post title is too long")
	ErrPostContentTooLong    = errors.New(int(usercenterPb.ErrorCode_POST_CONTENT_TOO_LONG), "POST_CONTENT_TOO_LONG", "post content is too long")
	ErrCommentContentTooLong = errors.New(int(usercenterPb.ErrorCode_COMMENT_CONTENT_TOO_LONG), "COMMENT_CONTENT_TOO_LONG", "comment content is too long")
	ErrPostEmpty             = errors.New(int(usercenterPb.ErrorCode_EMPTY_POST), "POST_EMPTY", "post is empty")
	ErrCommentEmpty          = errors.New(int(usercenterPb.ErrorCode_EMPTY_COMMENT), "COMMENT_EMPTY", "comment is empty")
	ErrPostNotFound          = errors.New(int(usercenterPb.ErrorCode_POST_NOT_FOUND), "POST_NOT_FOUND", "post not found")
	ErrCommentNotFound       = errors.New(int(usercenterPb.ErrorCode_COMMENT_NOT_FOUND), "COMMENT_NOT_FOUND", "comment not found")
	ErrRepeatLikeOrUnlike    = errors.New(int(usercenterPb.ErrorCode_REPEAT_LIKE_OR_UNLIKE), "REPEAT_LIKE_OR_UNLIKE", "repeat like or unlike")
	ErrMustLikeFirst         = errors.New(int(usercenterPb.ErrorCode_MUST_LIKE_FIRST), "MUST_LIKE_FIRST", "must like first")
	ErrLikeTooQuick          = errors.New(int(usercenterPb.ErrorCode_LIKE_TOO_QUICK), "LIKE_TOO_QUICK", "like too quick")
)

type CommunityRepoInterface interface {
	base.RepoInterface

	GetUserCommunityInfo(ctx common.Ctx, query *UserCommunityInfoQuery) (*UserCommunityInfoEntity, error)

	CreatePost(ctx common.Ctx, postEntity *PostEntity) error
	CreateComment(ctx common.Ctx, commentEntity *CommentEntity) error
	CreateOrUpdateUserLike(ctx common.Ctx, userLikeEntity *UserLikeEntity) error

	GetTopOnePostForEachMarketAddress(ctx common.Ctx, marketAddressList []string) ([]*PostEntity, error)

	GetNewReplysForEachComment(ctx common.Ctx, commentUUIDList []string, limit int) ([]*CommentEntity, error)
	GetCommentCountsByRootUUIDs(ctx common.Ctx, rootUUIDList []string) (map[string]int64, error)

	GetPost(ctx common.Ctx, query *PostQuery) (*PostEntity, error)
	GetPostsWithTotal(ctx common.Ctx, query *PostQuery) ([]*PostEntity, int64, error)
	GetComment(ctx common.Ctx, query *CommentQuery) (*CommentEntity, error)
	GetCommentsWithTotal(ctx common.Ctx, query *CommentQuery) ([]*CommentEntity, int64, error)
	GetUserLike(ctx common.Ctx, query *UserLikeQuery) (*UserLikeEntity, error)
	GetUserLikes(ctx common.Ctx, query *UserLikeQuery) ([]*UserLikeEntity, error)
	IncrementPostLike(ctx common.Ctx, postEntity *PostEntity) error
	DecrementPostLike(ctx common.Ctx, postEntity *PostEntity) error

	IncrementCommentLike(ctx common.Ctx, commentEntity *CommentEntity) error
	DecrementCommentLike(ctx common.Ctx, commentEntity *CommentEntity) error

	IncrementPostCommentCount(ctx common.Ctx, postEntity *PostEntity) error
	IncrementUserPostCount(ctx common.Ctx, uid string) error
	IncrementPostView(ctx common.Ctx, postEntity *PostEntity) error
}

type CommunityHandler struct {
	communityRepo CommunityRepoInterface
	log           *log.Helper
}

func NewCommunityHandler(communityRepo CommunityRepoInterface, logger log.Logger) *CommunityHandler {
	return &CommunityHandler{
		communityRepo: communityRepo,
		log:           log.NewHelper(logger),
	}
}

func (h *CommunityHandler) PublishPost(ctx common.Ctx, postEntity *PostEntity) (string, error) {
	if len(postEntity.Content) == 0 || len(postEntity.Title) == 0 {
		return "", ErrPostEmpty
	}
	// 标题限制2000字节 大约1000字
	if len(postEntity.Title) > 2000 {
		return "", ErrPostTitleTooLong
	}
	// 内容限制20000字节 大约10000字
	if len(postEntity.Content) > 20000 {
		return "", ErrPostContentTooLong
	}

	postEntity.UUID = util.GenerateUUID()
	err := h.communityRepo.ExecTx(ctx, func(ctx common.Ctx, db *gorm.DB) error {
		if err := h.communityRepo.CreatePost(ctx, postEntity); err != nil {
			ctx.Log.Errorf("CreatePost error: %+v", err)
			return err
		}
		if err := h.communityRepo.IncrementUserPostCount(ctx, postEntity.UID); err != nil {
			ctx.Log.Errorf("IncrementUserPostCount error: %+v", err)
			return err
		}
		return nil
	})

	if err != nil {
		return "", errors.New(int(usercenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}

	return postEntity.UUID, nil

}

func (h *CommunityHandler) PublishComment(ctx common.Ctx, commentEntity *CommentEntity) (string, string, string, error) {
	if commentEntity.UID == "" {
		return "", "", "", errors.New(int(usercenterPb.ErrorCode_PARAM), "PARAM_ERROR", "uid is empty")
	}

	if commentEntity.PostUUID == "" {
		return "", "", "", errors.New(int(usercenterPb.ErrorCode_PARAM), "PARAM_ERROR", "post_uuid is empty")
	}

	if (commentEntity.RootUUID == "" && commentEntity.ParentUUID != "") || (commentEntity.RootUUID != "" && commentEntity.ParentUUID == "") {
		return "", "", "", errors.New(int(usercenterPb.ErrorCode_PARAM), "PARAM_ERROR", "root_uuid and parent_uuid must be set together")
	}

	if len(commentEntity.Content) == 0 {
		return "", "", "", ErrCommentEmpty
	}
	// 内容限制10000字节 大约5000字
	if len(commentEntity.Content) > 10000 {
		return "", "", "", ErrCommentContentTooLong
	}

	post, err := h.communityRepo.GetPost(ctx, &PostQuery{
		UUID:   commentEntity.PostUUID,
		Status: PostStatusNormal,
	})
	if err != nil {
		ctx.Log.Errorf("GetPost error: %+v", err)
		return "", "", "", errors.New(int(usercenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}

	if post == nil || post.UUID == "" {
		return "", "", "", ErrPostNotFound
	}
	commentEntity.MarketAddress = post.MarketAddress

	if commentEntity.RootUUID != "" {
		rootComment, err := h.communityRepo.GetComment(ctx, &CommentQuery{
			UUID:   commentEntity.RootUUID,
			Status: CommentStatusNormal,
		})
		if err != nil {
			ctx.Log.Errorf("GetComment error: %+v", err)
			return "", "", "", errors.New(int(usercenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
		}

		if rootComment == nil || rootComment.UUID == "" {
			return "", "", "", ErrCommentNotFound
		}
	}
	if commentEntity.ParentUUID != "" {
		parentComment, err := h.communityRepo.GetComment(ctx, &CommentQuery{
			UUID:   commentEntity.ParentUUID,
			Status: CommentStatusNormal,
		})
		if err != nil {
			ctx.Log.Errorf("GetComment error: %+v", err)
			return "", "", "", errors.New(int(usercenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
		}

		if parentComment == nil || parentComment.UUID == "" {
			return "", "", "", ErrCommentNotFound
		}
		commentEntity.ParentUserUID = parentComment.UID
	}

	err = h.communityRepo.ExecTx(ctx, func(ctx common.Ctx, db *gorm.DB) error {
		commentEntity.UUID = util.GenerateUUID()
		if err := h.communityRepo.CreateComment(ctx, commentEntity); err != nil {
			ctx.Log.Errorf("CreateComment error: %+v", err)
			return err
		}

		// 更新帖子评论数
		if err := h.communityRepo.IncrementPostCommentCount(ctx, post); err != nil {
			ctx.Log.Errorf("IncrementPostCommentCount error: %+v", err)
			return err
		}
		return nil
	})
	if err != nil {
		return "", "", "", errors.New(int(usercenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}

	notifyUid := ""
	if commentEntity.ParentUUID == "" && commentEntity.RootUUID == "" {
		notifyUid = post.UID
	} else {
		notifyUid = commentEntity.ParentUserUID
	}
	return commentEntity.UUID, notifyUid, post.MarketAddress, nil
}

// 点赞 / 取消点赞
func (h *CommunityHandler) UpdateLikeContentStatus(ctx common.Ctx, userLikeEntity *UserLikeEntity) (string, error) {
	if userLikeEntity.UID == "" {
		return "", errors.New(int(usercenterPb.ErrorCode_PARAM), "PARAM_ERROR", "uid is empty")
	}

	if userLikeEntity.ContentUUID == "" {
		return "", errors.New(int(usercenterPb.ErrorCode_PARAM), "PARAM_ERROR", "content_uuid is empty")
	}

	if userLikeEntity.Status != UserLikeStatusNormal && userLikeEntity.Status != UserLikeStatusDeleted {
		return "", errors.New(int(usercenterPb.ErrorCode_PARAM), "PARAM_ERROR", "status is invalid")
	}

	// redis lock
	var err error
	lockKey := fmt.Sprintf("user-like-lock-%s-%s-%d", userLikeEntity.UID, userLikeEntity.ContentUUID, userLikeEntity.Type)
	lockID, ok, err := h.communityRepo.AcquireLock(ctx, lockKey, 5*time.Second)
	if err != nil {
		return "", errors.New(int(usercenterPb.ErrorCode_REDIS), "REDIS_ERROR", err.Error())
	}
	if !ok {
		return "", ErrLikeTooQuick
	}
	defer h.communityRepo.ReleaseLock(ctx, lockKey, lockID)

	var post *PostEntity
	var comment *CommentEntity
	var notifyUid string
	if userLikeEntity.Type == UserLikeTypePost {
		post, err = h.communityRepo.GetPost(ctx, &PostQuery{
			UUID:   userLikeEntity.ContentUUID,
			Status: PostStatusNormal,
		})
		if err != nil {
			ctx.Log.Errorf("GetPost error: %+v", err)
			return "", errors.New(int(usercenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
		}

		if post == nil || post.UUID == "" {
			return "", ErrPostNotFound
		}

		notifyUid = post.UID
	} else if userLikeEntity.Type == UserLikeTypeComment {
		comment, err = h.communityRepo.GetComment(ctx, &CommentQuery{
			UUID:   userLikeEntity.ContentUUID,
			Status: CommentStatusNormal,
		})
		if err != nil {
			ctx.Log.Errorf("GetComment error: %+v", err)
			return "", errors.New(int(usercenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
		}

		if comment == nil || comment.UUID == "" {
			return "", ErrCommentNotFound
		}

		notifyUid = comment.UID
	} else {
		return "", errors.New(int(usercenterPb.ErrorCode_PARAM), "PARAM_ERROR", "type is invalid")
	}

	userLike, err := h.communityRepo.GetUserLike(ctx, &UserLikeQuery{
		UID:         userLikeEntity.UID,
		ContentUUID: userLikeEntity.ContentUUID,
		Type:        userLikeEntity.Type,
	})
	if err != nil {
		ctx.Log.Errorf("GetUserLike error: %+v", err)
		return "", errors.New(int(usercenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}

	if userLikeEntity.Status == UserLikeStatusDeleted {
		if userLike != nil && userLike.UID != "" {
			if userLike.Status == UserLikeStatusDeleted {
				return "", ErrRepeatLikeOrUnlike
			}
		} else {
			// 未点赞过 不能取消点赞
			return "", ErrMustLikeFirst
		}
	} else {
		if userLike != nil && userLike.UID != "" {
			if userLike.Status == UserLikeStatusNormal {
				return "", ErrRepeatLikeOrUnlike
			}
		}
	}

	// 开启事务
	err = h.communityRepo.ExecTx(ctx, func(ctx common.Ctx, db *gorm.DB) error {
		// 不存在 创建或修改点赞记录
		if err := h.communityRepo.CreateOrUpdateUserLike(ctx, userLikeEntity); err != nil {
			ctx.Log.Errorf("CreateOrUpdateUserLike error: %+v", err)
			return err
		}
		var err error
		// 点赞数更新
		if userLikeEntity.Status == UserLikeStatusNormal {
			if userLikeEntity.Type == UserLikeTypePost {
				err = h.communityRepo.IncrementPostLike(ctx, post)
			} else {
				err = h.communityRepo.IncrementCommentLike(ctx, comment)
			}
		} else {
			if userLikeEntity.Type == UserLikeTypePost {
				err = h.communityRepo.DecrementPostLike(ctx, post)
			} else {
				err = h.communityRepo.DecrementCommentLike(ctx, comment)
			}
		}
		if err != nil {
			ctx.Log.Errorf("UpdateUserLikeStatus total error: %+v", err)
			return err
		}
		return nil
	})

	if err != nil {
		return "", errors.New(int(usercenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}
	return notifyUid, nil
}

func (h *CommunityHandler) GetPostsWithTotal(ctx common.Ctx, query *PostQuery) ([]*PostEntity, int64, error) {
	posts, total, err := h.communityRepo.GetPostsWithTotal(ctx, query)
	if err != nil {
		ctx.Log.Errorf("GetPostsWithTotal error: %+v", err)
		return nil, 0, errors.New(int(usercenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}
	return posts, total, nil
}

func (h *CommunityHandler) GetCommentsWithTotal(ctx common.Ctx, query *CommentQuery) ([]*CommentEntity, int64, error) {
	comments, total, err := h.communityRepo.GetCommentsWithTotal(ctx, query)
	if err != nil {
		ctx.Log.Errorf("GetCommentsWithTotal error: %+v", err)
		return nil, 0, errors.New(int(usercenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}
	return comments, total, nil
}

func (h *CommunityHandler) GetUserLikes(ctx common.Ctx, query *UserLikeQuery) ([]*UserLikeEntity, error) {
	userLikes, err := h.communityRepo.GetUserLikes(ctx, query)
	if err != nil {
		ctx.Log.Errorf("GetUserLikes error: %+v", err)
		return nil, errors.New(int(usercenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}
	return userLikes, nil
}

func (h *CommunityHandler) GetUserCommunityInfo(ctx common.Ctx, query *UserCommunityInfoQuery) (*UserCommunityInfoEntity, error) {
	userCommunityInfo, err := h.communityRepo.GetUserCommunityInfo(ctx, query)
	if err != nil {
		ctx.Log.Errorf("GetUserCommunityInfo error: %+v", err)
		return nil, errors.New(int(usercenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}
	return userCommunityInfo, nil
}

func (h *CommunityHandler) GetTopOnePostForEachMarketAddress(ctx common.Ctx, marketAddressList []string, uid string) ([]*PostEntity, error) {
	if len(marketAddressList) == 0 {
		return nil, nil
	}

	posts, err := h.communityRepo.GetTopOnePostForEachMarketAddress(ctx, marketAddressList)
	if err != nil {
		ctx.Log.Errorf("GetTopOnePostForEachMarketAddress error: %+v", err)
		return nil, errors.New(int(usercenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}
	if len(posts) == 0 {
		return nil, nil
	}

	userLikeMap := make(map[string]bool)
	if uid != "" {
		postsUuids := make([]string, 0, len(posts))
		for _, post := range posts {
			postsUuids = append(postsUuids, post.UUID)
		}

		userLikes, err := h.communityRepo.GetUserLikes(ctx, &UserLikeQuery{
			UID:             uid,
			ContentUUIDList: postsUuids,
			Type:            UserLikeTypePost,
			Status:          UserLikeStatusNormal,
		})
		if err != nil {
			ctx.Log.Errorf("GetUserLikes error: %+v", err)
			return nil, errors.New(int(usercenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
		}
		for _, userLike := range userLikes {
			userLikeMap[userLike.ContentUUID] = true
		}
	}

	for _, post := range posts {
		if _, ok := userLikeMap[post.UUID]; ok {
			post.IsLike = UserLikeStatusNormal
		}
	}

	return posts, nil
}

func (h *CommunityHandler) GetCommentCountsByRootUUIDs(ctx common.Ctx, commentUUIDList []string) (map[string]int64, error) {
	commentCounts, err := h.communityRepo.GetCommentCountsByRootUUIDs(ctx, commentUUIDList)
	if err != nil {
		ctx.Log.Errorf("GetCommentCountsByRootUUIDs error: %+v", err)
		return nil, errors.New(int(usercenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}
	return commentCounts, nil
}

func (h *CommunityHandler) GetNewReplysForEachComment(ctx common.Ctx, commentUUIDList []string, limit int, uid string) ([]*CommentEntity, error) {
	if len(commentUUIDList) == 0 {
		return nil, nil
	}

	replys, err := h.communityRepo.GetNewReplysForEachComment(ctx, commentUUIDList, limit)
	if err != nil {
		ctx.Log.Errorf("GetNewReplysForEachComment error: %+v", err)
		return nil, errors.New(int(usercenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}

	if len(replys) == 0 {
		return nil, nil
	}

	userLikeMap := make(map[string]bool)

	if uid != "" {
		replysUuids := make([]string, 0, len(replys))
		for _, reply := range replys {
			replysUuids = append(replysUuids, reply.UUID)
		}

		userLikes, err := h.communityRepo.GetUserLikes(ctx, &UserLikeQuery{
			UID:             uid,
			ContentUUIDList: replysUuids,
			Type:            UserLikeTypeComment,
			Status:          UserLikeStatusNormal,
		})
		if err != nil {
			ctx.Log.Errorf("GetUserLikes error: %+v", err)
			return nil, errors.New(int(usercenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
		}
		for _, userLike := range userLikes {
			userLikeMap[userLike.ContentUUID] = true
		}
	}

	for _, reply := range replys {
		if _, ok := userLikeMap[reply.UUID]; ok {
			reply.IsLike = UserLikeStatusNormal
		}
	}

	return replys, nil
}

func (h *CommunityHandler) GetComment(ctx common.Ctx, query *CommentQuery) (*CommentEntity, error) {
	comment, err := h.communityRepo.GetComment(ctx, query)
	if err != nil {
		ctx.Log.Errorf("GetComment error: %+v", err)
		return nil, errors.New(int(usercenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}
	return comment, nil
}

func (h *CommunityHandler) GetPost(ctx common.Ctx, query *PostQuery) (*PostEntity, error) {
	post, err := h.communityRepo.GetPost(ctx, query)
	if err != nil {
		ctx.Log.Errorf("GetPost error: %+v", err)
		return nil, errors.New(int(usercenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}
	return post, nil
}
