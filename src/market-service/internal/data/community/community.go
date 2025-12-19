package community

import (
	"market-service/internal/biz/community"
	"market-service/internal/data/base"
	communityModel "market-service/internal/model/usercenter/community"
	"market-service/internal/pkg/common"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type communityRepo struct {
	base.UsercenterInfra
}

func NewCommunityRepo(infra base.UsercenterInfra) community.CommunityRepoInterface {
	return &communityRepo{
		infra,
	}
}

func (r *communityRepo) GetPost(ctx common.Ctx, query *community.PostQuery) (*community.PostEntity, error) {
	db := common.GetDB(ctx.Ctx, r.GetDb().WithContext(ctx.Ctx).Model(&communityModel.Post{}))
	db = query.Condition(db, nil)
	postModel := &communityModel.Post{}
	err := db.First(&postModel).Error
	if err != nil && err != gorm.ErrRecordNotFound {
		ctx.Log.Errorf("GetPost sql failed, err: %v", err)
		return nil, err
	}
	return postModel.ToEntity(), nil
}

func (r *communityRepo) GetPostsWithTotal(ctx common.Ctx, query *community.PostQuery) ([]*community.PostEntity, int64, error) {
	var total int64
	db := common.GetDB(ctx.Ctx, r.GetDb().WithContext(ctx.Ctx).Model(&communityModel.Post{}))
	db = query.Condition(db, &total)
	postModels := []communityModel.Post{}
	err := db.Find(&postModels).Error
	if err != nil {
		ctx.Log.Errorf("GetPostsWithTotal sql failed, err: %v", err)
		return nil, 0, err
	}
	postEntities := make([]*community.PostEntity, 0, len(postModels))
	for _, postModel := range postModels {
		postEntities = append(postEntities, postModel.ToEntity())
	}
	return postEntities, total, nil
}

func (r *communityRepo) GetComment(ctx common.Ctx, query *community.CommentQuery) (*community.CommentEntity, error) {
	db := common.GetDB(ctx.Ctx, r.GetDb().WithContext(ctx.Ctx).Model(&communityModel.Comment{}))
	db = query.Condition(db, nil)
	commentModel := &communityModel.Comment{}
	err := db.First(&commentModel).Error
	if err != nil && err != gorm.ErrRecordNotFound {
		ctx.Log.Errorf("GetComment sql failed, err: %v", err)
		return nil, err
	}
	return commentModel.ToEntity(), nil
}

func (r *communityRepo) GetCommentsWithTotal(ctx common.Ctx, query *community.CommentQuery) ([]*community.CommentEntity, int64, error) {
	var total int64
	db := common.GetDB(ctx.Ctx, r.GetDb().WithContext(ctx.Ctx).Model(&communityModel.Comment{}))
	db = query.Condition(db, &total)
	commentModels := []communityModel.Comment{}
	err := db.Find(&commentModels).Error
	if err != nil {
		ctx.Log.Errorf("GetCommentsWithTotal sql failed, err: %v", err)
		return nil, 0, err
	}
	commentEntities := make([]*community.CommentEntity, 0, len(commentModels))
	for _, commentModel := range commentModels {
		commentEntities = append(commentEntities, commentModel.ToEntity())
	}
	return commentEntities, total, nil
}

func (r *communityRepo) GetUserLike(ctx common.Ctx, query *community.UserLikeQuery) (*community.UserLikeEntity, error) {
	db := common.GetDB(ctx.Ctx, r.GetDb().WithContext(ctx.Ctx).Model(&communityModel.UserLike{}))
	db = query.Condition(db, nil)
	userLikeModel := &communityModel.UserLike{}
	err := db.First(&userLikeModel).Error
	if err != nil && err != gorm.ErrRecordNotFound {
		ctx.Log.Errorf("GetUserLike sql failed, err: %v", err)
		return nil, err
	}
	return userLikeModel.ToEntity(), nil
}

func (r *communityRepo) GetUserLikes(ctx common.Ctx, query *community.UserLikeQuery) ([]*community.UserLikeEntity, error) {
	db := common.GetDB(ctx.Ctx, r.GetDb().WithContext(ctx.Ctx).Model(&communityModel.UserLike{}))
	db = query.Condition(db, nil)
	userLikeModels := []*communityModel.UserLike{}
	err := db.Find(&userLikeModels).Error
	if err != nil {
		ctx.Log.Errorf("GetUserLikes sql failed, err: %v", err)
		return nil, err
	}
	userLikeEntities := make([]*community.UserLikeEntity, 0, len(userLikeModels))
	for _, userLikeModel := range userLikeModels {
		userLikeEntities = append(userLikeEntities, userLikeModel.ToEntity())
	}
	return userLikeEntities, nil
}

func (r *communityRepo) CreatePost(ctx common.Ctx, postEntity *community.PostEntity) error {
	postModel := &communityModel.Post{}
	postModel.FromEntity(postEntity)
	if err := r.Create(ctx, postModel); err != nil {
		ctx.Log.Errorf("CreatePost create post failed, err: %v", err)
		return err
	}
	return nil
}

func (r *communityRepo) CreateComment(ctx common.Ctx, commentEntity *community.CommentEntity) error {
	commentModel := &communityModel.Comment{}
	commentModel.FromEntity(commentEntity)
	if err := r.Create(ctx, commentModel); err != nil {
		ctx.Log.Errorf("CreateComment create comment failed, err: %v", err)
		return err
	}
	return nil
}

func (r *communityRepo) CreateOrUpdateUserLike(ctx common.Ctx, userLikeEntity *community.UserLikeEntity) error {
	userLikeModel := &communityModel.UserLike{}
	userLikeModel.FromEntity(userLikeEntity)
	db := common.GetDB(ctx.Ctx, r.GetDb())
	return db.Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: "uid"},
			{Name: "content_uuid"},
			{Name: "type"},
		},
		DoUpdates: clause.AssignmentColumns([]string{"status"}), // 要更新的字段
	}).Create(&userLikeModel).Error
}

func (r *communityRepo) IncrementPostLike(ctx common.Ctx, postEntity *community.PostEntity) error {
	db := common.GetDB(ctx.Ctx, r.GetDb())
	postModel := &communityModel.Post{}
	return db.Model(postModel).Where("uuid = ?", postEntity.UUID).Update("like_count", gorm.Expr(postModel.TableName()+".like_count + 1")).Error
}

func (r *communityRepo) DecrementPostLike(ctx common.Ctx, postEntity *community.PostEntity) error {
	db := common.GetDB(ctx.Ctx, r.GetDb())
	postModel := &communityModel.Post{}
	return db.Model(postModel).Where("uuid = ?", postEntity.UUID).Update("like_count", gorm.Expr(postModel.TableName()+".like_count - 1")).Error
}

func (r *communityRepo) IncrementCommentLike(ctx common.Ctx, commentEntity *community.CommentEntity) error {
	db := common.GetDB(ctx.Ctx, r.GetDb())
	commentModel := &communityModel.Comment{}
	return db.Model(commentModel).Where("uuid = ?", commentEntity.UUID).Update("like_count", gorm.Expr(commentModel.TableName()+".like_count + 1")).Error
}

func (r *communityRepo) DecrementCommentLike(ctx common.Ctx, commentEntity *community.CommentEntity) error {
	db := common.GetDB(ctx.Ctx, r.GetDb())
	commentModel := &communityModel.Comment{}
	return db.Model(commentModel).Where("uuid = ?", commentEntity.UUID).Update("like_count", gorm.Expr(commentModel.TableName()+".like_count - 1")).Error
}

func (r *communityRepo) IncrementPostView(ctx common.Ctx, postEntity *community.PostEntity) error {
	db := common.GetDB(ctx.Ctx, r.GetDb())
	postModel := &communityModel.Post{}
	return db.Model(postModel).Where("uuid = ?", postEntity.UUID).Update("view_count", gorm.Expr(postModel.TableName()+".view_count + 1")).Error
}

func (r *communityRepo) IncrementPostCommentCount(ctx common.Ctx, postEntity *community.PostEntity) error {
	db := common.GetDB(ctx.Ctx, r.GetDb())
	postModel := &communityModel.Post{}
	return db.Model(postModel).Where("uuid = ?", postEntity.UUID).Update("comment_count", gorm.Expr(postModel.TableName()+".comment_count + 1")).Error
}

func (r *communityRepo) IncrementUserPostCount(ctx common.Ctx, uid string) error {
	db := common.GetDB(ctx.Ctx, r.GetDb())
	userCommunityInfoModel := &communityModel.UserCommunityInfo{UID: uid, PostCount: 1}
	return db.Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: "uid"},
		},
		DoUpdates: clause.Assignments(map[string]interface{}{
			"post_count": gorm.Expr(userCommunityInfoModel.TableName() + ".post_count + 1"),
		}),
	}).Create(&userCommunityInfoModel).Error
}

func (r *communityRepo) GetUserCommunityInfo(ctx common.Ctx, query *community.UserCommunityInfoQuery) (*community.UserCommunityInfoEntity, error) {
	db := common.GetDB(ctx.Ctx, r.GetDb().WithContext(ctx.Ctx).Model(&communityModel.UserCommunityInfo{}))
	db = query.Condition(db, nil)
	userCommunityInfoModel := &communityModel.UserCommunityInfo{}
	err := db.First(userCommunityInfoModel).Error
	if err != nil && err != gorm.ErrRecordNotFound {
		return nil, err
	}
	return userCommunityInfoModel.ToEntity(), nil
}

func (r *communityRepo) GetTopOnePostForEachMarketAddress(ctx common.Ctx, marketAddressList []string) ([]*community.PostEntity, error) {
	db := common.GetDB(ctx.Ctx, r.GetDb().WithContext(ctx.Ctx).Model(&communityModel.Post{}))

	query := `
        WITH RankedPosts AS (
            SELECT 
                p.*,
                ROW_NUMBER() OVER (
                    PARTITION BY p.market_address 
                    ORDER BY p.like_count DESC, p.comment_count DESC, p.created_at DESC  
                ) as rank
            FROM t_post p
            WHERE p.market_address in (?)
        )
        SELECT 
           *
        FROM RankedPosts 
        WHERE rank = 1
    `

	postModels := []communityModel.Post{}
	err := db.Raw(query, marketAddressList).Scan(&postModels).Error
	if err != nil {
		ctx.Log.Errorf("GetTopOnePostForEachMarketAddress sql failed, err: %v", err)
		return nil, err
	}
	postEntities := make([]*community.PostEntity, 0, len(postModels))
	for _, postModel := range postModels {
		postEntities = append(postEntities, postModel.ToEntity())
	}
	return postEntities, nil
}

func (r *communityRepo) GetNewReplysForEachComment(ctx common.Ctx, commentUUIDList []string, limit int) ([]*community.CommentEntity, error) {
	db := common.GetDB(ctx.Ctx, r.GetDb().WithContext(ctx.Ctx).Model(&communityModel.Comment{}))

	query := `
		WITH RankedReplys AS (
			SELECT 
				c.*,
				ROW_NUMBER() OVER (
					PARTITION BY c.root_uuid
					ORDER BY c.id DESC
				) as rank
			FROM t_comment c
			WHERE c.root_uuid in (?)
		)
		SELECT * FROM RankedReplys WHERE rank <= ?
	`
	commentModels := make([]communityModel.Comment, 0)
	err := db.Raw(query, commentUUIDList, limit).Scan(&commentModels).Error
	if err != nil {
		ctx.Log.Errorf("GetNewReplysForEachComment sql failed, err: %v", err)
		return nil, err
	}
	commentEntities := make([]*community.CommentEntity, 0, len(commentModels))
	for _, commentModel := range commentModels {
		commentEntities = append(commentEntities, commentModel.ToEntity())
	}
	return commentEntities, nil
}

func (r *communityRepo) GetCommentCountsByRootUUIDs(ctx common.Ctx, rootUUIDList []string) (map[string]int64, error) {
	db := common.GetDB(ctx.Ctx, r.GetDb().WithContext(ctx.Ctx).Model(&communityModel.Comment{}))

	query := `
        SELECT 
            root_uuid,
            COUNT(*) as count
        FROM t_comment
        WHERE root_uuid in (?)
        GROUP BY root_uuid
    `

	type CountResult struct {
		RootUUID string `gorm:"column:root_uuid"`
		Count    int64  `gorm:"column:count"`
	}

	var results []CountResult
	err := db.Raw(query, rootUUIDList).Scan(&results).Error
	if err != nil {
		ctx.Log.Errorf("GetCommentCountsByRootUUIDs sql failed, err: %v", err)
		return nil, err
	}

	countMap := make(map[string]int64)
	for _, result := range results {
		countMap[result.RootUUID] = result.Count
	}

	return countMap, nil
}
