package user

import (
	userBiz "market-service/internal/biz/user"
	userModel "market-service/internal/model/usercenter/user"
	"market-service/internal/pkg/common"

	base "market-service/internal/data/base"

	"encoding/json"
	"fmt"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type userRepo struct {
	base.UsercenterInfra
}

// NewUserRepo .
func NewUserRepo(infra base.UsercenterInfra) userBiz.UserRepoInterface {
	return &userRepo{
		infra,
	}
}

func (r *userRepo) GetUser(ctx common.Ctx, query *userBiz.UserQuery) (*userBiz.UserEntity, error) {
	var user *userModel.User
	db := query.Condition(r.GetDb().WithContext(ctx.Ctx).Model(&userModel.User{}), nil)
	if err := db.First(&user).Error; err != nil && err != gorm.ErrRecordNotFound {
		ctx.Log.Errorf("GetUser sql failed, err: %v", err)
		return nil, err
	}
	return user.ToEntity(), nil
}

func (r *userRepo) GetUsers(ctx common.Ctx, query *userBiz.UserQuery) ([]*userBiz.UserEntity, error) {
	db := query.Condition(r.GetDb().WithContext(ctx.Ctx).Model(&userModel.User{}), nil)
	var users []*userModel.User
	if err := db.Find(&users).Error; err != nil {
		ctx.Log.Errorf("GetUsers sql failed, err: %v", err)
		return nil, err
	}
	ctx.Log.Infof("len(users): [%+v] users: [%+v]", len(users), users)
	userEntities := make([]*userBiz.UserEntity, 0, len(users))
	for _, user := range users {
		userEntities = append(userEntities, user.ToEntity())
	}
	return userEntities, nil
}

func (r *userRepo) GetUsersWithTotal(ctx common.Ctx, query *userBiz.UserQuery) ([]*userBiz.UserEntity, int64, error) {
	var total int64
	db := query.Condition(r.GetDb().WithContext(ctx.Ctx).Model(&userModel.User{}), &total)
	var users []*userModel.User
	if err := db.Find(&users).Error; err != nil {
		ctx.Log.Errorf("GetUsersWithTotal sql failed, err: %v", err)
		return nil, 0, err
	}
	userEntities := make([]*userBiz.UserEntity, 0, len(users))
	for _, user := range users {
		userEntities = append(userEntities, user.ToEntity())
	}
	return userEntities, total, nil
}

func (r *userRepo) CreateUser(ctx common.Ctx, userEntity *userBiz.UserEntity) error {
	user := &userModel.User{}
	user.FromEntity(userEntity)
	return r.Infra.Create(ctx, user)
}

func (r *userRepo) UpdateUser(ctx common.Ctx, userEntity *userBiz.UserEntity) error {
	user := &userModel.User{
		Name:                userEntity.Name,
		Description:         userEntity.Description,
		Avatar:              userEntity.Avatar,
		InviterUID:          userEntity.InviterUID,
		InviteAt:            userEntity.InviteAt,
		ProvideInvitePoints: userEntity.ProvideInvitePoints,
	}
	err := common.GetDB(ctx.Ctx, r.GetDb().WithContext(ctx.Ctx)).Model(&userModel.User{}).Where("uid = ?", userEntity.UID).Updates(user).Error
	if err != nil {
		ctx.Log.Errorf("UpdateUser sql failed, err: %v", err)
		return err
	}
	return nil
}

func (r *userRepo) IncrUserEarnedInvitePoints(ctx common.Ctx, uid string, points int64) error {
	db := common.GetDB(ctx.Ctx, r.GetDb())
	userModel := &userModel.User{}
	return db.Model(userModel).Where("uid = ?", uid).Update("earned_invite_points", gorm.Expr(userModel.TableName()+".earned_invite_points + ?", points)).Error
}

func (r *userRepo) GetUserFollow(ctx common.Ctx, query *userBiz.UserFollowQuery) (*userBiz.UserFollowEntity, error) {
	var userFollow *userModel.UserFollow
	db := query.Condition(r.GetDb().WithContext(ctx.Ctx).Model(&userModel.UserFollow{}), nil)
	if err := db.First(&userFollow).Error; err != nil && err != gorm.ErrRecordNotFound {
		ctx.Log.Errorf("GetUserFollow sql failed, err: %v", err)
		return nil, err
	}
	return userFollow.ToEntity(), nil
}

func (r *userRepo) CreateOrUpdateUserFollow(ctx common.Ctx, userFollowEntity *userBiz.UserFollowEntity) error {
	userFollow := &userModel.UserFollow{}
	userFollow.FromEntity(userFollowEntity)
	db := common.GetDB(ctx.Ctx, r.GetDb())
	return db.Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: "uid"},
			{Name: "follow_uid"},
		},
		DoUpdates: clause.AssignmentColumns([]string{"status"}),
	}).Create(&userFollow).Error
}

func (r *userRepo) IncrementUserFollowCount(ctx common.Ctx, userFollowEntity *userBiz.UserFollowEntity) error {
	db := common.GetDB(ctx.Ctx, r.GetDb())
	userModel := &userModel.User{}
	return db.Model(userModel).Where("uid = ?", userFollowEntity.UID).Update("follow_count", gorm.Expr(userModel.TableName()+".follow_count + 1")).Error
}

func (r *userRepo) DecrementUserFollowCount(ctx common.Ctx, userFollowEntity *userBiz.UserFollowEntity) error {
	db := common.GetDB(ctx.Ctx, r.GetDb())
	userModel := &userModel.User{}
	return db.Model(userModel).Where("uid = ?", userFollowEntity.UID).Update("follow_count", gorm.Expr(userModel.TableName()+".follow_count - 1")).Error
}

func (r *userRepo) IncrementUserFollowerCount(ctx common.Ctx, userFollowEntity *userBiz.UserFollowEntity) error {
	db := common.GetDB(ctx.Ctx, r.GetDb())
	userModel := &userModel.User{}
	return db.Model(userModel).Where("uid = ?", userFollowEntity.FollowUID).Update("follower_count", gorm.Expr(userModel.TableName()+".follower_count + 1")).Error
}

func (r *userRepo) DecrementUserFollowerCount(ctx common.Ctx, userFollowEntity *userBiz.UserFollowEntity) error {
	db := common.GetDB(ctx.Ctx, r.GetDb())
	userModel := &userModel.User{}
	return db.Model(userModel).Where("uid = ?", userFollowEntity.FollowUID).Update("follower_count", gorm.Expr(userModel.TableName()+".follower_count - 1")).Error
}

func (r *userRepo) CreateUserNotification(ctx common.Ctx, userNotificationEntity *userBiz.UserNotificationEntity) error {
	userNotification := &userModel.UserNotification{}
	userNotification.FromEntity(userNotificationEntity)
	return r.Infra.Create(ctx, userNotification)
}

func (r *userRepo) GetUserNotifications(ctx common.Ctx, query *userBiz.UserNotificationQuery) ([]*userBiz.UserNotificationEntity, error) {
	db := query.Condition(r.GetDb().WithContext(ctx.Ctx).Model(&userModel.UserNotification{}), nil)
	var userNotifications []*userModel.UserNotification
	if err := db.Find(&userNotifications).Error; err != nil {
		ctx.Log.Errorf("GetUserNotifications sql failed, err: %v", err)
		return nil, err
	}
	userNotificationEntities := make([]*userBiz.UserNotificationEntity, 0, len(userNotifications))
	for _, userNotification := range userNotifications {
		userNotificationEntities = append(userNotificationEntities, userNotification.ToEntity())
	}
	return userNotificationEntities, nil
}

func (r *userRepo) GetUserNotificationsWithTotal(ctx common.Ctx, query *userBiz.UserNotificationQuery) ([]*userBiz.UserNotificationEntity, int64, error) {
	var total int64
	db := query.Condition(r.GetDb().WithContext(ctx.Ctx).Model(&userModel.UserNotification{}), &total)
	var userNotifications []*userModel.UserNotification
	if err := db.Find(&userNotifications).Error; err != nil {
		ctx.Log.Errorf("GetUserNotificationsWithTotal sql failed, err: %v", err)
		return nil, 0, err
	}
	userNotificationEntities := make([]*userBiz.UserNotificationEntity, 0, len(userNotifications))
	for _, userNotification := range userNotifications {
		userNotificationEntities = append(userNotificationEntities, userNotification.ToEntity())
	}
	return userNotificationEntities, total, nil
}

func (r *userRepo) CreateOrUpdateUserPostLikeNotification(ctx common.Ctx, userNotificationEntity *userBiz.UserNotificationEntity) error {
	db := common.GetDB(ctx.Ctx, r.GetDb())

	if userNotificationEntity.Type != 7 {
		return fmt.Errorf("wrong notification type: %d", userNotificationEntity.Type)
	}

	var likeData *userBiz.ReceiveLikeNotificationEntity
	if err := json.Unmarshal(userNotificationEntity.BizJson, &likeData); err != nil {
		return fmt.Errorf("BizJson unmarshal failed: %w", err)
	}

	if likeData.PostUUID == "" {
		return fmt.Errorf("BizJson missing valid post_uuid")
	}

	sql := `
		INSERT INTO t_user_notification (uuid, uid, type, biz_json, status, category, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, NOW(), NOW())
		ON CONFLICT (uid, (biz_json->>'post_uuid')) 
		WHERE type = 7 AND status = 2
		DO UPDATE SET
			biz_json = jsonb_set(
				t_user_notification.biz_json,
				'{new_like_count}',
				to_jsonb((t_user_notification.biz_json->>'new_like_count')::int + 1)
			),
			updated_at = NOW()
	`

	result := db.Exec(
		sql,
		userNotificationEntity.UUID,
		userNotificationEntity.UID,
		userNotificationEntity.Type,
		userNotificationEntity.BizJson,
		userNotificationEntity.Status,
		userNotificationEntity.Category,
	)

	return result.Error
}

func (r *userRepo) UpdateUserNotificationStatusToRead(ctx common.Ctx, notificationUUIDs []string) error {
	db := common.GetDB(ctx.Ctx, r.GetDb())
	return db.Model(&userModel.UserNotification{}).Where("uuid IN ?", notificationUUIDs).Updates(map[string]interface{}{"status": userBiz.NotificationStatusRead}).Error
}

func (r *userRepo) BatchCreateUserNotification(ctx common.Ctx, userNotificationEntities []*userBiz.UserNotificationEntity) error {
	db := common.GetDB(ctx.Ctx, r.GetDb()).Model(&userModel.UserNotification{})
	modelList := make([]*userModel.UserNotification, 0, len(userNotificationEntities))
	for _, userNotificationEntity := range userNotificationEntities {
		oneModel := &userModel.UserNotification{}
		oneModel.FromEntity(userNotificationEntity)
		modelList = append(modelList, oneModel)
	}
	return db.CreateInBatches(modelList, 100).Error
}
