package user

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"market-service/internal/biz/base"
	"market-service/internal/conf"
	"market-service/internal/pkg/alarm"
	"market-service/internal/pkg/common"
	"market-service/internal/pkg/util"
	"market-service/internal/sse_message"
	"strconv"
	"strings"
	"time"

	usercenterPb "market-proto/proto/market-service/usercenter/v1"

	ethCommon "github.com/ethereum/go-ethereum/common"
	"github.com/go-kratos/kratos/v2/errors"
	"github.com/go-kratos/kratos/v2/log"
	"gorm.io/gorm"
)

var (
	ErrNameTooLong              = errors.New(int(usercenterPb.ErrorCode_NAME_TOO_LONG), "NAME_TOO_LONG", "name length is too long")
	ErrDescTooLong              = errors.New(int(usercenterPb.ErrorCode_DESC_TOO_LONG), "DESC_TOO_LONG", "desc length is too long")
	ErrInvalidInviteCode        = errors.New(int(usercenterPb.ErrorCode_INVALID_INVITE_CODE), "INVALID_INVITE_CODE", "invite code is invalid")
	ErrDuplicateAddress         = errors.New(int(usercenterPb.ErrorCode_DUPLICATE_ADDRESS), "DUPLICATE_ADDRESS", "address already exists")
	ErrDuplicateEmail           = errors.New(int(usercenterPb.ErrorCode_DUPLICATE_EMAIL), "DUPLICATE_EMAIL", "email already exists")
	ErrUserNotFound             = errors.New(int(usercenterPb.ErrorCode_USER_NOT_FOUND), "USER_NOT_FOUND", "user not found")
	ErrRepeatFollowOrUnfollow   = errors.New(int(usercenterPb.ErrorCode_REPEAT_FOLLOW_OR_UNFOLLOW), "REPEAT_FOLLOW_OR_UNFOLLOW", "repeat follow or unfollow")
	ErrCanNotFollowSelf         = errors.New(int(usercenterPb.ErrorCode_FOLLOW_SELF), "CAN_NOT_FOLLOW_SELF", "can not follow self")
	ErrMustFollowFirst          = errors.New(int(usercenterPb.ErrorCode_NOT_FOLLOW), "MUST_FOLLOW_FIRST", "must follow first")
	ErrMarkOtherNotification    = errors.New(int(usercenterPb.ErrorCode_MARK_OTHER_NOTIFICATIONS), "MARK_OTHER_NOTIFICATION", "mark other's notification is not allowed")
	ErrInvalidNotificationUUIDs = errors.New(int(usercenterPb.ErrorCode_INVALID_NOTIFICATION_UUID), "INVALID_NOTIFICATION_UUIDS", "notifications is not valid")
	ErrBindInviteCodeRepeatedly = errors.New(int(usercenterPb.ErrorCode_BIND_INVITE_CODE_REPEAT), "BIND_INVITE_CODE_REPEATEDLY", "bind invite code repeatedly")
	ErrBindSelfInviteCode       = errors.New(int(usercenterPb.ErrorCode_BIND_SELF_INVITE_CODE), "BIND_SELF_INVITE_CODE", "bind self invite code is not allowed")
	ErrMutualInviteCode         = errors.New(int(usercenterPb.ErrorCode_MUTUAL_INVITE_CODE), "MUTUAL_INVITE_CODE", "mutual invite code is not allowed")
	ErrLoginTooQuick            = errors.New(int(usercenterPb.ErrorCode_LOGIN_TOO_QUICK), "LOGIN_TOO_QUICK", "login too quick")
	ErrUpdateUserTooQuick       = errors.New(int(usercenterPb.ErrorCode_UPDATE_USER_INFO_TOO_QUICK), "UPDATE_USER_INFO_TOO_QUICK", "update user info too quick")
)

// repo 包含db redis mq rpc操作
type UserRepoInterface interface {
	base.RepoInterface
	// user db
	GetUser(ctx common.Ctx, query *UserQuery) (*UserEntity, error)
	GetUsers(ctx common.Ctx, query *UserQuery) ([]*UserEntity, error)
	GetUsersWithTotal(ctx common.Ctx, query *UserQuery) ([]*UserEntity, int64, error)
	CreateUser(ctx common.Ctx, userEntity *UserEntity) error
	UpdateUser(ctx common.Ctx, userEntity *UserEntity) error
	IncrUserEarnedInvitePoints(ctx common.Ctx, uid string, points int64) error

	GetUserFollow(ctx common.Ctx, query *UserFollowQuery) (*UserFollowEntity, error)
	CreateOrUpdateUserFollow(ctx common.Ctx, userFollowEntity *UserFollowEntity) error

	IncrementUserFollowCount(ctx common.Ctx, userFollowEntity *UserFollowEntity) error
	DecrementUserFollowCount(ctx common.Ctx, userFollowEntity *UserFollowEntity) error
	IncrementUserFollowerCount(ctx common.Ctx, userFollowEntity *UserFollowEntity) error
	DecrementUserFollowerCount(ctx common.Ctx, userFollowEntity *UserFollowEntity) error

	CreateUserNotification(ctx common.Ctx, userNotificationEntity *UserNotificationEntity) error
	GetUserNotifications(ctx common.Ctx, query *UserNotificationQuery) ([]*UserNotificationEntity, error)
	GetUserNotificationsWithTotal(ctx common.Ctx, query *UserNotificationQuery) ([]*UserNotificationEntity, int64, error)
	CreateOrUpdateUserPostLikeNotification(ctx common.Ctx, userNotificationEntity *UserNotificationEntity) error
	UpdateUserNotificationStatusToRead(ctx common.Ctx, notificationUUIDs []string) error
	BatchCreateUserNotification(ctx common.Ctx, userNotificationEntities []*UserNotificationEntity) error
	//s3
	UploadFileToBizBucketS3(ctx common.Ctx, fileData []byte, key string) error
	DownloadFileFromBizBucketS3(ctx common.Ctx, key string) ([]byte, string, error)
	DownloadFileFromAdminBucketS3(ctx common.Ctx, key string) ([]byte, string, error)
}

type UserHandler struct {
	userRepo UserRepoInterface
	log      *log.Helper
	dataConf *conf.Data
}

func NewUserHandler(repo UserRepoInterface, logger log.Logger, dataConf *conf.Data) *UserHandler {
	return &UserHandler{userRepo: repo, log: log.NewHelper(logger), dataConf: dataConf}
}

func (handler *UserHandler) CreateUser(ctx common.Ctx, userEntity *UserEntity) (*UserEntity, error) {

	lockKey := fmt.Sprintf("create-user-lock-%s", userEntity.Address)
	lockID, ok, err := handler.userRepo.AcquireLock(ctx, lockKey, 5*time.Second)
	if err != nil {
		return nil, errors.New(int(usercenterPb.ErrorCode_REDIS), "REDIS_ERROR", err.Error())
	}
	if !ok {
		return nil, ErrLoginTooQuick
	}
	defer handler.userRepo.ReleaseLock(ctx, lockKey, lockID)

	if !ethCommon.IsHexAddress(userEntity.EoaAddress) || !ethCommon.IsHexAddress(userEntity.Address) {
		return nil, errors.New(int(usercenterPb.ErrorCode_PARAM), "PARAM_ERROR", "invalid eoa address or address")
	}
	if userEntity.Issuer == "" {
		return nil, errors.New(int(usercenterPb.ErrorCode_PARAM), "PARAM_ERROR", "issuer is empty")
	}
	if userEntity.Email == "" && userEntity.EoaAddress == "" {
		return nil, errors.New(int(usercenterPb.ErrorCode_PARAM), "PARAM_ERROR", "email and eoaAddress are both empty")
	}
	if len(userEntity.Name) > 64 {
		return nil, ErrNameTooLong
	}
	if len(userEntity.Description) > 1024 {
		return nil, ErrDescTooLong
	}

	err = handler.userRepo.ExecTx(ctx, func(ctx common.Ctx, tx *gorm.DB) error {
		// 1. 检查invitedByCode是否有效
		if userEntity.InviteCode != "" {
			inviteUser, err := handler.userRepo.GetUser(ctx, &UserQuery{InviteCode: userEntity.InviteCode})
			if err != nil {
				ctx.Log.Errorf("GetUser err: [%+v]", err)
				return err
			}
			if inviteUser == nil || inviteUser.UID == "" {
				return ErrInvalidInviteCode
			}
			userEntity.InviterUID = inviteUser.UID
			userEntity.InviteAt = time.Now().Unix()
		}

		// 2.生成uid
		userEntity.UID = util.GenerateUID()
		if userEntity.Name == "" {
			userEntity.Name = generateName(userEntity.UID)
		}
		// 3. 生成邀请码
		userEntity.InviteCode = util.GenerateInviteCode()

		// 4. 创建用户
		err := handler.userRepo.CreateUser(ctx, userEntity)
		if err != nil {
			// 检查唯一性约束冲突
			if errors.Is(err, gorm.ErrDuplicatedKey) {
				errMsg := err.Error()
				switch {
				case strings.Contains(errMsg, "address"), strings.Contains(errMsg, "eoa_address"):
					return ErrDuplicateAddress
				case strings.Contains(errMsg, "email"):
					return ErrDuplicateEmail
				case strings.Contains(errMsg, "uid"), strings.Contains(errMsg, "invite_code"):
					// 重新生成uid和邀请码并重试
					ctx.Log.Warnf("uid / invite_code 冲突, 尝试重新生成。errMsg: [%+v]", err.Error())
					userEntity.UID = util.GenerateUID()
					userEntity.InviteCode = util.GenerateInviteCode()
					return handler.userRepo.CreateUser(ctx, userEntity)
				default:
					ctx.Log.Errorf("CreateUser err: [%+v]", err)
					return errors.New(int(usercenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
				}
			}
			return err
		}
		return nil
	})
	if err != nil {
		ctx.Log.Errorf("CreateUser err: [%+v]", err)
		return nil, errors.New(int(usercenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}

	magiclinkUser, err := handler.userRepo.GetUser(ctx, &UserQuery{
		Email:  userEntity.Email,
		Source: UserSourceMagiclink,
	})
	if err != nil {
		ctx.Log.Errorf("GetUser err: [%+v]", err)
		return nil, errors.New(int(usercenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}
	// 旧用户 拿magiclink注册过
	if magiclinkUser != nil && magiclinkUser.UID != "" {
		// TODO
		ctx.Log.Infof("magiclinkUser: [%+v]", magiclinkUser)
		go alarm.Lark.Send(fmt.Sprintf("magiclinkUser: [%+v]", magiclinkUser))
	}

	return userEntity, err
}

func generateName(uid string) string {
	// Generic prediction-themed adjectives + nouns (no brand names)
	adjectives := []string{
		"Swift", "Bold", "Keen", "Sharp", "Bright",
		"Calm", "Prime", "Noble", "Vivid", "Lucid",
	}
	nouns := []string{
		"Seer", "Thinker", "Oracle", "Seeker", "Voyager",
		"Cipher", "Sage", "Ranger", "Scout", "Pilot",
	}

	// 从uid中提取snowflakeID (去掉"bayes"前缀)
	snowflakeID, _ := strconv.ParseInt(uid[5:], 10, 64)

	// 获取时间戳部分 (右移22位)
	timestamp := snowflakeID >> 22

	// 获取序列号部分 (与4095进行按位与操作)
	sequence := snowflakeID & 4095

	// 混合生成4位数字
	number := (timestamp%100)*100 + sequence%100
	number = number % 10000
	if number < 1000 {
		number += 1000
	}

	adj := adjectives[util.SecureRandom(0, int64(len(adjectives)-1))]
	noun := nouns[util.SecureRandom(0, int64(len(nouns)-1))]

	return fmt.Sprintf("%s%s_%04d", adj, noun, number)
}

func (handler *UserHandler) GetUsersInfo(ctx common.Ctx, query *UserQuery) ([]*UserEntity, error) {
	users, err := handler.userRepo.GetUsers(ctx, query)
	if err != nil {
		ctx.Log.Errorf("GetUsers err: [%+v]", err)
		return nil, errors.New(int(usercenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}
	return users, nil
}

func (handler *UserHandler) GetUserInfo(ctx common.Ctx, query *UserQuery) (*UserEntity, error) {
	user, err := handler.userRepo.GetUser(ctx, query)
	if err != nil {
		ctx.Log.Errorf("GetUser err: [%+v]", err)
		return nil, errors.New(int(usercenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}
	return user, nil
}

func (handler *UserHandler) UpdateUser(ctx common.Ctx, userEntity *UserEntity, inviteByCode string) (*UserEntity, error) {
	uid := userEntity.UID
	if uid == "" {
		return nil, errors.New(int(usercenterPb.ErrorCode_PARAM), "PARAM_ERROR", "uid is empty")
	}

	lockKey := fmt.Sprintf("user-update-lock-%s", uid)
	lockID, ok, err := handler.userRepo.AcquireLock(ctx, lockKey, 5*time.Second)
	if err != nil {
		return nil, errors.New(int(usercenterPb.ErrorCode_REDIS), "REDIS_ERROR", err.Error())
	}
	if !ok {
		return nil, ErrUpdateUserTooQuick
	}
	defer handler.userRepo.ReleaseLock(ctx, lockKey, lockID)

	user, err := handler.userRepo.GetUser(ctx, &UserQuery{UID: uid})
	if err != nil {
		ctx.Log.Errorf("GetUser err: [%+v]", err)
		return nil, errors.New(int(usercenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}
	if user == nil || user.UID == "" {
		return nil, ErrUserNotFound
	}

	var inviter *UserEntity
	if inviteByCode != "" {

		if user.InviterUID != "" && user.InviterUID != "0" {
			return nil, ErrBindInviteCodeRepeatedly
		}
		if inviteByCode == user.InviteCode {
			return nil, ErrBindSelfInviteCode
		}

		inviter, err = handler.userRepo.GetUser(ctx, &UserQuery{InviteCode: inviteByCode})
		if err != nil {
			ctx.Log.Errorf("GetUser err: [%+v]", err)
			return nil, errors.New(int(usercenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
		}
		if inviter == nil || inviter.UID == "" {
			return nil, ErrInvalidInviteCode
		}

		if inviter.InviterUID == user.UID {
			return nil, ErrMutualInviteCode
		}

		userEntity.InviterUID = inviter.UID
		userEntity.InviteAt = time.Now().Unix()
	}

	err = handler.userRepo.UpdateUser(ctx, userEntity)
	if err != nil {
		ctx.Log.Errorf("UpdateUser err: [%+v]", err)
		return nil, errors.New(int(usercenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}
	return inviter, nil
}

func (handler *UserHandler) UpdateUserInvitePoints(ctx common.Ctx, uid string, inviterUID string, points int64) error {
	err := handler.userRepo.ExecTx(ctx, func(ctx common.Ctx, tx *gorm.DB) error {

		err := handler.userRepo.UpdateUser(ctx, &UserEntity{
			UID:                 uid,
			ProvideInvitePoints: points,
		})
		if err != nil {
			ctx.Log.Errorf("UpdateUser err: [%+v]", err)
			return errors.New(int(usercenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
		}
		err = handler.userRepo.IncrUserEarnedInvitePoints(ctx, inviterUID, points)
		if err != nil {
			ctx.Log.Errorf("IncrUserEarnedInvitePoints err: [%+v]", err)
			return errors.New(int(usercenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
		}
		return nil
	})
	if err != nil {
		ctx.Log.Errorf("UpdateUserInvitePoints err: [%+v]", err)
		return errors.New(int(usercenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}
	return nil
}

func (handler *UserHandler) UpdateUserFollowStatus(ctx common.Ctx, userFollowEntity *UserFollowEntity) error {
	if userFollowEntity.UID == "" || userFollowEntity.FollowUID == "" {
		return errors.New(int(usercenterPb.ErrorCode_PARAM), "PARAM_ERROR", "uid or followUID is empty")
	}
	if userFollowEntity.Status != UserFollowStatusNormal && userFollowEntity.Status != UserFollowStatusDeleted {
		return errors.New(int(usercenterPb.ErrorCode_PARAM), "PARAM_ERROR", "invalid status")
	}
	if userFollowEntity.UID == userFollowEntity.FollowUID {
		return errors.New(int(usercenterPb.ErrorCode_PARAM), "PARAM_ERROR", "uid and followUID are the same")
	}

	// redis lock
	var err error
	lockKey := fmt.Sprintf("user-follow-lock-%s-%s", userFollowEntity.UID, userFollowEntity.FollowUID)
	lockID, ok, err := handler.userRepo.AcquireLock(ctx, lockKey, 5*time.Second)
	if err != nil {
		return errors.New(int(usercenterPb.ErrorCode_REDIS), "REDIS_ERROR", err.Error())
	}
	if !ok {
		return ErrUpdateUserTooQuick
	}
	defer handler.userRepo.ReleaseLock(ctx, lockKey, lockID)

	getusers, err := handler.userRepo.GetUsers(ctx, &UserQuery{
		UIDList: []string{
			userFollowEntity.UID,
			userFollowEntity.FollowUID,
		},
	})
	if err != nil {
		ctx.Log.Errorf("GetUsers err: [%+v]", err)
		return errors.New(int(usercenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}
	ctx.Log.Infof("len(getusers): [%+v] getusers: [%+v]", len(getusers), getusers)
	if len(getusers) != 2 {
		return ErrUserNotFound
	}

	userFollowRecord, err := handler.userRepo.GetUserFollow(ctx, &UserFollowQuery{
		UID:       userFollowEntity.UID,
		FollowUID: userFollowEntity.FollowUID,
	})
	if err != nil {
		ctx.Log.Errorf("GetUserFollow err: [%+v]", err)
		return errors.New(int(usercenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}

	if userFollowEntity.Status == UserFollowStatusDeleted {

		if userFollowRecord != nil && userFollowRecord.UID != "" {
			if userFollowRecord.Status == UserFollowStatusDeleted {
				return ErrRepeatFollowOrUnfollow
			}
		} else {
			return ErrMustFollowFirst
		}

	} else {

		if userFollowRecord != nil && userFollowRecord.UID != "" {
			if userFollowRecord.Status == UserFollowStatusNormal {
				return ErrRepeatFollowOrUnfollow
			}
		}
	}

	err = handler.userRepo.ExecTx(ctx, func(ctx common.Ctx, tx *gorm.DB) error {
		err = handler.userRepo.CreateOrUpdateUserFollow(ctx, userFollowEntity)
		if err != nil {
			ctx.Log.Errorf("UpdateUserFollowStatus err: [%+v]", err)
			return errors.New(int(usercenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
		}

		if userFollowEntity.Status == UserFollowStatusNormal {
			if err := handler.userRepo.IncrementUserFollowCount(ctx, userFollowEntity); err != nil {
				ctx.Log.Errorf("IncrementUserFollowCount err: [%+v]", err)
				return errors.New(int(usercenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
			}
			if err := handler.userRepo.IncrementUserFollowerCount(ctx, userFollowEntity); err != nil {
				ctx.Log.Errorf("IncrementUserFollowerCount err: [%+v]", err)
				return errors.New(int(usercenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
			}
		} else {
			if err := handler.userRepo.DecrementUserFollowCount(ctx, userFollowEntity); err != nil {
				ctx.Log.Errorf("DecrementUserFollowCount err: [%+v]", err)
				return errors.New(int(usercenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
			}
			if err := handler.userRepo.DecrementUserFollowerCount(ctx, userFollowEntity); err != nil {
				ctx.Log.Errorf("DecrementUserFollowerCount err: [%+v]", err)
				return errors.New(int(usercenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
			}
		}

		return nil
	})

	if err != nil {
		ctx.Log.Errorf("UpdateUserFollowStatus err: [%+v]", err)
		return errors.New(int(usercenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}
	return nil
}

func (handler *UserHandler) UploadFileToBizBucketS3(ctx common.Ctx, fileData []byte, biz string) (string, error) {
	hash := md5.Sum(fileData)
	fileName := hex.EncodeToString(hash[:])
	key := fmt.Sprintf("%s/%s", biz, fileName)
	err := handler.userRepo.UploadFileToBizBucketS3(ctx, fileData, key)
	if err != nil {
		ctx.Log.Errorf("UploadFileToBizBucketS3 err: [%+v]", err)
		return "", errors.New(int(usercenterPb.ErrorCode_S3), "S3_ERROR", err.Error())
	}
	return key, nil
}

func (handler *UserHandler) DownloadFileFromS3(ctx common.Ctx, key string) ([]byte, string, error) {
	if strings.HasPrefix(key, "avatar") {
		fileData, contentType, err := handler.userRepo.DownloadFileFromBizBucketS3(ctx, key)
		if err != nil {
			if strings.Contains(err.Error(), "NoSuchKey") {
				return nil, "", errors.New(int(usercenterPb.ErrorCode_NOT_FOUND), "NOT_FOUND", err.Error())
			}
			ctx.Log.Errorf("DownloadFileFromBizBucketS3 err: [%+v]", err)
			return nil, "", errors.New(int(usercenterPb.ErrorCode_S3), "S3_ERROR", err.Error())
		}
		return fileData, contentType, nil
	}
	fileData, contentType, err := handler.userRepo.DownloadFileFromAdminBucketS3(ctx, key)
	if err != nil {
		ctx.Log.Errorf("DownloadFileFromAdminBucketS3 err: [%+v]", err)
		return nil, "", errors.New(int(usercenterPb.ErrorCode_S3), "S3_ERROR", err.Error())
	}
	return fileData, contentType, nil
}

func (handler *UserHandler) SearchUser(ctx common.Ctx, query *UserQuery) ([]*UserEntity, int64, error) {
	if query.Search == "" {
		return nil, 0, errors.New(int(usercenterPb.ErrorCode_PARAM), "PARAM_ERROR", "search is empty")
	}
	users, total, err := handler.userRepo.GetUsersWithTotal(ctx, query)
	if err != nil {
		ctx.Log.Errorf("GetUsers err: [%+v]", err)
		return nil, 0, errors.New(int(usercenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}
	return users, total, nil
}

func (handler *UserHandler) GetUsersWithTotal(ctx common.Ctx, query *UserQuery) ([]*UserEntity, int64, error) {
	users, total, err := handler.userRepo.GetUsersWithTotal(ctx, query)
	if err != nil {
		ctx.Log.Errorf("GetUsers err: [%+v]", err)
		return nil, 0, errors.New(int(usercenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}
	return users, total, nil
}

func (handler *UserHandler) IsFollowed(ctx common.Ctx, uid string, followUID string) (bool, error) {
	userFollow, err := handler.userRepo.GetUserFollow(ctx, &UserFollowQuery{
		UID:       uid,
		FollowUID: followUID,
		Status:    UserFollowStatusNormal,
	})
	if err != nil {
		ctx.Log.Errorf("GetUserFollow err: [%+v]", err)
		return false, errors.New(int(usercenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}
	if userFollow == nil || userFollow.UID == "" {
		return false, nil
	}
	return true, nil
}

func (handler *UserHandler) GenerateNewUserNotification(ctx common.Ctx, userNotificationEntity *UserNotificationEntity) error {
	err := handler.userRepo.CreateUserNotification(ctx, userNotificationEntity)
	if err != nil {
		ctx.Log.Errorf("CreateUserNotification err: [%+v]", err)
		return errors.New(int(usercenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}
	return nil
}

func (handler *UserHandler) BatchGenerateNewUserNotification(ctx common.Ctx, userNotificationEntities []*UserNotificationEntity) error {
	err := handler.userRepo.BatchCreateUserNotification(ctx, userNotificationEntities)
	if err != nil {
		ctx.Log.Errorf("BatchCreateUserNotification err: [%+v]", err)
		return errors.New(int(usercenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}
	return nil
}

func (handler *UserHandler) GetUserNotifications(ctx common.Ctx, query *UserNotificationQuery) ([]*UserNotificationEntity, error) {
	userNotifications, err := handler.userRepo.GetUserNotifications(ctx, query)
	if err != nil {
		ctx.Log.Errorf("GetUserNotifications err: [%+v]", err)
		return nil, errors.New(int(usercenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}
	return userNotifications, nil
}

func (handler *UserHandler) GetUserNotificationsWithTotal(ctx common.Ctx, query *UserNotificationQuery) ([]*UserNotificationEntity, int64, error) {
	userNotifications, total, err := handler.userRepo.GetUserNotificationsWithTotal(ctx, query)
	if err != nil {
		ctx.Log.Errorf("GetUserNotificationsWithTotal err: [%+v]", err)
		return nil, 0, errors.New(int(usercenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}
	return userNotifications, total, nil
}

func (handler *UserHandler) UpdateUserNotification(ctx common.Ctx, userNotificationEntity *UserNotificationEntity, updateMap map[string]interface{}) error {
	err := handler.userRepo.ModifyByMap(ctx, userNotificationEntity, updateMap)
	if err != nil {
		ctx.Log.Errorf("UpdateUserNotification err: [%+v]", err)
		return errors.New(int(usercenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}
	return nil
}

func (handler *UserHandler) CreateOrUpdateUserPostLikeNotification(ctx common.Ctx, userNotificationEntity *UserNotificationEntity) error {
	err := handler.userRepo.CreateOrUpdateUserPostLikeNotification(ctx, userNotificationEntity)
	if err != nil {
		ctx.Log.Errorf("CreateOrUpdateUserPostLikeNotification err: [%+v]", err)
		return errors.New(int(usercenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}
	return nil
}

func (handler *UserHandler) UpdateNotificationsToRead(ctx common.Ctx, uid string, notificationUUIDs []string) error {

	// check
	if len(notificationUUIDs) == 0 || uid == "" {
		return errors.New(int(usercenterPb.ErrorCode_PARAM), "PARAM_ERROR", "notificationUUIDs or uid is empty")
	}

	notificationUUIDs = util.RemoveDuplicate(notificationUUIDs)
	list, err := handler.userRepo.GetUserNotifications(ctx, &UserNotificationQuery{
		UUIDList: notificationUUIDs,
	})
	if err != nil {
		ctx.Log.Errorf("GetUserNotifications err: [%+v]", err)
		return errors.New(int(usercenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}
	if len(list) != len(notificationUUIDs) {
		return ErrInvalidNotificationUUIDs
	}
	for _, notification := range list {
		if notification.UID != uid {
			return ErrMarkOtherNotification
		}
	}
	err = handler.userRepo.UpdateUserNotificationStatusToRead(ctx, notificationUUIDs)
	if err != nil {
		ctx.Log.Errorf("UpdateUserNotificationStatusToRead err: [%+v]", err)
		return errors.New(int(usercenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}
	return nil
}

func (handler *UserHandler) ProduceNewUserStreamMsg(ctx common.Ctx, userEntity *UserEntity) (string, error) {
	data, err := json.Marshal(userEntity)
	if err != nil {
		ctx.Log.Errorf("ProduceNewUserStreamMsg err: [%+v]", err)
		return "", errors.New(int(usercenterPb.ErrorCode_INTERNAL), "INTERNAL_ERROR", err.Error())
	}
	msgId, err := handler.userRepo.StreamAddMessage(ctx, NewUserStreamKey, map[string]interface{}{"data": data})
	if err != nil {
		ctx.Log.Errorf("ProcessMintPointsStreamMsg err: [%+v]", err)
		return "", errors.New(int(usercenterPb.ErrorCode_REDIS), "REDIS_ERROR", err.Error())
	}
	return msgId, nil
}

func (handler *UserHandler) ProduceBindInviteRelationStreamMsg(ctx common.Ctx, bindInviteRelationStreamMsg *BindInviteRelationStreamMsg) (string, error) {
	data, err := json.Marshal(bindInviteRelationStreamMsg)
	if err != nil {
		ctx.Log.Errorf("ProduceBindInviteRelationStreamMsg err: [%+v]", err)
		return "", errors.New(int(usercenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}
	msgId, err := handler.userRepo.StreamAddMessage(ctx, BindInviteRelationStreamKey, map[string]interface{}{"data": data})
	if err != nil {
		ctx.Log.Errorf("ProduceBindInviteRelationStreamMsg err: [%+v]", err)
		return "", errors.New(int(usercenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}
	return msgId, nil
}

func (handler *UserHandler) ProduceUserTradeStreamMsg(ctx common.Ctx, msg *UserTradeStreamMsg) (string, error) {
	data, err := json.Marshal(msg)
	if err != nil {
		ctx.Log.Errorf("ProduceUserTradeStreamMsg err: [%+v]", err)
		return "", errors.New(int(usercenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}
	msgId, err := handler.userRepo.StreamAddMessage(ctx, UserTradeStreamKey, map[string]interface{}{"data": data})
	if err != nil {
		ctx.Log.Errorf("ProduceUserTradeStreamMsg err: [%+v]", err)
		return "", errors.New(int(usercenterPb.ErrorCode_REDIS), "REDIS_ERROR", err.Error())
	}
	return msgId, nil
}

func (handler *UserHandler) ProduceClaimTaskRewardStreamMsg(ctx common.Ctx, claimTaskRewardStreamMsg *ClaimTaskRewardStreamMsg) (string, error) {
	data, err := json.Marshal(claimTaskRewardStreamMsg)
	if err != nil {
		ctx.Log.Errorf("ProduceClaimTaskRewardStreamMsg err: [%+v]", err)
		return "", errors.New(int(usercenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}
	msgId, err := handler.userRepo.StreamAddMessage(ctx, ClaimTaskRewardStreamKey, map[string]interface{}{"data": data})
	if err != nil {
		ctx.Log.Errorf("ProduceClaimTaskRewardStreamMsg err: [%+v]", err)
		return "", errors.New(int(usercenterPb.ErrorCode_REDIS), "REDIS_ERROR", err.Error())
	}

	return msgId, nil
}

func (handler *UserHandler) PublishUserChannel(ctx common.Ctx, uid string, message interface{}) error {
	return handler.userRepo.PublishJSON(ctx, fmt.Sprintf("%s%s", sse_message.ChannelUserPrefix, uid), message)
}

func (handler *UserHandler) PublishMarketChannel(ctx common.Ctx, marketAddress string, message interface{}) error {
	return handler.userRepo.PublishJSON(ctx, fmt.Sprintf("%s%s", sse_message.ChannelMarketPrefix, marketAddress), message)
}

func (handler *UserHandler) PublishBroadcastChannel(ctx common.Ctx, message interface{}) error {
	return handler.userRepo.PublishJSON(ctx, sse_message.ChannelBroadcast, message)
}
