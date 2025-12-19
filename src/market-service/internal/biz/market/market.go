package market

import (
	marketcenterPb "market-proto/proto/market-service/marketcenter/v1"
	"runtime/debug"
	"sort"

	"encoding/json"
	"fmt"
	"market-service/internal/biz/base"
	"market-service/internal/conf"
	"market-service/internal/pkg/alarm"
	"market-service/internal/pkg/common"
	"market-service/internal/pkg/util"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-redis/redis/v8"
	"github.com/shopspring/decimal"
	"gorm.io/gorm"

	"github.com/go-kratos/kratos/v2/errors"
)

var (
	ErrMarketNotFound         = errors.New(int(marketcenterPb.ErrorCode_MARKET_NOT_FOUND), "MARKET_NOT_FOUND", "market not found")
	ErrDuplicatedFollowMarket = errors.New(int(marketcenterPb.ErrorCode_REPEAT_FOLLOW_MARKET), "DUPLICATED_FOLLOW_OR_UNFOLLOW_MARKET", "already follow or unfollow to the market")
	ErrMustFollowFirst        = errors.New(int(marketcenterPb.ErrorCode_MUST_FOLLOW_FIRST), "MUST_FOLLOW_MARKET_FIRST", "must follow market first")
	ErrFollowTooQuick         = errors.New(int(marketcenterPb.ErrorCode_FOLLOW_MARKET_TOO_QUICK), "FOLLOW_MARKET_TOO_QUICK", "follow market too quick")
)

type MarketRepoInterface interface {
	base.RepoInterface

	GetMarket(ctx common.Ctx, query *MarketQuery) (*MarketEntity, error)
	GetMarkets(ctx common.Ctx, query *MarketQuery) ([]*MarketEntity, error)
	GetMarketsWithTotal(ctx common.Ctx, query *MarketQuery) ([]*MarketEntity, int64, error)
	GetOptions(ctx common.Ctx, query *OptionQuery) ([]*OptionEntity, error)

	UpdateMarketsEmbedding(ctx common.Ctx, marketEntityList []*MarketEntity) error

	UpdateMarketVolumeAndParticipants(ctx common.Ctx, marketEntity *MarketEntity) error
	UpdateMarketByAddress(ctx common.Ctx, marketEntity *MarketEntity, updateMap map[string]interface{}) error
	UpdateMarket(ctx common.Ctx, marketEntity *MarketEntity) error
	UpdateOption(ctx common.Ctx, optionEntity *OptionEntity) error

	GetUserMarketFollow(ctx common.Ctx, query *UserMarketFollowQuery) (*UserMarketFollowEntity, error)
	GetUserMarketFollows(ctx common.Ctx, query *UserMarketFollowQuery) ([]*UserMarketFollowEntity, error)
	GetUserMarketFollowsWithTotal(ctx common.Ctx, query *UserMarketFollowQuery) ([]*UserMarketFollowEntity, int64, error)
	CreateOrUpdateUserMarketFollow(ctx common.Ctx, userMarketFollowEntity *UserMarketFollowEntity) error

	BatchCreateMarketsAndOptions(ctx common.Ctx, marketEntityList []*MarketEntity) error
	BatchCreateMarketTags(ctx common.Ctx, tags []string) error
	GetMarketTagsWithTotal(ctx common.Ctx, query *MarketTagQuery) ([]*TagEntity, int64, error)
	GetMarketTagsOrderByMarketCount(ctx common.Ctx, query *MarketTagQuery) ([]*TagEntity, int64, error)

	BatchCreateOptionTokenPrice(ctx common.Ctx, optionTokenPriceEntityList []*OptionTokenPriceEntity) error
	GetLatestOptionPrice(ctx common.Ctx, optionAddressList []string) ([]*OptionTokenPriceEntity, error)
	GetMarketOptionPriceHistory(ctx common.Ctx, tokenAddresses []string, isMarketRunning bool, timeRange string) ([]*TokenPricePoint, error)

	// Redis
	GetMarketsAndOptionsFromCache(ctx common.Ctx, marketAddressList []string) ([]*MarketEntity, error)
	SetMarketsAndOptionsToCache(ctx common.Ctx, marketEntityList []*MarketEntity) error

	// s3
	DownloadFileFromAdminBucketS3(ctx common.Ctx, key string) ([]byte, string, error)

	EmbeddingMarketInfo(ctx common.Ctx, marketEntityList []*MarketEntity) ([]*MarketEntity, error)
	EmbeddingTags(ctx common.Ctx, tags []string) ([]float64, error)

	GetTagEmbeddingFromCache(ctx common.Ctx, tagList []string) ([]float64, error)
	SetTagEmbeddingToCache(ctx common.Ctx, tagList []string, embedding []float64) error
}

type MarketHandler struct {
	marketRepo MarketRepoInterface
	log        log.Logger
	confCustom *conf.Custom
}

type S3PredictionInfo struct {
	TenantId     int    `json:"TenantId"`
	Title        string `json:"Title"`
	Description  string `json:"Description"`
	Rules        string `json:"Rules"`
	RulesFileUrl string `json:"RulesFileUrl"`
	ImageFileUrl string `json:"ImageFileUrl"`
	IsSettled    bool   `json:"IsSettled"`
	// WinningOptionId    *string          `json:"WinningOptionId"`
	Status             int              `json:"Status"`
	PublishTx          string           `json:"PublishTx"`
	SettleTx           *string          `json:"SettleTx"`
	Address            string           `json:"Address"`
	Options            []S3OptionInfo   `json:"Options"`
	IsDeleted          bool             `json:"IsDeleted"`
	DeleterUserId      *int             `json:"DeleterUserId"`
	LastModifierUserId int              `json:"LastModifierUserId"`
	CreatorUserId      int              `json:"CreatorUserId"`
	Id                 string           `json:"Id"`
	Tags               []S3TagInfo      `json:"Tags"`
	Categories         []S3CategoryInfo `json:"Categories"`
	// ExpirationTime       time.Time      `json:"ExpirationTime"`
	// DeletionTime         *time.Time     `json:"DeletionTime"`
	// LastModificationTime time.Time      `json:"LastModificationTime"`
	// CreationTime         time.Time      `json:"CreationTime"`
}

type S3TagInfo struct {
	Id string `json:"Id"`
}

type S3CategoryInfo struct {
	Id     string `json:"Id"`
	Name   string `json:"Name"`
	Weight int32  `json:"Weight"`
}

type S3CategoryList []*S3CategoryInfo

// [{"Weight":1,"Image":"image_7e895486-04f2-4559-b4f1-73898136ae3d_X Cover.png","Type":1,"Url":"0x3AaC6832dCF48577B042567443F03c5cF9E3FfF3","Id":"7e895486-04f2-4559-b4f1-73898136ae3d"},{"Weight":2,"Image":"image_f043d535-48d0-4200-9351-ac134f0f2c57_X Cover.png","Type":0,"Url":"Sports","Id":"f043d535-48d0-4200-9351-ac134f0f2c57"}]
type S3BannerInfo struct {
	Weight int32  `json:"Weight"`
	Image  string `json:"Image"`
	Type   int32  `json:"Type"`
	Url    string `json:"Url"`
	Id     string `json:"Id"`
}

type S3BannerList []*S3BannerInfo

// [{"Predictions":[{"Prediction":"0x1F0c087178a5a189671B69889Cc5ef40cb8e37ec","Weight":3,"Id":"3fd884d7-9138-4bd8-4bba-08ddbea6190f"},{"Prediction":"0xDb31118563f04575aF3465839a62E2Ed5C628112","Weight":2,"Id":"142cfb75-1434-48fa-4bbb-08ddbea6190f"},{"Prediction":"0x769a0B6ADd80c77bDaC51b84461c7D8eC126fFD8","Weight":1,"Id":"f1ac40bb-6fcc-4ecc-4bbc-08ddbea6190f"}],"Color":"rgb(255,208,208)","Title":"Sports","Type":1,"Weight":1,"Id":"2b86b132-512d-4946-9892-6231667818cb"},{"Predictions":[{"Prediction":"0xF6F2E039D31Bd5B6560b7474e3b2c2aC6b6876a3","Weight":0,"Id":"60d1d5d7-89c2-4a0b-4bbd-08ddbea6190f"},{"Prediction":"0xCb806005752D3460b159EdF182BFB96CfE078F7d","Weight":1,"Id":"56176a73-9ed0-4f6d-4bbe-08ddbea6190f"},{"Prediction":"0x764774E6E926C43C0417C3374D6df9E7cD736bF3","Weight":2,"Id":"7a349794-8a74-49ed-4bbf-08ddbea6190f"},{"Prediction":"0xb29f7fc2637dDf58168fD184376fe8612d3d043E","Weight":3,"Id":"a14566cb-4d06-4680-4bc0-08ddbea6190f"},{"Prediction":"0xDb31118563f04575aF3465839a62E2Ed5C628112","Weight":4,"Id":"eb914ee8-a6af-4754-4bc1-08ddbea6190f"}],"Color":"rgb(83,159,143)","Title":"Trump","Type":0,"Weight":2,"Id":"4ace4d3f-c9a0-4982-be9b-9c00ecba8263"}]
type S3SectionPredictionInfo struct {
	Prediction string `json:"Prediction"`
	Weight     int32  `json:"Weight"`
	Id         string `json:"Id"`
}

type S3SectionInfo struct {
	Predictions []*S3SectionPredictionInfo `json:"Predictions"`
	Color       string                     `json:"Color"`
	Title       string                     `json:"Title"`
	Type        int32                      `json:"Type"`
	Weight      int32                      `json:"Weight"`
	Id          string                     `json:"Id"`
}

type S3SectionList []*S3SectionInfo

type S3OptionInfo struct {
	Content            string  `json:"Content"`
	ImageFileUrl       string  `json:"ImageFileUrl"`
	PredictionId       string  `json:"PredictionId"`
	Index              float64 `json:"Index"`
	Weight             float64 `json:"Weight"`
	IsDeleted          bool    `json:"IsDeleted"`
	DeleterUserId      *int    `json:"DeleterUserId"`
	LastModifierUserId int     `json:"LastModifierUserId"`
	CreatorUserId      int     `json:"CreatorUserId"`
	Id                 string  `json:"Id"`
	// DeletionTime         *time.Time `json:"DeletionTime"`
	// LastModificationTime time.Time `json:"LastModificationTime"`
	// CreationTime         time.Time  `json:"CreationTime"`
}

func NewMarketHandler(marketRepo MarketRepoInterface, logger log.Logger, conf *conf.Custom) *MarketHandler {
	return &MarketHandler{marketRepo: marketRepo, log: logger, confCustom: conf}
}

// tryFetchAndValidateS3Info 尝试从S3获取市场信息并验证是否包含图片和规则文件
func (h *MarketHandler) tryFetchAndValidateS3Info(ctx common.Ctx, address string) bool {
	data, _, err := h.marketRepo.DownloadFileFromAdminBucketS3(ctx, address)
	if err != nil {
		ctx.Log.Errorf("tryFetchAndValidateS3Info DownloadFileFromAdminBucketS3 error: %+v", err)
		return false
	}

	ctx.Log.Infof("tryFetchAndValidateS3Info DownloadFileFromAdminBucketS3: %s", string(data))
	marketS3Info := S3PredictionInfo{}
	err = json.Unmarshal(data, &marketS3Info)
	if err != nil {
		ctx.Log.Errorf("tryFetchAndValidateS3Info Unmarshal error: %+v", err)
		return false
	}

	// 验证是否有图片和规则文件
	hasImageOrRules := marketS3Info.ImageFileUrl != "" || marketS3Info.Rules != "" || marketS3Info.RulesFileUrl != ""
	if !hasImageOrRules {
		ctx.Log.Warnf("market %s S3 info missing required image or rules info", address)
		return false
	}

	ctx.Log.Infof("market %s S3 info validation successful", address)
	return true
}

func (h *MarketHandler) GetMarket(ctx common.Ctx, marketQuery *MarketQuery) (*MarketEntity, error) {
	marketEntity, err := h.marketRepo.GetMarket(ctx, marketQuery)
	if err != nil {
		ctx.Log.Errorf("GetMarket error: %+v", err)
		return nil, errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}
	return marketEntity, nil
}

func (h *MarketHandler) CreateMarketAndOptions(ctx common.Ctx, marketEntityList []*MarketEntity) ([]*MarketEntity, error) {
	// 从s3查询market图片， desc, rules, rules_file tag 和 option的图片
	// TODO 查s3能不能批量/并发
	tagList := make([]string, 0, len(marketEntityList))
	for _, marketEntity := range marketEntityList {
		marketS3InfoBytes, _, err := h.marketRepo.DownloadFileFromAdminBucketS3(ctx, marketEntity.Address)
		if err != nil {
			ctx.Log.Warnf("CreateMarketAndOptions DownloadFileFromAdminBucketS3 error: %+v", err)
			continue
		}
		marketS3Info := S3PredictionInfo{}
		err = json.Unmarshal(marketS3InfoBytes, &marketS3Info)
		if err != nil {
			ctx.Log.Errorf("CreateMarketAndOptions Unmarshal error: %+v", err)
			return nil, errors.New(int(marketcenterPb.ErrorCode_INTERNAL), "INTERNAL_ERROR", err.Error())
		}
		tags := make([]string, 0, len(marketS3Info.Tags))
		for _, tag := range marketS3Info.Tags {
			if tag.Id != "" {
				tags = append(tags, tag.Id)
			}
		}
		categories := make([]string, 0, len(marketS3Info.Categories))
		for _, category := range marketS3Info.Categories {
			if category.Id != "" {
				categories = append(categories, category.Id)
			}
		}

		marketEntity.Tags = tags
		marketEntity.Categories = categories

		tagList = append(tagList, tags...)
		IndexToS3OptionInfoMap := make(map[int]*S3OptionInfo)
		for _, s3Option := range marketS3Info.Options {
			IndexToS3OptionInfoMap[int(s3Option.Index)] = &S3OptionInfo{
				Content:      s3Option.Content,
				ImageFileUrl: s3Option.ImageFileUrl,
				PredictionId: s3Option.PredictionId,
				Index:        s3Option.Index,
				Weight:       s3Option.Weight,
			}
		}

		marketEntity.Description = marketS3Info.Description
		marketEntity.Rules = marketS3Info.Rules
		marketEntity.RulesUrl = marketS3Info.RulesFileUrl
		marketEntity.PicUrl = marketS3Info.ImageFileUrl

		for _, option := range marketEntity.Options {
			if s3Option, ok := IndexToS3OptionInfoMap[int(option.Index)]; ok {
				option.PicUrl = s3Option.ImageFileUrl
			}
		}

	}

	// 创建的同时设置无过期缓存(只包含静态信息 后续不更新) 包在事务里 目前只给爬块用于过滤非市场合约的同名事件
	err := h.marketRepo.ExecTx(ctx, func(ctx common.Ctx, db *gorm.DB) error {

		if err := h.marketRepo.BatchCreateMarketsAndOptions(ctx, marketEntityList); err != nil {
			ctx.Log.Errorf("BatchCreateMarketsAndOptions error: %+v", err)
			return err
		}

		if len(tagList) > 0 {
			tagList = util.RemoveDuplicate(tagList)
			if err := h.marketRepo.BatchCreateMarketTags(ctx, tagList); err != nil {
				ctx.Log.Errorf("BatchCreateMarketTags error: %+v", err)
				return err
			}
		}

		if err := h.marketRepo.SetMarketsAndOptionsToCache(ctx, marketEntityList); err != nil {
			ctx.Log.Errorf("SetMarketsAndOptionsToCache error: %+v", err)
			return err
		}
		return nil
	})
	if err != nil {
		ctx.Log.Errorf("CreateMarketAndOptions error: %+v", err)
		return nil, errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}

	for _, marketEntity := range marketEntityList {
		if marketEntity.Address == "" || (marketEntity.PicUrl != "" && marketEntity.Rules != "" && marketEntity.RulesUrl != "") {
			continue
		}
		go func(newCtx common.Ctx, address string) {
			defer func() {
				if err := recover(); err != nil {
					alarm.Lark.Send(fmt.Sprintf("tryFetchAndValidateS3Info panic err: %+v, stack: %+v", err, string(debug.Stack())))
					newCtx.Log.Errorf("tryFetchAndValidateS3Info panic err: %+v, stack: %+v", err, string(debug.Stack()))
				}
			}()

			timeout := time.NewTimer(2 * time.Minute)
			ticker := time.NewTicker(10 * time.Second)
			defer timeout.Stop()
			defer ticker.Stop()

			for {
				select {
				case <-timeout.C:
					newCtx.Log.Warnf("market %s failed to fetch complete S3 info after 60 seconds timeout", address)
					return
				case <-ticker.C:
					success := h.tryFetchAndValidateS3Info(newCtx, address)
					if success {
						err := h.UpdateMarketInfoByS3Data(newCtx, address)
						if err != nil {
							newCtx.Log.Errorf("UpdateMarketInfoByS3Data error: %+v", err)
							continue
						}
						return
					}
				}
			}
		}(common.CloneBaseCtx(ctx, h.log), marketEntity.Address)

	}

	return marketEntityList, nil
}

func (h *MarketHandler) EmbeddingMarketInfo(ctx common.Ctx, marketEntityList []*MarketEntity) error {
	marketEntityList, err := h.marketRepo.EmbeddingMarketInfo(ctx, marketEntityList)
	if err != nil {
		ctx.Log.Errorf("EmbeddingMarketInfo error: %+v", err)
		return errors.New(int(marketcenterPb.ErrorCode_RPC), "RPC_ERROR", err.Error())
	}

	err = h.marketRepo.UpdateMarketsEmbedding(ctx, marketEntityList)
	if err != nil {
		ctx.Log.Errorf("UpdateMarketsEmbedding error: %+v", err)
		return errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}
	return nil
}

// 爬块查询市场和代币静态信息 用于过滤链上事件
func (h *MarketHandler) GetMarketsAndOptionsForBlockListener(ctx common.Ctx, marketsEntity []*MarketEntity) ([]*MarketEntity, error) {

	marketAddressList := make([]string, 0, len(marketsEntity))
	for _, marketEntity := range marketsEntity {
		marketAddressList = append(marketAddressList, marketEntity.Address)
	}

	// 查询市场信息
	marketEntityList, err := h.marketRepo.GetMarkets(ctx, &MarketQuery{
		AddressList: marketAddressList,
	})
	if err != nil {
		ctx.Log.Errorf("GetMarketsAndOptionsForBlockListener GetMarkets error: %+v", err)
		return nil, errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}

	if len(marketEntityList) == 0 {
		return marketEntityList, nil
	}

	// 查询选项信息
	optionEntityList, err := h.marketRepo.GetOptions(ctx, &OptionQuery{
		MarketAddressList: marketAddressList,
	})
	if err != nil {
		ctx.Log.Errorf("GetMarketsAndOptionsForBlockListener GetOptions error: %+v", err)
		return nil, errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}

	marketAddressToOptionEntityMap := make(map[string][]*OptionEntity)
	for _, optionEntity := range optionEntityList {
		if _, ok := marketAddressToOptionEntityMap[optionEntity.MarketAddress]; !ok {
			marketAddressToOptionEntityMap[optionEntity.MarketAddress] = make([]*OptionEntity, 0)
		}
		marketAddressToOptionEntityMap[optionEntity.MarketAddress] = append(marketAddressToOptionEntityMap[optionEntity.MarketAddress], optionEntity)
	}

	for _, marketEntity := range marketEntityList {
		if optionEntityList, ok := marketAddressToOptionEntityMap[marketEntity.Address]; ok {
			marketEntity.Options = optionEntityList
		}
	}

	return marketEntityList, nil
}

func (h *MarketHandler) IsFollowedMarket(ctx common.Ctx, marketAddress string, uid string) (bool, error) {
	userMarketFollow, err := h.marketRepo.GetUserMarketFollow(ctx, &UserMarketFollowQuery{
		MarketAddress: marketAddress,
		UID:           uid,
		Status:        UserMarketFollowStatusActive,
	})
	if err != nil {
		ctx.Log.Errorf("IsFollowedMarket GetUserMarketFollow error: %+v", err)
		return false, errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}
	if userMarketFollow == nil || userMarketFollow.Status != UserMarketFollowStatusActive {
		return false, nil
	}
	return true, nil
}

func (h *MarketHandler) GetUserFollowedMarketsWithTotal(ctx common.Ctx, query *UserMarketFollowQuery) ([]*MarketEntity, int64, error) {
	userMarketFollows, total, err := h.marketRepo.GetUserMarketFollowsWithTotal(ctx, query)
	if err != nil {
		ctx.Log.Errorf("GetUserFollowedMarket GetUserMarketFollow error: %+v", err)
		return nil, 0, errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}

	if len(userMarketFollows) == 0 {
		return nil, 0, nil
	}

	var marketAddressList []string
	for _, userMarketFollow := range userMarketFollows {
		marketAddressList = append(marketAddressList, userMarketFollow.MarketAddress)
	}

	marketEntityList, err := h.marketRepo.GetMarkets(ctx, &MarketQuery{
		AddressList: marketAddressList,
	})
	if err != nil {
		ctx.Log.Errorf("GetUserFollowedMarket GetMarkets error: %+v", err)
		return nil, 0, errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}
	return marketEntityList, total, nil
}

func (h *MarketHandler) UpdateUserMarketFollowStatus(ctx common.Ctx, userMarketFollowEntity *UserMarketFollowEntity) error {
	if userMarketFollowEntity.Status != UserMarketFollowStatusActive && userMarketFollowEntity.Status != UserMarketFollowStatusInactive {
		return errors.New(int(marketcenterPb.ErrorCode_PARAM), "PARAM_ERROR", "invalid status")
	}
	if userMarketFollowEntity.UID == "" || userMarketFollowEntity.MarketAddress == "" {
		return errors.New(int(marketcenterPb.ErrorCode_PARAM), "PARAM_ERROR", "invalid uid or market address")
	}
	// redis lock
	var err error
	lockKey := fmt.Sprintf("user-follow-market-lock-%s-%s", userMarketFollowEntity.UID, userMarketFollowEntity.MarketAddress)
	lockID, ok, err := h.marketRepo.AcquireLock(ctx, lockKey, 5*time.Second)
	if err != nil {
		return errors.New(int(marketcenterPb.ErrorCode_REDIS), "REDIS_ERROR", err.Error())
	}
	if !ok {
		return ErrFollowTooQuick
	}
	defer h.marketRepo.ReleaseLock(ctx, lockKey, lockID)

	marketEntity, err := h.marketRepo.GetMarket(ctx, &MarketQuery{
		Address: userMarketFollowEntity.MarketAddress,
	})
	if err != nil {
		return errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}
	if marketEntity == nil || marketEntity.Address == "" {
		return errors.New(int(marketcenterPb.ErrorCode_NOT_FOUND), "NOT_FOUND", "market not found")
	}

	checkRecord, err := h.marketRepo.GetUserMarketFollow(ctx, &UserMarketFollowQuery{
		MarketAddress: userMarketFollowEntity.MarketAddress,
		UID:           userMarketFollowEntity.UID,
	})
	if err != nil {
		return errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}

	if userMarketFollowEntity.Status == UserMarketFollowStatusInactive {
		if checkRecord != nil && checkRecord.UID != "" {
			if checkRecord.Status == UserMarketFollowStatusInactive {
				return ErrDuplicatedFollowMarket
			}
		} else {
			return ErrMustFollowFirst
		}
	} else {
		if checkRecord != nil && checkRecord.UID != "" {
			if checkRecord.Status == UserMarketFollowStatusActive {
				return ErrDuplicatedFollowMarket
			}
		}
	}

	err = h.marketRepo.CreateOrUpdateUserMarketFollow(ctx, userMarketFollowEntity)
	if err != nil {
		ctx.Log.Errorf("UpdateUserMarketFollowStatus CreateOrUpdateUserMarketFollow error: %+v", err)
		return errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}
	return nil
}

func (h *MarketHandler) GetMarketsByTokenAddress(ctx common.Ctx, tokenAddressList []string) ([]*MarketEntity, error) {
	if len(tokenAddressList) == 0 {
		return nil, nil
	}
	optionEntities, err := h.marketRepo.GetOptions(ctx, &OptionQuery{
		AddressList: tokenAddressList,
	})
	if err != nil {
		ctx.Log.Errorf("GetMarketsByTokenAddress GetOptions error: %+v", err)
		return nil, errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}

	marketAddressList := make([]string, 0, len(optionEntities))
	for _, optionEntity := range optionEntities {
		marketAddressList = append(marketAddressList, optionEntity.MarketAddress)
	}

	if len(marketAddressList) == 0 {
		return nil, nil
	}
	marketEntityList, err := h.marketRepo.GetMarkets(ctx, &MarketQuery{
		AddressList: marketAddressList,
	})
	if err != nil {
		ctx.Log.Errorf("GetMarketsByTokenAddress GetMarkets error: %+v", err)
		return nil, errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}
	return marketEntityList, nil
}

func (h *MarketHandler) GetMarkets(ctx common.Ctx, query *MarketQuery) ([]*MarketEntity, error) {
	marketEntityList, err := h.marketRepo.GetMarkets(ctx, query)
	if err != nil {
		ctx.Log.Errorf("GetMarkets GetMarkets error: %+v", err)
		return nil, errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}
	return marketEntityList, nil
}

func (h *MarketHandler) GetMarketsAndOptionsInfoAndPricesWithTotal(ctx common.Ctx, query *MarketQuery, uid string, notQueryPrice bool) ([]*MarketEntity, int64, error) {

	marketEntityList, total, err := h.marketRepo.GetMarketsWithTotal(ctx, query)
	if err != nil {
		ctx.Log.Errorf("GetMarketsAndOptionsInfoWithTotal GetMarketsWithTotal error: %+v", err)
		return nil, 0, errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}

	if len(marketEntityList) == 0 {
		return marketEntityList, total, nil
	}

	marketAddressList := make([]string, 0, len(marketEntityList))
	for _, marketEntity := range marketEntityList {
		marketAddressList = append(marketAddressList, marketEntity.Address)
	}
	optionEntityList, err := h.marketRepo.GetOptions(ctx, &OptionQuery{
		MarketAddressList: marketAddressList,
	})
	if err != nil {
		ctx.Log.Errorf("GetMarketsAndOptionsInfoWithTotal GetOptions error: %+v", err)
		return nil, 0, errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}

	marketAddressToOptionEntityMap := make(map[string][]*OptionEntity)
	optionAddressList := make([]string, 0, len(optionEntityList))
	for _, optionEntity := range optionEntityList {
		if _, ok := marketAddressToOptionEntityMap[optionEntity.MarketAddress]; !ok {
			marketAddressToOptionEntityMap[optionEntity.MarketAddress] = make([]*OptionEntity, 0)
		}
		marketAddressToOptionEntityMap[optionEntity.MarketAddress] = append(marketAddressToOptionEntityMap[optionEntity.MarketAddress], optionEntity)
		optionAddressList = append(optionAddressList, optionEntity.Address)
	}

	for _, marketEntity := range marketEntityList {
		if optionEntityList, ok := marketAddressToOptionEntityMap[marketEntity.Address]; ok {
			marketEntity.Options = optionEntityList
		}
	}

	if notQueryPrice {
		return marketEntityList, total, nil
	}

	optionTokenPriceMap := make(map[string]*OptionTokenPriceEntity)
	optionTokenPriceEntities, err := h.marketRepo.GetLatestOptionPrice(ctx, optionAddressList)
	if err != nil {
		ctx.Log.Errorf("GetMarketsAndOptionsInfoAndPricesWithTotal GetLatestOptionPrice error: %+v", err)
		return nil, 0, errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}

	for _, optionTokenPriceEntity := range optionTokenPriceEntities {
		optionTokenPriceMap[optionTokenPriceEntity.TokenAddress] = optionTokenPriceEntity
	}

	userMarketFollowMap := make(map[string]bool)
	if uid != "" {
		userMarketFollows, err := h.marketRepo.GetUserMarketFollows(ctx, &UserMarketFollowQuery{
			UID:               uid,
			MarketAddressList: marketAddressList,
			Status:            UserMarketFollowStatusActive,
		})
		if err != nil {
			ctx.Log.Errorf("GetMarketsAndOptionsInfoAndPricesWithTotal GetUserMarketFollows error: %+v", err)
			return nil, 0, errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
		}
		for _, userMarketFollow := range userMarketFollows {
			userMarketFollowMap[userMarketFollow.MarketAddress] = true
		}
	}

	for _, marketEntity := range marketEntityList {
		if _, ok := userMarketFollowMap[marketEntity.Address]; ok {
			marketEntity.IsFollowed = uint8(UserMarketFollowStatusActive)
		}
		for _, optionEntity := range marketEntity.Options {
			if optionTokenPriceEntity, ok := optionTokenPriceMap[optionEntity.Address]; ok {
				optionEntity.OptionTokenPrice = optionTokenPriceEntity
			}
		}
	}
	return marketEntityList, total, nil
}

func (h *MarketHandler) GetOptions(ctx common.Ctx, query *OptionQuery) ([]*OptionEntity, error) {
	optionEntityList, err := h.marketRepo.GetOptions(ctx, query)
	if err != nil {
		ctx.Log.Errorf("GetOptions GetOptions error: %+v", err)
		return nil, errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}
	return optionEntityList, nil
}

func (h *MarketHandler) GetMarketDetail(ctx common.Ctx, marketAddress string) (*MarketEntity, error) {
	marketEntity, err := h.marketRepo.GetMarket(ctx, &MarketQuery{
		Address: marketAddress,
	})
	if err != nil {
		ctx.Log.Errorf("GetMarketDetail GetMarket error: %+v", err)
		return nil, errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}

	if marketEntity == nil || marketEntity.Address == "" {
		return marketEntity, nil
	}

	optionEntityList, err := h.marketRepo.GetOptions(ctx, &OptionQuery{
		MarketAddress: marketAddress,
	})

	tokenAddressList := make([]string, 0, len(optionEntityList))
	for _, optionEntity := range optionEntityList {
		tokenAddressList = append(tokenAddressList, optionEntity.Address)
	}

	optionTokenPriceEntities, err := h.marketRepo.GetLatestOptionPrice(ctx, tokenAddressList)
	if err != nil {
		ctx.Log.Errorf("GetMarketDetail GetLatestOptionPrice error: %+v", err)
		return nil, errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}

	optionTokenPriceMap := make(map[string]*OptionTokenPriceEntity)
	for _, optionTokenPriceEntity := range optionTokenPriceEntities {
		optionTokenPriceMap[optionTokenPriceEntity.TokenAddress] = optionTokenPriceEntity
	}

	for _, optionEntity := range optionEntityList {
		if optionTokenPriceEntity, ok := optionTokenPriceMap[optionEntity.Address]; ok {
			optionEntity.OptionTokenPrice = optionTokenPriceEntity
		}
		marketEntity.Options = append(marketEntity.Options, optionEntity)
	}
	return marketEntity, nil
}

func (h *MarketHandler) GetOptionPrices(ctx common.Ctx, optionAddressList []string) ([]*OptionTokenPriceEntity, error) {
	optionTokenPriceEntities, err := h.marketRepo.GetLatestOptionPrice(ctx, optionAddressList)
	if err != nil {
		ctx.Log.Errorf("GetOptionPrices GetLatestOptionPrice error: %+v", err)
		return nil, errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}
	return optionTokenPriceEntities, nil
}

func (h *MarketHandler) SearchMarket(ctx common.Ctx, query *MarketQuery) ([]*MarketEntity, int64, error) {
	if query.Search == "" {
		return nil, 0, errors.New(int(marketcenterPb.ErrorCode_PARAM), "PARAM_ERROR", "search is empty")
	}
	marketEntityList, total, err := h.marketRepo.GetMarketsWithTotal(ctx, query)
	if err != nil {
		ctx.Log.Errorf("SearchMarket GetMarketsWithTotal error: %+v", err)
		return nil, 0, errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}
	return marketEntityList, total, nil
}

func (h *MarketHandler) ProcessMarketDepositEventInMarketHandler(ctx common.Ctx, req *marketcenterPb.ProcessMarketDepositOrWithdrawEventRequest) error {

	// amountIn 投入的资产代币数量
	amountIn, err := decimal.NewFromString(req.AmountIn)
	if err != nil {
		return errors.New(int(marketcenterPb.ErrorCode_PARAM), "PARAM_ERROR", err.Error())
	}

	// 3. 更新market表 参与人数 volume
	err = h.marketRepo.UpdateMarketVolumeAndParticipants(ctx, &MarketEntity{
		Address:           req.MarketAddress,
		Volume:            amountIn,
		ParticipantsCount: 1,
	})
	if err != nil {
		ctx.Log.Errorf("ProcessUserDepositEventInMarketHandler UpdateMarketVolumeAndParticipants error: %+v", err)
		return err
	}
	// 4. 更新option_price表 价格
	optionTokenPriceEntityList := make([]*OptionTokenPriceEntity, 0, len(req.OptionPrices))
	for _, optionPrice := range req.OptionPrices {

		price, err := decimal.NewFromString(optionPrice.Price)
		if err != nil {
			return errors.New(int(marketcenterPb.ErrorCode_PARAM), "PARAM_ERROR", err.Error())
		}

		optionTokenPriceEntityList = append(optionTokenPriceEntityList, &OptionTokenPriceEntity{
			TokenAddress:  optionPrice.Address,
			Price:         price,
			Decimals:      uint8(optionPrice.Decimal),
			BaseTokenType: uint8(req.BaseTokenType),
			BlockNumber:   req.BlockNumber,
			BlockTime:     time.Unix(int64(req.BlockTime), 0),
		})
	}
	err = h.marketRepo.BatchCreateOptionTokenPrice(ctx, optionTokenPriceEntityList)
	if err != nil {
		ctx.Log.Errorf("ProcessUserDepositEventInMarketHandler BatchCreateOptionTokenPrice error: %+v", err)
		return errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}
	return nil
}

func (h *MarketHandler) ProcessMarketWithdrawEventInMarketHandler(ctx common.Ctx, req *marketcenterPb.ProcessMarketDepositOrWithdrawEventRequest) error {

	// amountOut 提取的资产代币数量
	amountOut, err := decimal.NewFromString(req.AmountOut)
	if err != nil {
		return errors.New(int(marketcenterPb.ErrorCode_PARAM), "PARAM_ERROR", err.Error())
	}
	// 3. 更新market表 参与人数 volume
	err = h.marketRepo.UpdateMarketVolumeAndParticipants(ctx, &MarketEntity{
		Address:           req.MarketAddress,
		Volume:            amountOut,
		ParticipantsCount: 1,
	})
	if err != nil {
		ctx.Log.Errorf("ProcessUserDepositEventInMarketHandler UpdateMarketVolumeAndParticipants error: %+v", err)
		return errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}

	// 4. 更新option_price表 价格
	optionTokenPriceEntityList := make([]*OptionTokenPriceEntity, 0, len(req.OptionPrices))
	for _, optionPrice := range req.OptionPrices {
		price, err := decimal.NewFromString(optionPrice.Price)
		if err != nil {
			return errors.New(int(marketcenterPb.ErrorCode_PARAM), "PARAM_ERROR", err.Error())
		}
		optionTokenPriceEntityList = append(optionTokenPriceEntityList, &OptionTokenPriceEntity{
			TokenAddress:  optionPrice.Address,
			Price:         price,
			Decimals:      uint8(optionPrice.Decimal),
			BaseTokenType: uint8(req.BaseTokenType),
			BlockNumber:   req.BlockNumber,
			BlockTime:     time.Unix(int64(req.BlockTime), 0),
		})
	}
	err = h.marketRepo.BatchCreateOptionTokenPrice(ctx, optionTokenPriceEntityList)
	if err != nil {
		ctx.Log.Errorf("ProcessUserDepositEventInMarketHandler BatchCreateOptionTokenPrice error: %+v", err)
		return errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}
	return nil
}

func (h *MarketHandler) ProcessMarketSwapEventInMarketHandler(ctx common.Ctx, req *marketcenterPb.ProcessMarketSwapEventRequest) error {

	optionTokenPriceEntityList := make([]*OptionTokenPriceEntity, 0, len(req.OptionPrices))
	for _, optionPrice := range req.OptionPrices {
		price, err := decimal.NewFromString(optionPrice.Price)
		if err != nil {
			return errors.New(int(marketcenterPb.ErrorCode_PARAM), "PARAM_ERROR", err.Error())
		}
		optionTokenPriceEntityList = append(optionTokenPriceEntityList, &OptionTokenPriceEntity{
			TokenAddress:  optionPrice.Address,
			Price:         price,
			Decimals:      uint8(optionPrice.Decimal),
			BaseTokenType: uint8(req.BaseTokenType),
			BlockNumber:   req.BlockNumber,
			BlockTime:     time.Unix(int64(req.BlockTime), 0),
		})
	}
	err := h.marketRepo.BatchCreateOptionTokenPrice(ctx, optionTokenPriceEntityList)
	if err != nil {
		ctx.Log.Errorf("ProcessUserDepositEventInMarketHandler BatchCreateOptionTokenPrice error: %+v", err)
		return errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}
	return nil
}

func (h *MarketHandler) UpdateMarketByAddress(ctx common.Ctx, marketAddress string, updateMap map[string]interface{}) error {

	err := h.marketRepo.UpdateMarketByAddress(ctx, &MarketEntity{Address: marketAddress}, updateMap)
	if err != nil {
		ctx.Log.Errorf("UpdateMarketByAddress UpdateMarketByAddress error: %+v", err)
		return err
	}
	return nil
}

func (h *MarketHandler) GetMarketOptionPriceHistory(ctx common.Ctx, marketAddress string, timeRange string) ([]*TokenPricePoint, map[string]*OptionEntity, []string, error) {

	var isMarketRunning bool = false
	marketEntity, err := h.marketRepo.GetMarket(ctx, &MarketQuery{
		Address: marketAddress,
	})
	if err != nil {
		ctx.Log.Errorf("GetMarketOptionPriceHistory GetMarket error: %+v", err)
		return nil, nil, nil, errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}
	if marketEntity == nil || marketEntity.Address != marketAddress {
		return nil, nil, nil, errors.New(int(marketcenterPb.ErrorCode_MARKET_NOT_FOUND), "MARKET_NOT_FOUND", "market not found")
	}

	if marketEntity.Status == MarketStatusRunnig {
		isMarketRunning = true
	}

	optionEntities, err := h.marketRepo.GetOptions(ctx, &OptionQuery{
		MarketAddress: marketAddress,
	})
	if err != nil {
		ctx.Log.Errorf("GetMarketOptionPriceHistory GetOptions error: %+v", err)
		return nil, nil, nil, errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}
	if len(optionEntities) == 0 {
		return nil, nil, nil, errors.New(int(marketcenterPb.ErrorCode_MARKET_NOT_FOUND), "MARKET_NOT_FOUND", "no options found")
	}

	tokenAddresses := make([]string, 0, len(optionEntities))
	tokenAddressMap := make(map[string]*OptionEntity)
	for _, optionEntity := range optionEntities {
		tokenAddresses = append(tokenAddresses, optionEntity.Address)
		tokenAddressMap[optionEntity.Address] = optionEntity
	}
	tokenPricePoints, err := h.marketRepo.GetMarketOptionPriceHistory(ctx, tokenAddresses, isMarketRunning, timeRange)
	if err != nil {
		ctx.Log.Errorf("GetMarketOptionPriceHistory error: %+v", err)
		return nil, nil, nil, errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}
	return tokenPricePoints, tokenAddressMap, tokenAddresses, nil
}

func (h *MarketHandler) UpdateMarketStatus(ctx common.Ctx, entity *MarketEntity) error {

	var status uint8
	switch entity.Status {
	case MarketStatusRunnig:
		status = MarketStatusRunnig
	case MarketStatusSettling:
		status = MarketStatusSettling
	case MarketStatusDisputed:
		status = MarketStatusDisputed
	case MarketStatusEnd:
		status = MarketStatusEnd
	default:
		return errors.New(int(marketcenterPb.ErrorCode_PARAM), "PARAM_ERROR", "invalid status")
	}

	err := h.marketRepo.UpdateMarketByAddress(ctx, entity, map[string]interface{}{
		"status": status,
	})
	if err != nil {
		ctx.Log.Errorf("UpdateMarketStatus UpdateMarketByAddress error: %+v", err)
		return errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}
	return nil
}

func (h *MarketHandler) GetMarketTags(ctx common.Ctx, query *MarketTagQuery) ([]*TagEntity, int64, error) {
	tagEntities, total, err := h.marketRepo.GetMarketTagsOrderByMarketCount(ctx, query)
	if err != nil {
		ctx.Log.Errorf("GetMarketTags GetMarketTagsWithTotal error: %+v", err)
		return nil, 0, errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}
	return tagEntities, total, nil
}

func (h *MarketHandler) BatchUpdateOptionPrice(ctx common.Ctx, optionTokenPriceEntityList []*OptionTokenPriceEntity) error {
	err := h.marketRepo.BatchCreateOptionTokenPrice(ctx, optionTokenPriceEntityList)
	if err != nil {
		ctx.Log.Errorf("BatchUpdateOptionPrice BatchCreateOptionTokenPrice error: %+v", err)
		return errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}
	return nil
}

func (h *MarketHandler) GetTagsEmbedding(ctx common.Ctx, tagList []string) ([]float64, error) {

	tagEmbedding, err := h.marketRepo.GetTagEmbeddingFromCache(ctx, tagList)
	if err != nil && err != redis.Nil {
		ctx.Log.Errorf("GetTagsEmbedding GetTagEmbeddingFromCache error: %+v", err)
		return nil, errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}
	if tagEmbedding != nil && len(tagEmbedding) > 0 {
		ctx.Log.Infof("GetTagsEmbedding GetTagEmbeddingFromCache success. ")
		return tagEmbedding, nil
	}

	tagEmbedding, err = h.marketRepo.EmbeddingTags(ctx, tagList)
	if err != nil {
		ctx.Log.Errorf("GetTagEmbedding GetTagEmbedding error: %+v", err)
		return nil, errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}

	err = h.marketRepo.SetTagEmbeddingToCache(ctx, tagList, tagEmbedding)
	if err != nil {
		ctx.Log.Errorf("GetTagsEmbedding SetTagEmbeddingToCache error: %+v", err)
		return nil, errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}
	return tagEmbedding, nil
}

func (h *MarketHandler) GetCategoriesFromS3(ctx common.Ctx, baseTokenType uint8) ([]*S3CategoryInfo, int64, error) {

	var key string
	switch baseTokenType {
	case BaseTokenTypePoints:
		key = S3MarketCategoriesDefaultKey
	case BaseTokenTypeUsdc:
		key = S3MarketCategoriesUsdcKey
	default:
		key = S3MarketCategoriesDefaultKey
	}
	data, _, err := h.marketRepo.DownloadFileFromAdminBucketS3(ctx, key)
	if err != nil {
		return nil, 0, errors.New(int(marketcenterPb.ErrorCode_S3), "S3_ERROR", err.Error())
	}

	s3CategoryList := S3CategoryList{}
	err = json.Unmarshal(data, &s3CategoryList)
	if err != nil {
		return nil, 0, errors.New(int(marketcenterPb.ErrorCode_S3), "S3_ERROR", err.Error())
	}

	sort.Slice(s3CategoryList, func(i, j int) bool {
		return s3CategoryList[i].Weight > s3CategoryList[j].Weight
	})

	return s3CategoryList, int64(len(s3CategoryList)), nil
}

func (h *MarketHandler) GetBannersFromS3(ctx common.Ctx, baseTokenType uint8) ([]*S3BannerInfo, int64, error) {

	var key string
	switch baseTokenType {
	case BaseTokenTypePoints:
		key = S3MarketBannersDefaultKey
	case BaseTokenTypeUsdc:
		key = S3MarketBannersUsdcKey
	default:
		key = S3MarketBannersDefaultKey
	}
	data, _, err := h.marketRepo.DownloadFileFromAdminBucketS3(ctx, key)
	if err != nil {
		return nil, 0, errors.New(int(marketcenterPb.ErrorCode_S3), "S3_ERROR", err.Error())
	}

	s3BannerList := S3BannerList{}
	err = json.Unmarshal(data, &s3BannerList)
	if err != nil {
		return nil, 0, errors.New(int(marketcenterPb.ErrorCode_S3), "S3_ERROR", err.Error())
	}

	return s3BannerList, int64(len(s3BannerList)), nil
}

func (h *MarketHandler) GetSectionsFromS3(ctx common.Ctx, baseTokenType uint8) ([]*S3SectionInfo, int64, error) {

	var key string
	switch baseTokenType {
	case BaseTokenTypePoints:
		key = S3MarketSectionsDefaultKey
	case BaseTokenTypeUsdc:
		key = S3MarketSectionsUsdcKey
	default:
		key = S3MarketSectionsDefaultKey
	}
	data, _, err := h.marketRepo.DownloadFileFromAdminBucketS3(ctx, key)
	if err != nil {
		return nil, 0, errors.New(int(marketcenterPb.ErrorCode_S3), "S3_ERROR", err.Error())
	}

	s3SectionList := S3SectionList{}
	err = json.Unmarshal(data, &s3SectionList)
	if err != nil {
		return nil, 0, errors.New(int(marketcenterPb.ErrorCode_S3), "S3_ERROR", err.Error())
	}

	return s3SectionList, int64(len(s3SectionList)), nil
}

func (h *MarketHandler) UpdateMarketInfoByS3Data(ctx common.Ctx, marketAddress string) error {

	marketEntity, err := h.marketRepo.GetMarket(ctx, &MarketQuery{
		Address: marketAddress,
	})
	if err != nil {
		return errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}
	if marketEntity == nil || marketEntity.Address != marketAddress {
		return errors.New(int(marketcenterPb.ErrorCode_MARKET_NOT_FOUND), "MARKET_NOT_FOUND", "market not found")
	}

	optionEntities, err := h.marketRepo.GetOptions(ctx, &OptionQuery{
		MarketAddress: marketAddress,
	})
	if err != nil {
		return errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}

	data, _, err := h.marketRepo.DownloadFileFromAdminBucketS3(ctx, marketAddress)
	if err != nil {
		return errors.New(int(marketcenterPb.ErrorCode_S3), "S3_ERROR", err.Error())
	}
	ctx.Log.Infof("DownloadFileFromAdminBucketS3: %s", string(data))
	marketS3Info := S3PredictionInfo{}
	err = json.Unmarshal(data, &marketS3Info)
	if err != nil {
		return errors.New(int(marketcenterPb.ErrorCode_S3), "S3_ERROR", err.Error())
	}
	ctx.Log.Infof("Unmarshal: %+v", marketS3Info)

	tags := make([]string, 0, len(marketS3Info.Tags))
	for _, tag := range marketS3Info.Tags {
		if tag.Id != "" {
			tags = append(tags, tag.Id)
		}
	}
	categories := make([]string, 0, len(marketS3Info.Categories))
	for _, category := range marketS3Info.Categories {
		if category.Id != "" {
			categories = append(categories, category.Id)
		}
	}

	updateMarketEntity := &MarketEntity{
		Address:     marketAddress,
		Tags:        tags,
		Categories:  categories,
		Description: marketS3Info.Description,
		Rules:       marketS3Info.Rules,
		RulesUrl:    marketS3Info.RulesFileUrl,
		PicUrl:      marketS3Info.ImageFileUrl,
		IsShow: func() uint8 {
			if marketS3Info.IsDeleted {
				return MarketHide
			}
			return MarketShow
		}(),
	}

	err = h.marketRepo.UpdateMarket(ctx, updateMarketEntity)
	if err != nil {
		ctx.Log.Errorf("UpdateMarketInfoByS3Data UpdateMarket error: %+v", err)
		return errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
	}

	for _, oneS3Option := range marketS3Info.Options {

		for _, optionEntity := range optionEntities {

			if optionEntity.Index == uint32(oneS3Option.Index) {
				err = h.marketRepo.UpdateOption(ctx, &OptionEntity{
					Address: optionEntity.Address,
					PicUrl:  oneS3Option.ImageFileUrl,
				})
				if err != nil {
					ctx.Log.Errorf("UpdateMarketInfoByS3Data UpdateOption error: %+v", err)
					return errors.New(int(marketcenterPb.ErrorCode_DATABASE), "DATABASE_ERROR", err.Error())
				}
			}

		}
	}

	return nil
}
