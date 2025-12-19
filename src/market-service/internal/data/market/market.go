package market

import (
	"encoding/json"
	"fmt"
	recommendationv1 "market-proto/proto/recommendation/v1"
	marketBiz "market-service/internal/biz/market"
	"market-service/internal/data/base"
	marketModel "market-service/internal/model/marketcenter/market"
	"market-service/internal/pkg/common"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type marketRepo struct {
	base.MarketcenterInfra
}

// NewMarketRepo .
func NewMarketRepo(infra base.MarketcenterInfra) marketBiz.MarketRepoInterface {
	return &marketRepo{
		infra,
	}
}

func (r *marketRepo) CreateMarket(ctx common.Ctx, marketEntity *marketBiz.MarketEntity) error {
	marketModel := &marketModel.Market{}
	marketModel.FromEntity(marketEntity)
	return r.Create(ctx, marketModel)
}

func (r *marketRepo) CreateOption(ctx common.Ctx, optionEntity *marketBiz.OptionEntity) error {
	optionModel := &marketModel.Option{}
	optionModel.FromEntity(optionEntity)
	return r.Create(ctx, optionModel)
}

func (r *marketRepo) GetMarket(ctx common.Ctx, query *marketBiz.MarketQuery) (*marketBiz.MarketEntity, error) {
	db := query.Condition(r.GetDb().WithContext(ctx.Ctx).Model(&marketModel.Market{}), nil)
	market := &marketModel.Market{}
	if err := db.First(&market).Error; err != nil && err != gorm.ErrRecordNotFound {
		ctx.Log.Errorf("GetMarket sql failed, err: %v", err)
		return nil, err
	}
	return market.ToEntity(), nil
}

func (r *marketRepo) GetMarkets(ctx common.Ctx, query *marketBiz.MarketQuery) ([]*marketBiz.MarketEntity, error) {
	db := query.Condition(r.GetDb().WithContext(ctx.Ctx).Model(&marketModel.Market{}), nil)
	var markets []*marketModel.Market
	ctx.Log.Debugf("GetMarkets market query: %v", query)

	if err := db.Find(&markets).Error; err != nil {
		ctx.Log.Errorf("GetMarkets sql failed, err: %v", err)
		return nil, err
	}
	ctx.Log.Debugf("GetMarkets markets: %v", markets)

	marketEntities := make([]*marketBiz.MarketEntity, 0, len(markets))
	for _, market := range markets {
		ctx.Log.Debugf("GetMarkets market: %v", market)
		marketEntities = append(marketEntities, market.ToEntity())
	}
	return marketEntities, nil
}

func (r *marketRepo) GetMarketsWithTotal(ctx common.Ctx, query *marketBiz.MarketQuery) ([]*marketBiz.MarketEntity, int64, error) {
	var total int64
	db := query.Condition(r.GetDb().WithContext(ctx.Ctx).Model(&marketModel.Market{}), &total)
	var markets []*marketModel.Market
	if err := db.Find(&markets).Error; err != nil {
		ctx.Log.Errorf("GetMarketsWithTotal sql failed, err: %v", err)
		return nil, 0, err
	}
	marketEntities := make([]*marketBiz.MarketEntity, 0, len(markets))
	for _, market := range markets {
		marketEntities = append(marketEntities, market.ToEntity())
	}
	return marketEntities, total, nil
}

func (r *marketRepo) GetOptions(ctx common.Ctx, query *marketBiz.OptionQuery) ([]*marketBiz.OptionEntity, error) {
	db := query.Condition(r.GetDb().WithContext(ctx.Ctx).Model(&marketModel.Option{}), nil)
	var options []*marketModel.Option
	if err := db.Find(&options).Error; err != nil {
		ctx.Log.Errorf("GetOptions sql failed, err: %v", err)
		return nil, err
	}
	optionEntities := make([]*marketBiz.OptionEntity, 0, len(options))
	for _, option := range options {
		optionEntities = append(optionEntities, option.ToEntity())
	}
	return optionEntities, nil
}

func (r *marketRepo) GetMarketsAndOptionsFromCache(ctx common.Ctx, marketAddressList []string) ([]*marketBiz.MarketEntity, error) {
	// 从缓存中获取市场数据
	cmd := r.GetRedis().MGet(ctx.Ctx, marketAddressList...)
	if cmd.Err() != nil {
		return nil, cmd.Err()
	}

	// 解析缓存数据
	marketEntities := make([]*marketBiz.MarketEntity, 0)
	for _, val := range cmd.Val() {
		if val == nil {
			marketEntities = append(marketEntities, nil)
			continue
		}

		marketEntity := &marketBiz.MarketEntity{}
		if err := json.Unmarshal([]byte(val.(string)), marketEntity); err != nil {
			ctx.Log.Errorf("GetMarketsAndOptionsFromCache unmarshal failed, err: %v", err)
			return nil, err
		}
		marketEntities = append(marketEntities, marketEntity)
	}

	return marketEntities, nil
}

func (r *marketRepo) SetMarketsAndOptionsToCache(ctx common.Ctx, marketEntityList []*marketBiz.MarketEntity) error {
	keyAndValues := make([]interface{}, 0, len(marketEntityList))
	for _, marketEntity := range marketEntityList {
		jsonData, err := json.Marshal(marketEntity)
		if err != nil {
			ctx.Log.Errorf("SetMarketsAndOptionsToCache marshal failed, err: %v", err)
			return err
		}
		keyAndValues = append(keyAndValues, marketEntity.Address, jsonData)
	}
	if err := r.GetRedis().MSet(ctx.Ctx, keyAndValues...).Err(); err != nil {
		ctx.Log.Errorf("SetMarketsAndOptionsToCache mset failed, err: %v", err)
		return err
	}
	return nil
}

func (r *marketRepo) BatchCreateMarketsAndOptions(ctx common.Ctx, marketEntityList []*marketBiz.MarketEntity) error {
	db := common.GetDB(ctx.Ctx, r.GetDb())

	marketModels := make([]*marketModel.Market, 0, len(marketEntityList))
	optionModels := make([]*marketModel.Option, 0)

	for _, marketEntity := range marketEntityList {
		marketModelSt := &marketModel.Market{}
		marketModelSt.FromEntity(marketEntity)
		marketModels = append(marketModels, marketModelSt)

		for _, option := range marketEntity.Options {
			optionModel := &marketModel.Option{}
			optionModel.FromEntity(option)
			optionModels = append(optionModels, optionModel)
		}
	}

	if err := db.
		Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "address"}},
			DoNothing: true,
		}).CreateInBatches(marketModels, 100).Error; err != nil {
		ctx.Log.Errorf("BatchCreateMarketsAndOptions create market failed, err: %v", err)
		return err
	}

	if err := db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "address"}},
		DoNothing: true,
	}).CreateInBatches(optionModels, 100).Error; err != nil {
		ctx.Log.Errorf("BatchCreateMarketsAndOptions create option failed, err: %v", err)
		return err
	}

	return nil
}

func (r *marketRepo) BatchCreateOptionTokenPrice(ctx common.Ctx, optionTokenPriceEntityList []*marketBiz.OptionTokenPriceEntity) error {
	db := common.GetDB(ctx.Ctx, r.GetDb())
	optionTokenPriceModels := make([]*marketModel.OptionTokenPrice, 0, len(optionTokenPriceEntityList))
	for _, optionTokenPriceEntity := range optionTokenPriceEntityList {
		optionTokenPriceModel := &marketModel.OptionTokenPrice{}
		optionTokenPriceModel.FromEntity(optionTokenPriceEntity)
		optionTokenPriceModels = append(optionTokenPriceModels, optionTokenPriceModel)
	}
	if err := db.
		Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "token_address"}, {Name: "block_number"}},
			DoNothing: true,
		}).
		CreateInBatches(optionTokenPriceModels, 100).Error; err != nil {
		ctx.Log.Errorf("BatchCreateOptionTokenPrice create option token price failed, err: %v", err)
		return err
	}
	return nil
}

func (r *marketRepo) UpdateMarketByAddress(ctx common.Ctx, marketEntity *marketBiz.MarketEntity, updateMap map[string]interface{}) error {
	db := common.GetDB(ctx.Ctx, r.GetDb())
	if err := db.Model(&marketModel.Market{}).Where("address = ?", marketEntity.Address).Updates(updateMap).Error; err != nil {
		ctx.Log.Errorf("UpdateMarketByAddress update market failed, err: %v", err)
		return err
	}
	return nil
}

func (r *marketRepo) UpdateMarket(ctx common.Ctx, marketEntity *marketBiz.MarketEntity) error {
	db := common.GetDB(ctx.Ctx, r.GetDb())
	mModel := &marketModel.Market{}
	mModel.FromEntity(marketEntity)
	return db.Model(&mModel).Where("address = ?", marketEntity.Address).Updates(mModel).Error
}

func (r *marketRepo) UpdateOption(ctx common.Ctx, optionEntity *marketBiz.OptionEntity) error {
	db := common.GetDB(ctx.Ctx, r.GetDb())
	oModel := &marketModel.Option{}
	oModel.FromEntity(optionEntity)
	return db.Model(&oModel).Where("address = ?", optionEntity.Address).Updates(oModel).Error
}

func (r *marketRepo) GetUserMarketFollow(ctx common.Ctx, query *marketBiz.UserMarketFollowQuery) (*marketBiz.UserMarketFollowEntity, error) {
	db := query.Condition(r.GetDb().WithContext(ctx.Ctx).Model(&marketModel.UserMarketFollow{}), nil)
	userMarketFollow := &marketModel.UserMarketFollow{}
	if err := db.First(&userMarketFollow).Error; err != nil && err != gorm.ErrRecordNotFound {
		ctx.Log.Errorf("GetUserMarketFollow sql failed, err: %v", err)
		return nil, err
	}
	return userMarketFollow.ToEntity(), nil
}

func (r *marketRepo) GetUserMarketFollows(ctx common.Ctx, userMarketFollowQuery *marketBiz.UserMarketFollowQuery) ([]*marketBiz.UserMarketFollowEntity, error) {
	db := userMarketFollowQuery.Condition(r.GetDb().WithContext(ctx.Ctx).Model(&marketModel.UserMarketFollow{}), nil)
	var userMarketFollows []*marketModel.UserMarketFollow
	if err := db.Find(&userMarketFollows).Error; err != nil {
		ctx.Log.Errorf("GetUserMarketFollows sql failed, err: %v", err)
		return nil, err
	}

	userMarketFollowEntities := make([]*marketBiz.UserMarketFollowEntity, 0, len(userMarketFollows))
	for _, userMarketFollow := range userMarketFollows {
		userMarketFollowEntities = append(userMarketFollowEntities, userMarketFollow.ToEntity())
	}
	return userMarketFollowEntities, nil
}

func (r *marketRepo) GetUserMarketFollowsWithTotal(ctx common.Ctx, query *marketBiz.UserMarketFollowQuery) ([]*marketBiz.UserMarketFollowEntity, int64, error) {
	var total int64
	db := query.Condition(r.GetDb().WithContext(ctx.Ctx).Model(&marketModel.UserMarketFollow{}), &total)
	var userMarketFollows []*marketModel.UserMarketFollow
	if err := db.Find(&userMarketFollows).Error; err != nil {
		ctx.Log.Errorf("GetUserMarketFollowsWithTotal sql failed, err: %v", err)
		return nil, 0, err
	}
	userMarketFollowEntities := make([]*marketBiz.UserMarketFollowEntity, 0, len(userMarketFollows))
	for _, userMarketFollow := range userMarketFollows {
		userMarketFollowEntities = append(userMarketFollowEntities, userMarketFollow.ToEntity())
	}
	return userMarketFollowEntities, total, nil
}

func (r *marketRepo) CreateOrUpdateUserMarketFollow(ctx common.Ctx, userMarketFollowEntity *marketBiz.UserMarketFollowEntity) error {
	userMarketFollowModel := &marketModel.UserMarketFollow{}
	userMarketFollowModel.FromEntity(userMarketFollowEntity)
	db := common.GetDB(ctx.Ctx, r.GetDb())
	return db.Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: "uid"},
			{Name: "market_address"}, // 联合唯一的多个字段
		},
		DoUpdates: clause.AssignmentColumns([]string{"status"}), // 要更新的字段
	}).Create(&userMarketFollowModel).Error
}

func (r *marketRepo) CreateOptionTokenPrice(ctx common.Ctx, optionTokenPriceEntity *marketBiz.OptionTokenPriceEntity) error {
	optionTokenPriceModel := &marketModel.OptionTokenPrice{}
	optionTokenPriceModel.FromEntity(optionTokenPriceEntity)
	return r.Create(ctx, optionTokenPriceModel)
}

func (r *marketRepo) GetLatestOptionPrice(ctx common.Ctx, optionAddressList []string) ([]*marketBiz.OptionTokenPriceEntity, error) {
	db := common.GetDB(ctx.Ctx, r.GetDb())
	var allPriceList []*marketModel.OptionTokenPrice

	// 批量处理，每批20个地址
	const batchSize = 20
	total := len(optionAddressList)

	for i := 0; i < total; i += batchSize {
		end := i + batchSize
		if end > total {
			end = total
		}

		var batchPriceList []*marketModel.OptionTokenPrice
		err := db.Model(&marketModel.OptionTokenPrice{}).
			Select("DISTINCT ON (token_address) *").
			Where("token_address IN (?)", optionAddressList[i:end]).
			Order("token_address, block_time DESC").
			Find(&batchPriceList).Error
		if err != nil {
			ctx.Log.Errorf("GetLatestOptionPrice sql failed for batch %d-%d, err: %v", i, end, err)
			return nil, err
		}
		allPriceList = append(allPriceList, batchPriceList...)
	}

	optionTokenPriceEntities := make([]*marketBiz.OptionTokenPriceEntity, 0, len(allPriceList))
	for _, price := range allPriceList {
		optionTokenPriceEntities = append(optionTokenPriceEntities, price.ToEntity())
	}
	return optionTokenPriceEntities, nil
}

func (r *marketRepo) UpdateMarketVolumeAndParticipants(ctx common.Ctx, marketEntity *marketBiz.MarketEntity) error {
	db := common.GetDB(ctx.Ctx, r.GetDb())
	marketModel := &marketModel.Market{}
	return db.Model(marketModel).Where("address = ?", marketEntity.Address).
		Updates(map[string]interface{}{
			"volume":             gorm.Expr(marketModel.TableName()+".volume + ?", marketEntity.Volume),
			"participants_count": gorm.Expr(marketModel.TableName()+".participants_count + ?", marketEntity.ParticipantsCount),
		}).Error
}

func (r *marketRepo) GetMarketOptionPriceHistory(ctx common.Ctx, tokenAddresses []string, isMarketRunning bool, timeRange string) ([]*marketBiz.TokenPricePoint, error) {
	var fromTime, toTime time.Time
	var interval string

	db := common.GetDB(ctx.Ctx, r.GetDb())

	toTime = time.Now()
	// 查询全部时间且市场不在运行时，使用最后一个数据点的时间
	if timeRange == "all" && !isMarketRunning {
		if err := db.Model(&marketModel.OptionTokenPrice{}).
			Where("token_address in ?", tokenAddresses).
			Order("block_time DESC").
			Limit(1).
			Pluck("block_time", &toTime).Error; err != nil {
			return nil, err
		}
	}
	// 根据不同时间范围使用合适的间隔
	switch timeRange {
	case "1h":
		fromTime = toTime.Add(-1 * time.Hour)
		interval = "minute"
	case "6h":
		fromTime = toTime.Add(-6 * time.Hour)
		interval = "minute"
	case "1d":
		fromTime = toTime.Add(-24 * time.Hour)
		interval = "minute"
	case "1w":
		fromTime = toTime.Add(-7 * 24 * time.Hour)
		interval = "hour"
	case "all":
		if err := db.Model(&marketModel.OptionTokenPrice{}).
			Where("token_address in ?", tokenAddresses).
			Order("block_time ASC").
			Limit(1).
			Pluck("block_time", &fromTime).Error; err != nil {
			return nil, err
		}
		// 根据时间跨度选择合适的间隔
		duration := toTime.Sub(fromTime)
		switch {
		case duration <= 24*time.Hour: // 一天 每分钟一个点 最多1440个点
			interval = "minute"
		case duration <= 2*30*24*time.Hour: // 两个月 每小时一个点 最多1440个点
			interval = "hour"
		case duration <= 2*365*24*time.Hour: // 两年 每天一个点 最多730个点
			interval = "day"
		case duration <= 15*365*24*time.Hour: // 20年 每周一个点 最多大概1000个点
			interval = "week"
		default:
			interval = "month"
		}
	default:
		return nil, fmt.Errorf("invalid time range: %s", timeRange)
	}

	query := `WITH sample_times AS (
            SELECT 
                date_trunc('%s', block_time) as timestamp,
                token_address,
                LAST_VALUE(price) OVER (
                    PARTITION BY date_trunc('%s', block_time), token_address
                    ORDER BY block_time
                ) as price
            FROM t_option_token_price
            WHERE token_address in (?)
                AND block_time >= ?
                AND block_time <= ?
        )
        SELECT 
            timestamp,
            jsonb_object_agg(token_address, price::text) as token_prices
        FROM sample_times
        GROUP BY timestamp
        ORDER BY timestamp
    `
	formattedQuery := fmt.Sprintf(query, interval, interval)

	type TokenPricePointRaw struct {
		Timestamp   time.Time `gorm:"column:timestamp"`
		TokenPrices []byte    `gorm:"column:token_prices"` // 先接收为[]byte
	}

	var rawResults []TokenPricePointRaw
	// 确保时间参数在SQL中也带有时区信息
	fromTimeStr := fromTime.Format("2006-01-02 15:04:05 +0800")
	toTimeStr := toTime.Format("2006-01-02 15:04:05 +0800")
	err := db.Raw(formattedQuery, tokenAddresses, fromTimeStr, toTimeStr).Scan(&rawResults).Error
	if err != nil {
		ctx.Log.Errorf("GetMarketOptionPriceHistory query failed: %v", err)
		return nil, err
	}
	// 如果查询结果为空，查询不带时间范围的最后一个点，保证一定能返回数据
	if len(rawResults) == 0 {
		latestPrices, err := r.GetLatestOptionPrice(ctx, tokenAddresses)
		if err != nil {
			ctx.Log.Errorf("GetMarketOptionPriceHistory fallback query failed: %v", err)
			return nil, err
		}

		if len(latestPrices) == 0 {
			return []*marketBiz.TokenPricePoint{}, nil
		}

		// 构造返回数据
		tokenPrices := make(map[string]string, len(latestPrices))
		latestTimestamp := latestPrices[0].BlockTime

		for _, price := range latestPrices {
			tokenPrices[price.TokenAddress] = price.Price.String()
			if price.BlockTime.After(latestTimestamp) {
				latestTimestamp = price.BlockTime
			}
		}

		return []*marketBiz.TokenPricePoint{{
			Timestamp:   latestTimestamp,
			TokenPrices: tokenPrices,
		}}, nil
	}

	result := make([]*marketBiz.TokenPricePoint, len(rawResults))
	for i, raw := range rawResults {
		tokenPrices := make(map[string]string)
		if err := json.Unmarshal(raw.TokenPrices, &tokenPrices); err != nil {
			ctx.Log.Errorf("Failed to unmarshal token prices: %v", err)
			return nil, err
		}

		result[i] = &marketBiz.TokenPricePoint{
			Timestamp:   raw.Timestamp,
			TokenPrices: tokenPrices,
		}
	}

	return result, nil
}

func (r *marketRepo) GetMarketTagsWithTotal(ctx common.Ctx, query *marketBiz.MarketTagQuery) ([]*marketBiz.TagEntity, int64, error) {
	var total int64
	db := query.Condition(r.GetDb().WithContext(ctx.Ctx).Model(&marketModel.Tag{}), &total)
	var tags []*marketModel.Tag
	if err := db.Find(&tags).Error; err != nil {
		ctx.Log.Errorf("GetMarketTagsWithTotal sql failed, err: %v", err)
		return nil, 0, err
	}
	tagEntities := make([]*marketBiz.TagEntity, 0, len(tags))
	for _, tag := range tags {
		tagEntities = append(tagEntities, tag.ToEntity())
	}
	return tagEntities, total, nil
}

func (r *marketRepo) GetMarketTagsOrderByMarketCount(ctx common.Ctx, query *marketBiz.MarketTagQuery) ([]*marketBiz.TagEntity, int64, error) {
	offset := query.Offset
	limit := query.Limit

	db := common.GetDB(ctx.Ctx, r.GetDb())
	// 先查询总数
	var total int64
	if err := db.Model(&marketModel.Tag{}).Count(&total).Error; err != nil {
		ctx.Log.Errorf("GetMarketTagsOrderByMarketCount count total failed, err: %v", err)
		return nil, 0, err
	}
	if total == 0 {
		return nil, 0, nil
	}

	sql := fmt.Sprintf(`
	SELECT 
		t.tag_name,
		COUNT(DISTINCT m.address) as market_count
	FROM 
		t_tag t
		LEFT JOIN t_market m ON m.tags ? t.tag_name 
	GROUP BY 
		t.tag_name order by market_count desc offset %d limit %d
	`, offset, limit)

	var tagEntities []*marketBiz.TagEntity
	if err := db.Raw(sql).Scan(&tagEntities).Error; err != nil {
		ctx.Log.Errorf("GetMarketTagsOrderByMarketCount sql failed, err: %v", err)
		return nil, 0, err
	}

	return tagEntities, total, nil
}

func (r *marketRepo) CreateMarketTag(ctx common.Ctx, tag string) error {
	db := common.GetDB(ctx.Ctx, r.GetDb())
	return db.Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: "tag_name"},
		},
		DoNothing: true,
	}).Create(&marketModel.Tag{TagName: tag}).Error
}

func (r *marketRepo) BatchCreateMarketTags(ctx common.Ctx, tags []string) error {
	db := common.GetDB(ctx.Ctx, r.GetDb())

	// 构建 Tag 切片
	tagModels := make([]*marketModel.Tag, len(tags))
	for i, tag := range tags {
		tagModels[i] = &marketModel.Tag{TagName: tag}
	}

	return db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "tag_name"}},
		DoNothing: true,
	}).CreateInBatches(tagModels, 100).Error
}

func (r *marketRepo) GetTagEmbeddingFromCache(ctx common.Ctx, tagList []string) ([]float64, error) {
	key := "embedding-tags-" + strings.Join(tagList, "-")
	cmd := r.GetRedis().Get(ctx.Ctx, key)
	if cmd.Err() != nil {
		return nil, cmd.Err()
	}
	embedding := make([]float64, 0)
	if err := json.Unmarshal([]byte(cmd.Val()), &embedding); err != nil {
		return nil, err
	}
	return embedding, nil
}

func (r *marketRepo) SetTagEmbeddingToCache(ctx common.Ctx, tagList []string, embedding []float64) error {
	key := "embedding-tags-" + strings.Join(tagList, "-")
	embeddingBytes, err := json.Marshal(embedding)
	if err != nil {
		return err
	}
	return r.GetRedis().Set(ctx.Ctx, key, string(embeddingBytes), 0).Err()
}

func (r *marketRepo) EmbeddingTags(ctx common.Ctx, tags []string) ([]float64, error) {
	recommendationClient := r.GetRpcClient().RecommendationClient
	if recommendationClient == nil {
		return nil, fmt.Errorf("recommendation client is nil")
	}

	req := &recommendationv1.EmbeddingRequest{
		Pattern: []string{"@tags@"},
		Data:    make([]*anypb.Any, 0, len(tags)),
	}

	embeddingTagsData, err := structpb.NewStruct(map[string]interface{}{
		"id":   "embedding-tags-" + strings.Join(tags, "-"),
		"tags": strings.Join(tags, ","),
	})
	if err != nil {
		ctx.Log.Errorf("EmbeddingTagsData to structpb failed, err: %v", err)
		return nil, err
	}
	data, err := anypb.New(embeddingTagsData)
	if err != nil {
		ctx.Log.Errorf("EmbeddingTagsData to anypb failed, err: %v", err)
		return nil, err
	}
	req.Data = append(req.Data, data)
	resp, err := recommendationClient.Embedding(ctx.Ctx, req)
	if err != nil {
		ctx.Log.Errorf("Embedding failed, err: %v", err)
		return nil, err
	}
	if len(resp.Res) == 0 {
		return nil, fmt.Errorf("embedding response is empty")
	}
	embedding := make([]float64, 0, len(resp.Res[0].Embedding))
	for _, v := range resp.Res[0].Embedding {
		embedding = append(embedding, float64(v))
	}
	return embedding, nil
}

func (r *marketRepo) EmbeddingMarketInfo(ctx common.Ctx, marketEntityList []*marketBiz.MarketEntity) ([]*marketBiz.MarketEntity, error) {
	// 获取RPC客户端
	recommendationClient := r.GetRpcClient().RecommendationClient
	if recommendationClient == nil {
		return nil, fmt.Errorf("recommendation client is nil")
	}
	// 调用推荐系统的Embedding服务
	req := &recommendationv1.EmbeddingRequest{
		Pattern: []string{"@title@ | @description@ | @tags@"},
		Data:    make([]*anypb.Any, 0, len(marketEntityList)),
	}

	marketEntityMap := make(map[string]*marketBiz.MarketEntity)
	for _, market := range marketEntityList {
		marketEntityMap[market.Address] = market

		tags := strings.Join(market.Tags, ",")
		embeddingData, err := structpb.NewStruct(map[string]interface{}{
			"id":          market.Address,
			"title":       market.Name,
			"description": market.Description,
			"tags":        tags,
		})
		if err != nil {
			ctx.Log.Errorf("EmbeddingData to structpb failed, err: %v", err)
			return nil, err
		}
		data, err := anypb.New(embeddingData)
		if err != nil {
			ctx.Log.Errorf("Embedding data to anypb failed, err: %v", err)
			return nil, err
		}
		req.Data = append(req.Data, data)
	}
	resp, err := recommendationClient.Embedding(ctx.Ctx, req)
	if err != nil {
		ctx.Log.Errorf("Embedding failed, err: %v", err)
		return nil, err
	}

	ctx.Log.Infof("Embedding response: %v", resp)

	for _, result := range resp.Res {
		if marketEntity, ok := marketEntityMap[result.Id]; ok {
			embedding := make([]float64, 0, len(result.Embedding))
			for _, v := range result.Embedding {
				embedding = append(embedding, float64(v))
			}
			marketEntity.Embedding = embedding
		}
	}
	return marketEntityList, nil
}

func (r *marketRepo) UpdateMarketsEmbedding(ctx common.Ctx, marketEntityList []*marketBiz.MarketEntity) error {
	if len(marketEntityList) == 0 {
		return nil
	}

	// 构建批量更新数据
	marketModels := make([]*marketModel.Market, 0, len(marketEntityList))
	for _, market := range marketEntityList {
		m := &marketModel.Market{}
		m.FromEntity(market)
		marketModels = append(marketModels, m)
	}

	db := common.GetDB(ctx.Ctx, r.GetDb()).Model(&marketModel.Market{})
	return db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "address"}},
		DoUpdates: clause.AssignmentColumns([]string{"embedding"}),
	}).CreateInBatches(marketModels, 100).Error
}
