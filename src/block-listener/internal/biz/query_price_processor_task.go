package biz

import (
	"block-listener/internal/conf"
	"block-listener/internal/data"
	"block-listener/pkg/alarm"
	com "block-listener/pkg/common"
	"context"
	"fmt"
	rpc "market-proto/proto/market-service/marketcenter/v1"
	"time"

	"github.com/go-kratos/kratos/v2/log"
)

// QueryPriceProcessor 价格查询处理器
type QueryPriceProcessor struct {
	db          *data.DbClient
	arbClient   *data.ArbClient
	redisClient *data.RedisClient
	rpcClient   *data.RpcClient
	bc          *conf.Bootstrap
	log         log.Logger
}

// NewQueryPriceProcessor 创建价格查询处理器
func NewQueryPriceProcessor(
	db *data.DbClient,
	arbClient *data.ArbClient,
	redisClient *data.RedisClient,
	rpcClient *data.RpcClient,
	bc *conf.Bootstrap,
	logger log.Logger,
) *QueryPriceProcessor {
	return &QueryPriceProcessor{
		db:          db,
		arbClient:   arbClient,
		redisClient: redisClient,
		rpcClient:   rpcClient,
		bc:          bc,
		log:         logger,
	}
}

// Name 返回任务名称
func (p *QueryPriceProcessor) Name() string {
	return "query_price_processor"
}

// Timeout 返回任务的超时时间
func (p *QueryPriceProcessor) Timeout() time.Duration {
	return 30 * time.Minute
}

// RedisLockTimeOut 返回任务的redis锁超时时间
func (p *QueryPriceProcessor) RedisLockTimeOut() time.Duration {
	return 30 * time.Minute
}

// Run 执行任务
func (p *QueryPriceProcessor) Run(ctx context.Context) error {

	c := com.NewBaseCtx(ctx, p.log)
	c.Log.Info("query_price_processor processing...")

	// 获取最新区块号
	latestBlockNumber, err := p.arbClient.GetLatestBlockNumber(ctx)
	if err != nil {
		c.Log.Errorf("get latest block number failed, err: %v", err)
		return err
	}
	blockTimestamps, err := p.arbClient.BatchQueryBlockTimestamps(ctx, []uint64{latestBlockNumber})
	if err != nil {
		c.Log.Errorf("get block timestamp failed, err: %v", err)
		return err
	}
	blockTimestamp, ok := blockTimestamps[latestBlockNumber]
	if !ok {
		c.Log.Errorf("get block timestamp failed, block number: %d", latestBlockNumber)
		return fmt.Errorf("get block timestamp failed, block number: %d", latestBlockNumber)
	}

	var page uint32 = 1
	var pageSize uint32 = 20
	totalProcessed := 0

	for {
		marketCounts, err := p.processMarketsPage(c, page, pageSize, latestBlockNumber, blockTimestamp)
		if err != nil {
			c.Log.Errorf("processMarketsPage failed, page: %d, err: %v", page, err)
			alarm.Lark.Send(fmt.Sprintf("processMarketsPage failed, total processed: %d, page: %d, err: %v", totalProcessed, page, err))
			return err
		}

		if marketCounts == 0 {
			break
		}

		totalProcessed += marketCounts
		page++

		// 如果返回的数据量小于pageSize，说明已经是最后一页
		if marketCounts < int(pageSize) {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}

	c.Log.Infof("query_price_processor completed, total processed: %d", totalProcessed)
	return nil
}

func (p *QueryPriceProcessor) processMarketsPage(ctx com.Ctx, page, pageSize uint32, latestBlockNumber uint64, blockTimestamp uint64) (int, error) {
	GetMarketsInfoRsp, err := p.rpcClient.MarketcenterClient.GetMarketsAndOptionsInfo(ctx.Ctx, &rpc.GetMarketsAndOptionsInfoRequest{
		Page:          page,
		PageSize:      pageSize,
		Status:        1,
		SortType:      rpc.GetMarketsAndOptionsInfoRequest_SORT_TYPE_OLDEST,
		NotQueryPrice: true,
	})
	if err != nil {
		ctx.Log.Errorf("get markets and options info failed, err: %v", err)
		return 0, err
	}

	if len(GetMarketsInfoRsp.Markets) == 0 {
		return 0, nil
	}

	marketAddressToMarketInfoMap := make(map[string]*rpc.GetMarketsAndOptionsInfoResponse_Market)
	queryParams := make([][3]interface{}, 0)
	keyToOptionAddrMap := make(map[string]string)

	for _, market := range GetMarketsInfoRsp.Markets {
		marketAddressToMarketInfoMap[market.Address] = market
		for _, option := range market.Options {
			queryParams = append(queryParams, [3]interface{}{market.Address, option.Index, latestBlockNumber})
			keyToOptionAddrMap[fmt.Sprintf("%s-%d-%d", market.Address, option.Index, latestBlockNumber)] = option.Address
		}
	}

	// 打印查询参数
	ctx.Log.Infof("查询参数:")
	for _, param := range queryParams {
		marketAddr := param[0].(string)
		optionIndex := param[1].(uint32)
		blockNum := param[2].(uint64)

		marketInfo := marketAddressToMarketInfoMap[marketAddr]
		var optionName string
		for _, opt := range marketInfo.Options {
			if opt.Index == optionIndex {
				optionName = opt.Name
				break
			}
		}

		ctx.Log.Infof("市场地址: %s, 期权名称: %s, 期权序号: %d, 区块号: %d",
			marketAddr,
			optionName,
			optionIndex,
			blockNum)
	}

	// 批量查询价格
	pricesMap, err := p.arbClient.BatchQueryOptionPrices(ctx.Ctx, queryParams, latestBlockNumber)
	if err != nil {
		ctx.Log.Errorf("batch query option prices failed, err: %v", err)
		return 0, err
	}

	// 打印价格查询结果
	ctx.Log.Infof("查询到 %d 个期权价格信息:", len(pricesMap))
	for key, priceInfo := range pricesMap {
		marketAddr := priceInfo.PredictionAddr
		optionAddr := keyToOptionAddrMap[key]

		marketInfo := marketAddressToMarketInfoMap[marketAddr]
		var optionName string
		for _, opt := range marketInfo.Options {
			if opt.Address == optionAddr {
				optionName = opt.Name
				break
			}
		}

		ctx.Log.Infof("市场地址: %s, 期权名称: %s, 期权地址: %s, 价格: %s",
			marketAddr,
			optionName,
			optionAddr,
			priceInfo.Price.String())
	}

	// 构建更新价格请求
	updateReq := &rpc.BatchUpdateOptionPriceRequest{
		OptionPrices: make([]*rpc.BatchUpdateOptionPriceRequest_OptionPrice, 0, len(pricesMap)),
	}

	for key, ptionPriceSt := range pricesMap {
		marketAddr := ptionPriceSt.PredictionAddr

		if optionAddr, ok := keyToOptionAddrMap[key]; ok {
			oneOptionPrice := &rpc.BatchUpdateOptionPriceRequest_OptionPrice{
				OptionAddress: optionAddr,
				Price:         ptionPriceSt.Price.String(),
				BlockTime:     blockTimestamp,
				BlockNumber:   latestBlockNumber,
			}

			if marketInfo, ok := marketAddressToMarketInfoMap[marketAddr]; ok {

				oneOptionPrice.Decimal = func() uint32 {
					for _, option := range marketInfo.Options {
						if option.Index == ptionPriceSt.OptionIndex {
							return uint32(option.Decimal)
						}
					}
					return 6
				}()
				oneOptionPrice.BaseTokenType = func() rpc.BaseTokenType {
					if marketInfo.BaseTokenType == rpc.BaseTokenType_BASE_TOKEN_TYPE_USDC {
						return rpc.BaseTokenType_BASE_TOKEN_TYPE_USDC
					}
					return rpc.BaseTokenType_BASE_TOKEN_TYPE_POINTS
				}()
			}

			updateReq.OptionPrices = append(updateReq.OptionPrices, oneOptionPrice)
		}
	}

	// 调用marketcenter更新价格
	if len(updateReq.OptionPrices) > 0 {
		_, err = p.rpcClient.MarketcenterClient.BatchUpdateOptionPrice(ctx.Ctx, updateReq)
		if err != nil {
			ctx.Log.Errorf("batch update option price failed, err: %v", err)
			return 0, err
		}
		ctx.Log.Infof("successfully updated %d option prices", len(updateReq.OptionPrices))
	}

	return len(GetMarketsInfoRsp.Markets), nil
}
