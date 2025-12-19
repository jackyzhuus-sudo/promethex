package biz

import (
	"block-listener/internal/conf"
	"block-listener/internal/contract"
	"block-listener/internal/data"
	"block-listener/internal/model"
	"context"
	"fmt"
	marketcenterPb "market-proto/proto/market-service/marketcenter/v1"
	usercenterPb "market-proto/proto/market-service/usercenter/v1"
	"math/big"
	"runtime/debug"
	"sync"
	"time"

	"block-listener/pkg/alarm"
	com "block-listener/pkg/common"

	"github.com/ethereum/go-ethereum/common"
	"github.com/go-kratos/kratos/v2/log"
)

// 常量定义
const (
	// 事件处理相关
	EventChannelBufferSize = 500

	// 格式化字符串
	BalanceKeyFormat = "%s-%s-%d" // userAddr-tokenAddr-blockNumber
	PriceKeyFormat   = "%s-%d-%d" // marketAddr-optionIndex-blockNumber

	// 日志消息
	MsgUserNotFound   = "Status. 过滤 用户不存在: %s"
	MsgMarketNotFound = "Status. 过滤 市场不存在: %s"
	MsgEventIgnored   = "过滤 丢弃事件: %s"
)

// EventProcessor 事件处理器任务
type EventProcessor struct {
	db *data.DbClient

	arbClient   *data.ArbClient
	redisClient *data.RedisClient
	rpcClient   *data.RpcClient
	bc          *conf.Bootstrap
	log         log.Logger
}

// NewEventProcessor 创建事件处理器任务
func NewEventProcessor(
	db *data.DbClient,
	arbClient *data.ArbClient,
	redisClient *data.RedisClient,
	rpcClient *data.RpcClient,
	bc *conf.Bootstrap,
	logger log.Logger,
) *EventProcessor {
	return &EventProcessor{
		db:          db,
		arbClient:   arbClient,
		redisClient: redisClient,
		rpcClient:   rpcClient,
		bc:          bc,
		log:         logger,
	}
}

// Name 返回任务名称
func (p *EventProcessor) Name() string {
	return "event_processor"
}

// Timeout 返回任务的超时时间
func (p *EventProcessor) Timeout() time.Duration {
	return 1 * time.Minute
}

// RedisLockTimeOut 返回任务的redis锁超时时间
func (p *EventProcessor) RedisLockTimeOut() time.Duration {
	return 1 * time.Minute
}

// Run 执行任务
func (p *EventProcessor) Run(ctx context.Context) error {
	c := com.NewBaseCtx(ctx, p.log)

	confirmedBlockNum := p.bc.Data.Blockchain.ConfirmedBlockNum
	// 获取当前区块高度
	blockNumber, err := p.arbClient.GetLatestBlockNumber(ctx)
	if err != nil {
		c.Log.Errorf("get latest block number failed, err: %v", err)
		return err
	}
	endBlockNumber := blockNumber - uint64(confirmedBlockNum)

	eventLogs, err := p.db.GetConfirmedWaitEventLogs(ctx, endBlockNumber, int(p.bc.Data.Blockchain.CronScanDbRows))
	c.Log.Infof("event_processor get %d event logs", len(eventLogs))
	if err != nil {
		c.Log.Errorf("get event logs failed, err: %v", err)
		return err
	}

	// 分离创建市场事件和其他事件
	factoryEvents, operationEvents := p.separateEvents(c, eventLogs)

	// 1. 先处理创建市场事件
	if len(factoryEvents) > 0 {
		c.Log.Infof("处理创建市场事件，数量: %d", len(factoryEvents))

		err := p.handleFactoryEvents(c, factoryEvents, endBlockNumber)
		if err != nil {
			c.Log.Errorf("处理创建市场事件失败: %v", err)
			return err
		}

		c.Log.Infof("创建市场事件处理完成")
	}

	// 2. 再处理其他事件
	if len(operationEvents) > 0 {
		c.Log.Infof("处理市场运营事件，数量: %d", len(operationEvents))

		err := p.handleOperationEvents(c, operationEvents, endBlockNumber)
		if err != nil {
			c.Log.Errorf("处理市场运营事件失败: %v", err)
			return err
		}

		c.Log.Infof("市场运营事件处理完成")
	}

	return nil
}

// EventCategories 事件分类结果
type EventCategories struct {
	FactoryEvents []*model.EventLog
	OtherEvents   []*model.EventLog
}

// separateEvents 分离创建市场事件和运营事件
func (p *EventProcessor) separateEvents(ctx com.Ctx, eventLogs []*model.EventLog) ([]*model.EventLog, []*model.EventLog) {
	factoryEvents := make([]*model.EventLog, 0)
	operationEvents := make([]*model.EventLog, 0)

	for _, eventLog := range eventLogs {
		if eventLog.Removed {
			ctx.Log.Debugf("事件已删除: %s", eventLog.TxHash)
			continue
		}

		switch eventLog.Type {
		case model.TypeFactory:
			if p.isMarketCreationEvent(ctx, eventLog) {
				factoryEvents = append(factoryEvents, eventLog)
			}
		default:
			operationEvents = append(operationEvents, eventLog)
		}
	}

	return factoryEvents, operationEvents
}

// isMarketCreationEvent 检查是否为市场创建事件
func (p *EventProcessor) isMarketCreationEvent(ctx com.Ctx, eventLog *model.EventLog) bool {
	factoryEvent, err := p.arbClient.Contract.FactoryContract.ParseEvent(*eventLog.ToChainLog())
	if err != nil {
		ctx.Log.Errorf("解析工厂事件失败: %v", err)
		return false
	}

	_, ok := factoryEvent.(*contract.PredictionCreatedEvent)
	return ok
}

// ChainData 用于存储链上批量查询结果
type ChainData struct {
	marketAddressToMarketInfo map[string]*contract.PredictionMarketInfo

	userTokenBalances map[string]*data.UserTokenBalance // key: user-token-blockNumber
	optionPrices      map[string]*data.OptionPrice      // key: marketAddr-optionIndex-blockNumber
	blockNumberToTime map[uint64]uint64
}

// ProcessingContext 事件处理上下文
type ProcessingContext struct {
	MarketMap   map[string]*marketcenterPb.GetMarketsAndOptionsForBlockListenerResponse_Market
	UserInfoMap map[string]*usercenterPb.GetUsersInfoByAddressesReply_User
	ChainData   *ChainData
}

// QueryParams 查询参数结构
type QueryParams struct {
	BlockNumberList   []uint64
	UserTokenPairs    [][3]interface{}
	MarketOptionPairs [][3]interface{}
}

// handleFactoryEvents 专门处理创建市场事件
func (p *EventProcessor) handleFactoryEvents(ctx com.Ctx, factoryEvents []*model.EventLog, endBlockNumber uint64) error {
	// 提取预测市场地址和区块号
	creationParams := make([][2]interface{}, 0)
	addrToEvent := make(map[string]*model.EventLog)

	for _, eventLog := range factoryEvents {
		factoryEvent, err := p.arbClient.Contract.FactoryContract.ParseEvent(*eventLog.ToChainLog())
		if err != nil {
			ctx.Log.Errorf("解析工厂事件失败: %v", err)
			continue
		}

		if e, ok := factoryEvent.(*contract.PredictionCreatedEvent); ok {
			marketAddr := e.Prediction.Hex()
			creationParams = append(creationParams, [2]interface{}{marketAddr, e.BlockNumber})
			addrToEvent[marketAddr] = eventLog
			ctx.Log.Debugf("添加预测市场创建参数: address=%s, block number=%d", marketAddr, e.BlockNumber)
		}
	}

	if len(creationParams) == 0 {
		return nil
	}

	// 批量查询市场信息
	marketInfos, err := p.arbClient.BatchQueryPredictionMarkets(ctx.Ctx, creationParams, endBlockNumber)
	if err != nil {
		return fmt.Errorf("批量查询市场信息失败: %w", err)
	}

	// 查询选项信息
	optionParams := make([][2]interface{}, 0)
	addrToInfo := make(map[string]*contract.PredictionMarketInfo)

	for _, marketInfo := range marketInfos {
		marketAddr := marketInfo.Address.Hex()
		addrToInfo[marketAddr] = marketInfo

		for _, optionAddress := range marketInfo.Options {
			optionParams = append(optionParams, [2]interface{}{optionAddress, marketInfo.BlockNumber})
		}
	}

	// 查询选项详情
	optionInfos, err := p.arbClient.BatchQueryOptionInfo(ctx.Ctx, optionParams, endBlockNumber)
	if err != nil {
		return fmt.Errorf("批量查询选项信息失败: %w", err)
	}

	optionPriceParams := make([][3]interface{}, 0)
	optionKeyToOptionInfo := make(map[string]*contract.OptionInfo)
	// 将选项信息关联到市场
	for _, optionInfo := range optionInfos {
		marketAddress := optionInfo.PoolAddress.Hex()
		optionIndex := uint32(optionInfo.Index)
		oneQueryParams := [3]interface{}{marketAddress, optionIndex, endBlockNumber}
		optionPriceParams = append(optionPriceParams, oneQueryParams)
		optionKeyToOptionInfo[fmt.Sprintf(PriceKeyFormat, marketAddress, optionIndex, endBlockNumber)] = optionInfo
		marketInfo, ok := addrToInfo[marketAddress]
		if ok {
			marketInfo.OptionsInfo = append(marketInfo.OptionsInfo, optionInfo)
		}
	}

	err = p.createMarkets(ctx, addrToInfo, addrToEvent)
	if err != nil {
		return fmt.Errorf("创建市场失败: %w", err)
	}

	go func(newCtx com.Ctx) {
		defer func() {
			if e := recover(); e != nil {
				newCtx.Log.Errorf("new market query option price panic error: %v, stack: %s", e, string(debug.Stack()))
			}
		}()

		newCtx.Log.Infof("新市场开始批量查询选项价格")
		optionPrices, err := p.arbClient.BatchQueryOptionPrices(newCtx.Ctx, optionPriceParams, endBlockNumber)
		if err != nil {
			newCtx.Log.Errorf("批量查询选项价格失败: %v", err)
			return
		}

		blockTimeMap, err := p.arbClient.BatchQueryBlockTimestamps(newCtx.Ctx, []uint64{endBlockNumber})
		if err != nil {
			newCtx.Log.Errorf("批量查询区块时间失败: %v", err)
			return
		}

		updateReq := &marketcenterPb.BatchUpdateOptionPriceRequest{
			OptionPrices: make([]*marketcenterPb.BatchUpdateOptionPriceRequest_OptionPrice, 0),
		}
		for _, oneOptionPrice := range optionPrices {
			if optionInfo, ok := optionKeyToOptionInfo[fmt.Sprintf(PriceKeyFormat, oneOptionPrice.PredictionAddr, oneOptionPrice.OptionIndex, oneOptionPrice.BlockNumber)]; ok {
				blocktime, ok := blockTimeMap[oneOptionPrice.BlockNumber]
				if !ok {
					newCtx.Log.Errorf("区块时间不存在: %d", oneOptionPrice.BlockNumber)
					continue
				}

				marketInfo, ok := addrToInfo[oneOptionPrice.PredictionAddr]
				if !ok {
					newCtx.Log.Errorf("市场信息不存在: %s", oneOptionPrice.PredictionAddr)
					continue
				}

				onePricereq := &marketcenterPb.BatchUpdateOptionPriceRequest_OptionPrice{
					OptionAddress: optionInfo.Address.Hex(),
					Price:         oneOptionPrice.Price.String(),
					Decimal:       uint32(optionInfo.Decimals),
					BaseTokenType: func(marketInfo *contract.PredictionMarketInfo) marketcenterPb.BaseTokenType {
						if marketInfo.BaseToken.Hex() == p.bc.Custom.AssetTokens.Usdc.Address {
							return marketcenterPb.BaseTokenType_BASE_TOKEN_TYPE_USDC
						} else if marketInfo.BaseToken.Hex() == p.bc.Custom.AssetTokens.Points.Address {
							return marketcenterPb.BaseTokenType_BASE_TOKEN_TYPE_POINTS
						}
						return marketcenterPb.BaseTokenType_BASE_TOKEN_TYPE_POINTS
					}(marketInfo),
					BlockTime:   blocktime,
					BlockNumber: oneOptionPrice.BlockNumber,
				}
				updateReq.OptionPrices = append(updateReq.OptionPrices, onePricereq)
			}

		}

		if len(updateReq.OptionPrices) > 0 {
			_, err = p.rpcClient.MarketcenterClient.BatchUpdateOptionPrice(newCtx.Ctx, updateReq)
			if err != nil {
				newCtx.Log.Errorf("批量更新选项价格失败: %v", err)
			}
		}
		newCtx.Log.Infof("新市场批量查询选项价格完成")
	}(com.CloneBaseCtx(ctx, p.log))
	return nil
}

// createMarkets 根据工厂事件创建市场
func (p *EventProcessor) createMarkets(
	ctx com.Ctx,
	addrToInfo map[string]*contract.PredictionMarketInfo,
	addrToEvent map[string]*model.EventLog,
) error {
	reqMarket := make([]*marketcenterPb.CreateMarketsAndOptionsRequest_Market, 0)
	processedIds := make([]uint64, 0)

	for marketAddr, marketInfo := range addrToInfo {
		eventLog, exists := addrToEvent[marketAddr]
		if !exists {
			continue
		}

		// 创建选项请求
		reqOptions := make([]*marketcenterPb.CreateMarketsAndOptionsRequest_Option, 0)
		for _, option := range marketInfo.OptionsInfo {
			reqOptions = append(reqOptions, &marketcenterPb.CreateMarketsAndOptionsRequest_Option{
				Name:        option.Name,
				Address:     option.Address.Hex(),
				Symbol:      option.Symbol,
				Description: option.Description,
				Decimal:     uint32(option.Decimals),
				Index:       uint32(option.Index),
			})
		}

		reqMarket = append(reqMarket, &marketcenterPb.CreateMarketsAndOptionsRequest_Market{
			Name:             marketInfo.Description,
			Address:          marketAddr,
			BaseTokenAddress: marketInfo.BaseToken.Hex(),
			OracleAddress:    marketInfo.Oracle.Hex(),
			Deadline:         uint64(marketInfo.AssertTime),
			TxHash:           eventLog.TxHash,
			Options:          reqOptions,
		})

		processedIds = append(processedIds, uint64(eventLog.ID))
	}

	_, err := p.rpcClient.MarketcenterClient.CreateMarketsAndOptions(ctx.Ctx, &marketcenterPb.CreateMarketsAndOptionsRequest{
		Markets: reqMarket,
	})
	if err != nil {
		return fmt.Errorf("创建市场失败: %w", err)
	}

	// 创建市场成功后，立即更新事件状态
	if len(processedIds) > 0 {
		if err := p.db.UpdateEventLogsStatusSucc(ctx.Ctx, processedIds); err != nil {
			ctx.Log.Errorf("更新工厂事件状态失败: %v", err)
			return fmt.Errorf("更新工厂事件状态失败: %w", err)
		}
	}

	return nil
}

// handleOperationEvents 处理市场运营事件
func (p *EventProcessor) handleOperationEvents(ctx com.Ctx, operationEvents []*model.EventLog, endBlockNumber uint64) error {
	// 1. 查询基础数据
	processingCtx, err := p.buildContext(ctx, operationEvents)
	if err != nil {
		return fmt.Errorf("构建处理上下文失败: %w", err)
	}

	// 2. 收集查询参数并过滤事件
	queryParams, eventChannels, invalidIds, err := p.prepareEventsAndParams(ctx, operationEvents, processingCtx)
	if err != nil {
		return fmt.Errorf("收集查询参数失败: %w", err)
	}

	// 3. 立即更新无效事件状态
	if len(invalidIds) > 0 {
		if err := p.db.UpdateEventLogsStatusFiltered(ctx.Ctx, invalidIds); err != nil {
			ctx.Log.Errorf("更新无效事件状态失败: %v", err)
			// return err
		}
	}

	// 4. 如果没有有效事件，直接返回
	if len(eventChannels) == 0 {
		return nil
	}

	// 5. 批量查询链上数据
	chainData, err := p.queryChainData(ctx, queryParams, endBlockNumber)
	if err != nil {
		return fmt.Errorf("批量查询链上数据失败: %w", err)
	}

	processingCtx.ChainData = chainData

	// 6. 并发处理事件（每个市场处理完立即更新状态）
	err = p.processEventsConcurrently(ctx, eventChannels, processingCtx)
	if err != nil {
		return fmt.Errorf("并发处理事件失败: %w", err)
	}

	return nil
}

// buildContext 构建事件处理上下文
func (p *EventProcessor) buildContext(ctx com.Ctx, operationEvents []*model.EventLog) (*ProcessingContext, error) {
	// 1. 提取所有市场地址
	marketAddresses := p.extractMarketAddresses(operationEvents)

	// 2. 查询已有市场信息
	marketMap, err := p.getMarketInfo(ctx, marketAddresses)
	if err != nil {
		return nil, fmt.Errorf("查询市场信息失败: %w", err)
	}

	// 3. 收集用户地址
	userAddresses := p.collectUserAddresses(ctx, operationEvents, marketMap)

	// 4. 查询用户信息
	userInfoMap, err := p.getUserInfo(ctx, userAddresses)
	if err != nil {
		return nil, fmt.Errorf("查询用户信息失败: %w", err)
	}

	return &ProcessingContext{
		MarketMap:   marketMap,
		UserInfoMap: userInfoMap,
	}, nil
}

// extractMarketAddresses 提取市场地址
func (p *EventProcessor) extractMarketAddresses(operationEvents []*model.EventLog) []string {
	marketAddresses := make([]string, 0)
	marketAddrMap := make(map[string]bool)

	for _, eventLog := range operationEvents {
		switch eventLog.Type {
		case model.TypePrediction:
			if !marketAddrMap[eventLog.Address] {
				marketAddrMap[eventLog.Address] = true
				marketAddresses = append(marketAddresses, eventLog.Address)
			}
		}
	}

	return marketAddresses
}

// getMarketInfo 查询市场信息
func (p *EventProcessor) getMarketInfo(ctx com.Ctx, marketAddresses []string) (map[string]*marketcenterPb.GetMarketsAndOptionsForBlockListenerResponse_Market, error) {
	marketMap := make(map[string]*marketcenterPb.GetMarketsAndOptionsForBlockListenerResponse_Market)

	if len(marketAddresses) == 0 {
		return marketMap, nil
	}

	resp, err := p.rpcClient.MarketcenterClient.GetMarketsAndOptionsForBlockListener(ctx.Ctx, &marketcenterPb.GetMarketsAndOptionsForBlockListenerRequest{
		MarketAddresses: marketAddresses,
	})
	if err != nil {
		return nil, err
	}

	for _, market := range resp.Markets {
		marketMap[market.Address] = market
	}

	return marketMap, nil
}

// collectUserAddresses 收集用户地址
func (p *EventProcessor) collectUserAddresses(
	ctx com.Ctx,
	operationEvents []*model.EventLog,
	marketMap map[string]*marketcenterPb.GetMarketsAndOptionsForBlockListenerResponse_Market,
) []string {
	userAddresses := make([]string, 0)
	userAddrMap := make(map[string]bool)

	for _, eventLog := range operationEvents {
		switch eventLog.Type {
		case model.TypeErc20Transfer:
			// 对于ERC20事件，需要同时收集from和to地址
			erc20Event, err := p.arbClient.Contract.Erc20Contract.ParseTransferEvent(*eventLog.ToChainLog())
			if err != nil {
				ctx.Log.Errorf("解析erc20代币转移事件失败: %v", err)
				continue
			}
			from := erc20Event.From.Hex()
			to := erc20Event.To.Hex()
			if from != "" && !userAddrMap[from] {
				ctx.Log.Debugf("收集 用户地址: %s", from)
				userAddrMap[from] = true
				userAddresses = append(userAddresses, from)
			}
			if to != "" && !userAddrMap[to] {
				ctx.Log.Debugf("收集 用户地址: %s", to)
				userAddrMap[to] = true
				userAddresses = append(userAddresses, to)
			}

		case model.TypePrediction:
			// 检查是否是已知市场
			_, marketExists := marketMap[eventLog.Address]
			if !marketExists {
				ctx.Log.Debugf("过滤 市场不存在: %s", eventLog.Address)
				continue
			}

			predictionEvent, err := p.arbClient.Contract.PredictionContract.ParseEvent(*eventLog.ToChainLog())
			if err != nil {
				ctx.Log.Errorf("解析预测事件失败: %v", err)
				continue
			}

			var userAddr string
			switch e := predictionEvent.(type) {
			case *contract.DepositedEvent:
				ctx.Log.Debugf("处理 存款事件: %s, address: %s", eventLog.TxHash, eventLog.Address)
				userAddr = e.User.Hex()
			case *contract.WithdrawnEvent:
				ctx.Log.Debugf("处理 提款事件: %s, address: %s", eventLog.TxHash, eventLog.Address)
				userAddr = e.User.Hex()
			case *contract.SwappedEvent:
				ctx.Log.Debugf("处理 交换事件: %s, address: %s", eventLog.TxHash, eventLog.Address)
				// 忽略user
			case *contract.ClaimedEvent:
				ctx.Log.Debugf("处理 领取事件: %s, address: %s", eventLog.TxHash, eventLog.Address)
				userAddr = e.User.Hex()
			default:
				// 其他事件不用收集用户地址
			}

			if userAddr != "" && !userAddrMap[userAddr] {
				userAddrMap[userAddr] = true
				userAddresses = append(userAddresses, userAddr)
			}
		}
	}

	return userAddresses
}

// getUserInfo 查询用户信息
func (p *EventProcessor) getUserInfo(ctx com.Ctx, userAddresses []string) (map[string]*usercenterPb.GetUsersInfoByAddressesReply_User, error) {
	userInfoMap := make(map[string]*usercenterPb.GetUsersInfoByAddressesReply_User)

	if len(userAddresses) == 0 {
		return userInfoMap, nil
	}

	resp, err := p.rpcClient.UsercenterClient.GetUsersInfoByAddresses(ctx.Ctx, &usercenterPb.GetUsersInfoByAddressesRequest{
		Addresses: userAddresses,
	})
	if err != nil {
		return nil, err
	}

	for _, user := range resp.Users {
		userInfoMap[user.Address] = user
	}

	return userInfoMap, nil
}

// prepareEventsAndParams 收集查询参数并过滤事件
func (p *EventProcessor) prepareEventsAndParams(
	ctx com.Ctx,
	operationEvents []*model.EventLog,
	processingCtx *ProcessingContext,
) (*QueryParams, map[string]chan *model.EventLog, []uint64, error) {

	queryParams := &QueryParams{
		BlockNumberList:   make([]uint64, 0),
		UserTokenPairs:    make([][3]interface{}, 0),
		MarketOptionPairs: make([][3]interface{}, 0),
	}

	invalidIds := make([]uint64, 0)
	eventChannels := make(map[string]chan *model.EventLog)

	for _, eventLog := range operationEvents {
		if eventLog.Removed {
			ctx.Log.Infof(MsgEventIgnored, eventLog.TxHash)
			invalidIds = append(invalidIds, uint64(eventLog.ID))
			continue
		}

		switch eventLog.Type {
		case model.TypeErc20Transfer:
			if p.handleERC20Event(ctx, eventLog, queryParams, eventChannels, processingCtx) {
				// 事件有效，已处理
			} else {
				invalidIds = append(invalidIds, uint64(eventLog.ID))
			}
		case model.TypePrediction:
			if p.handlePredictionEvent(ctx, eventLog, queryParams, eventChannels, processingCtx) {
				// 事件有效，已处理
			} else {
				invalidIds = append(invalidIds, uint64(eventLog.ID))
			}
		default:
			ctx.Log.Debugf("忽略 未知事件类型: address: %s, type: %d, txhash: %s", eventLog.Address, eventLog.Type, eventLog.TxHash)
			invalidIds = append(invalidIds, uint64(eventLog.ID))
		}
	}

	ctx.Log.Infof("unvalidIds: %+v", invalidIds)

	// 关闭所有通道
	for _, eventChan := range eventChannels {
		close(eventChan)
	}

	queryParams.BlockNumberList = removeDuplicateUint64(queryParams.BlockNumberList)

	return queryParams, eventChannels, invalidIds, nil
}

// removeDuplicateUint64 去除uint64切片中的重复元素
func removeDuplicateUint64(slice []uint64) []uint64 {
	if len(slice) == 0 {
		return slice
	}

	seen := make(map[uint64]bool)
	result := make([]uint64, 0, len(slice))

	for _, v := range slice {
		if !seen[v] {
			seen[v] = true
			result = append(result, v)
		}
	}

	return result
}

// handleERC20Event 处理ERC20事件
func (p *EventProcessor) handleERC20Event(
	ctx com.Ctx,
	eventLog *model.EventLog,
	queryParams *QueryParams,
	eventChannels map[string]chan *model.EventLog,
	processingCtx *ProcessingContext,
) bool {
	erc20Event, err := p.arbClient.Contract.Erc20Contract.ParseTransferEvent(*eventLog.ToChainLog())
	if err != nil {
		ctx.Log.Errorf("解析erc20代币转移事件失败: %v", err)
		return false
	}

	fromAddr := erc20Event.From.Hex()
	toAddr := erc20Event.To.Hex()

	_, fromOk := processingCtx.UserInfoMap[fromAddr]
	_, toOk := processingCtx.UserInfoMap[toAddr]

	if !fromOk && !toOk {
		ctx.Log.Debugf(MsgUserNotFound, eventLog.TxHash)
		return false
	}

	// 收集查询参数
	queryParams.BlockNumberList = append(queryParams.BlockNumberList, eventLog.BlockNumber)
	if fromOk {
		queryParams.UserTokenPairs = append(queryParams.UserTokenPairs, [3]interface{}{erc20Event.From, erc20Event.Address, eventLog.BlockNumber})
	}
	if toOk {
		queryParams.UserTokenPairs = append(queryParams.UserTokenPairs, [3]interface{}{erc20Event.To, erc20Event.Address, eventLog.BlockNumber})
	}

	// 添加到事件通道
	p.addEventToChannel(eventChannels, eventLog.Address, eventLog)
	return true
}

// handlePredictionEvent 处理预测事件
func (p *EventProcessor) handlePredictionEvent(
	ctx com.Ctx,
	eventLog *model.EventLog,
	queryParams *QueryParams,
	eventChannels map[string]chan *model.EventLog,
	processingCtx *ProcessingContext,
) bool {
	// 检查是否是已知市场
	marketInfo, ok := processingCtx.MarketMap[eventLog.Address]
	if !ok {
		ctx.Log.Infof(MsgMarketNotFound, eventLog.Address)
		return false
	}

	// 收集查询参数
	queryParams.BlockNumberList = append(queryParams.BlockNumberList, eventLog.BlockNumber)

	// 解析事件以判断事件类型
	predictionEvent, err := p.arbClient.Contract.PredictionContract.ParseEvent(*eventLog.ToChainLog())
	if err != nil {
		ctx.Log.Errorf("解析预测事件失败: %v %s", err, eventLog.TxHash)
		return false
	}

	// 只有swap、deposit、withdraw三个事件才收集选项价格查询参数
	switch predictionEvent.(type) {
	case *contract.DepositedEvent, *contract.WithdrawnEvent, *contract.SwappedEvent:
		// 收集选项价格查询参数
		for _, option := range marketInfo.Options {
			queryParams.MarketOptionPairs = append(queryParams.MarketOptionPairs, [3]interface{}{eventLog.Address, option.Index, eventLog.BlockNumber})
		}
	}

	// 解析事件并收集用户代币对
	if p.collectTokenPairs(ctx, eventLog, marketInfo, queryParams, processingCtx) {
		p.addEventToChannel(eventChannels, eventLog.Address, eventLog)
		return true
	}

	return false
}

// collectTokenPairs 从预测事件中收集用户代币对
func (p *EventProcessor) collectTokenPairs(
	ctx com.Ctx,
	eventLog *model.EventLog,
	marketInfo *marketcenterPb.GetMarketsAndOptionsForBlockListenerResponse_Market,
	queryParams *QueryParams,
	processingCtx *ProcessingContext,
) bool {
	predictionEvent, err := p.arbClient.Contract.PredictionContract.ParseEvent(*eventLog.ToChainLog())
	if err != nil {
		ctx.Log.Errorf("解析预测事件失败: %v %s", err, eventLog.TxHash)
		return false
	}

	switch e := predictionEvent.(type) {
	case *contract.DepositedEvent:
		return p.prepareDepositQuery(e, marketInfo, queryParams, processingCtx)
	case *contract.WithdrawnEvent:
		return p.prepareWithdrawQuery(e, marketInfo, queryParams, processingCtx)
	case *contract.ClaimedEvent:
		return p.prepareClaimQuery(e, marketInfo, queryParams, processingCtx)
	case *contract.SwappedEvent, *contract.SettlingEvent, *contract.AssertionDisputedEvent, *contract.AssertionResolvedEvent:
		return true // 这些事件不需要收集用户代币对
	default:
		ctx.Log.Infof("忽略未知事件: %s", eventLog.TxHash)
		return false
	}
}

// prepareDepositQuery 处理存款事件的查询参数收集
func (p *EventProcessor) prepareDepositQuery(
	event *contract.DepositedEvent,
	marketInfo *marketcenterPb.GetMarketsAndOptionsForBlockListenerResponse_Market,
	queryParams *QueryParams,
	processingCtx *ProcessingContext,
) bool {
	userAddr := event.User.Hex()
	_, userExists := processingCtx.UserInfoMap[userAddr]
	if !userExists {
		return true // 非平台用户也要返回true，但不收集代币对
	}

	optionIndex := uint32(event.OptionOut)
	for _, option := range marketInfo.Options {
		if option.Index == optionIndex {
			queryParams.UserTokenPairs = append(queryParams.UserTokenPairs, [3]interface{}{event.User, common.HexToAddress(option.Address), uint64(event.BlockNumber)})
			break
		}
	}
	return true
}

// prepareWithdrawQuery 处理提款事件的查询参数收集
func (p *EventProcessor) prepareWithdrawQuery(
	event *contract.WithdrawnEvent,
	marketInfo *marketcenterPb.GetMarketsAndOptionsForBlockListenerResponse_Market,
	queryParams *QueryParams,
	processingCtx *ProcessingContext,
) bool {
	userAddr := event.User.Hex()
	_, userExists := processingCtx.UserInfoMap[userAddr]
	if !userExists {
		return true
	}

	optionIndex := uint32(event.OptionIn)
	for _, option := range marketInfo.Options {
		if option.Index == optionIndex {
			queryParams.UserTokenPairs = append(queryParams.UserTokenPairs, [3]interface{}{event.User, common.HexToAddress(option.Address), uint64(event.BlockNumber)})
			break
		}
	}
	return true
}

// prepareClaimQuery 处理认领事件的查询参数收集
func (p *EventProcessor) prepareClaimQuery(
	event *contract.ClaimedEvent,
	marketInfo *marketcenterPb.GetMarketsAndOptionsForBlockListenerResponse_Market,
	queryParams *QueryParams,
	processingCtx *ProcessingContext,
) bool {
	userAddr := event.User.Hex()
	_, userExists := processingCtx.UserInfoMap[userAddr]
	if !userExists {
		return true
	}

	optionIndex := uint32(event.Option)
	for _, option := range marketInfo.Options {
		if option.Index == optionIndex {
			queryParams.UserTokenPairs = append(queryParams.UserTokenPairs, [3]interface{}{event.User, common.HexToAddress(option.Address), uint64(event.BlockNumber)})
			break
		}
	}
	return true
}

// addEventToChannel 添加事件到通道
func (p *EventProcessor) addEventToChannel(eventChannels map[string]chan *model.EventLog, address string, eventLog *model.EventLog) {
	if eventChan, ok := eventChannels[address]; ok {
		eventChan <- eventLog
	} else {
		eventChan = make(chan *model.EventLog, EventChannelBufferSize)
		eventChannels[address] = eventChan
		eventChan <- eventLog
	}
}

// queryChainData 批量查询链上数据
func (p *EventProcessor) queryChainData(ctx com.Ctx, queryParams *QueryParams, endBlockNumber uint64) (*ChainData, error) {
	chainData := &ChainData{
		marketAddressToMarketInfo: make(map[string]*contract.PredictionMarketInfo),
		userTokenBalances:         make(map[string]*data.UserTokenBalance),
		optionPrices:              make(map[string]*data.OptionPrice),
		blockNumberToTime:         make(map[uint64]uint64),
	}

	// 查询选项价格
	if len(queryParams.MarketOptionPairs) > 0 {
		prices, err := p.arbClient.BatchQueryOptionPrices(ctx.Ctx, queryParams.MarketOptionPairs, endBlockNumber)
		if err != nil {
			return nil, fmt.Errorf("批量查询选项价格失败: %w", err)
		}
		chainData.optionPrices = prices
	}

	// 查询用户代币余额
	if len(queryParams.UserTokenPairs) > 0 {
		balances, err := p.arbClient.BatchQueryERC20BalancesByPairs(ctx.Ctx, queryParams.UserTokenPairs, endBlockNumber)
		if err != nil {
			return nil, fmt.Errorf("批量查询用户代币余额失败: %w", err)
		}
		chainData.userTokenBalances = balances
	}

	// 查询区块时间
	if len(queryParams.BlockNumberList) > 0 {
		blockTimes, err := p.arbClient.BatchQueryBlockTimestamps(ctx.Ctx, queryParams.BlockNumberList)
		if err != nil {
			return nil, fmt.Errorf("批量查询区块时间失败: %w", err)
		}
		chainData.blockNumberToTime = blockTimes
	}

	p.logChainData(ctx, chainData)
	return chainData, nil
}

// logChainData 记录链上查询数据结果
func (p *EventProcessor) logChainData(ctx com.Ctx, chainData *ChainData) {
	ctx.Log.Debugf("选项价格数据: %+v", func() string {
		result := ""
		for k, v := range chainData.optionPrices {
			result += fmt.Sprintf("\n市场选项: %s, 价格: %s", k, v.Price.String())
		}
		return result
	}())

	ctx.Log.Debugf("用户代币余额数据: %+v", func() string {
		result := ""
		for k, v := range chainData.userTokenBalances {
			result += fmt.Sprintf("\n用户地址: %s, 代币地址: %s, 余额: %s", k, v.TokenAddr, v.Balance.String())
		}
		return result
	}())

	ctx.Log.Debugf("区块时间数据: %+v", func() string {
		result := ""
		for blockNum, timestamp := range chainData.blockNumberToTime {
			result += fmt.Sprintf("\n区块号: %d, 时间戳: %d", blockNum, timestamp)
		}
		return result
	}())
}

// processEventsConcurrently 并发处理事件并立即更新状态
func (p *EventProcessor) processEventsConcurrently(
	ctx com.Ctx,
	eventChannels map[string]chan *model.EventLog,
	processingCtx *ProcessingContext,
) error {
	if len(eventChannels) == 0 {
		return nil
	}

	wg := sync.WaitGroup{}
	wg.Add(len(eventChannels))

	ctx.Log.Infof("开始并发处理事件，市场数量: %d", len(eventChannels))

	for marketAddr, eventChan := range eventChannels {
		go func(marketAddr string, eventChan chan *model.EventLog) {
			defer func() {
				if err := recover(); err != nil {
					ctx.Log.Errorf("address %s handler events panic: %+v", marketAddr, string(debug.Stack()))
					alarm.Lark.Send(fmt.Sprintf("address %s handler events panic err: [%+v], stack: [%+v]", marketAddr, err, string(debug.Stack())))
				}
				wg.Done()
			}()

			processedIds := make([]uint64, 0)
			for eventLog := range eventChan {
				var err error
				switch eventLog.Type {
				case model.TypeErc20Transfer:
					err = p.handleTransferEvent(ctx, eventLog, processingCtx.ChainData, processingCtx.MarketMap, processingCtx.UserInfoMap)
				case model.TypePrediction:
					err = p.handleMarketEvent(ctx, eventLog, processingCtx.ChainData, processingCtx.MarketMap, processingCtx.UserInfoMap)
				default:
					ctx.Log.Debugf("忽略未知事件: address: %s, type: %d, txhash: %s", eventLog.Address, eventLog.Type, eventLog.TxHash)
					continue
				}

				if err != nil {
					ctx.Log.Errorf("address %s handler events err: %v", marketAddr, err)
					alarm.Lark.Send(fmt.Sprintf("address %s handler events err: %v", marketAddr, err))
					break
				}

				processedIds = append(processedIds, uint64(eventLog.ID))
			}

			ctx.Log.Infof("address %s handler events done, success event count: %d", marketAddr, len(processedIds))

			// 立即更新当前市场的成功事件状态
			if len(processedIds) == 0 {
				return
			}

			if err := p.db.UpdateEventLogsStatusSucc(ctx.Ctx, processedIds); err != nil {
				ctx.Log.Errorf("update event logs status err: %v", err)
			}

		}(marketAddr, eventChan)
	}

	wg.Wait()
	return nil
}

func (p *EventProcessor) handleTransferEvent(
	ctx com.Ctx,
	eventLog *model.EventLog,
	chainData *ChainData,
	marketMap map[string]*marketcenterPb.GetMarketsAndOptionsForBlockListenerResponse_Market,
	userInfoMap map[string]*usercenterPb.GetUsersInfoByAddressesReply_User,
) error {

	erc20Event, err := p.arbClient.Contract.Erc20Contract.ParseTransferEvent(*eventLog.ToChainLog())
	if err != nil {
		return fmt.Errorf("解析erc20代币转移事件失败: %v", err)
	}
	from := erc20Event.From.Hex()
	to := erc20Event.To.Hex()
	transferAmount := erc20Event.Value.String()

	for _, address := range []string{from, to} {
		if userInfo, userExists := userInfoMap[address]; userExists {

			isFrom := address == from
			// 查询用户基础代币余额
			balanceKey := fmt.Sprintf(BalanceKeyFormat, address, eventLog.Address, eventLog.BlockNumber)
			if balance, ok := chainData.userTokenBalances[balanceKey]; ok {
				userBalance := balance.Balance

				// 更新用户余额
				_, err := p.rpcClient.MarketcenterClient.UpdateUserBaseTokenBalance(ctx.Ctx, &marketcenterPb.UpdateUserBaseTokenBalanceRequest{
					Uid:         userInfo.Uid,
					UserAddress: address,
					TokenBalance: &marketcenterPb.TokenBalance{
						TokenAddress: eventLog.Address,
						Amount:       userBalance.String(),
						BlockNumber:  eventLog.BlockNumber,
					},
					TxHash:         eventLog.TxHash,
					BlockNumber:    eventLog.BlockNumber,
					From:           from,
					To:             to,
					TransferAmount: transferAmount,
					Side: func() uint32 {
						if isFrom {
							return 2
						}
						return 1
					}(),
				})
				if err != nil {
					ctx.Log.Errorf("更新用户基础代币余额失败: %v", err)
					return fmt.Errorf("更新用户基础代币余额失败: %w", err)
				}

			}
		}
	}

	return nil
}

// handleMarketEvent 处理市场事件
func (p *EventProcessor) handleMarketEvent(
	ctx com.Ctx,
	eventLog *model.EventLog,
	chainData *ChainData,
	marketMap map[string]*marketcenterPb.GetMarketsAndOptionsForBlockListenerResponse_Market,
	userInfoMap map[string]*usercenterPb.GetUsersInfoByAddressesReply_User,
) error {
	if eventLog.Type != model.TypePrediction {
		return fmt.Errorf("非预测市场事件")
	}

	marketAddress := eventLog.Address
	marketInfo, exists := marketMap[marketAddress]
	if !exists {
		return fmt.Errorf("市场信息不存在")
	}

	predictionEvent, err := p.arbClient.Contract.PredictionContract.ParseEvent(*eventLog.ToChainLog())
	if err != nil {
		return fmt.Errorf("解析预测事件失败: %w", err)
	}

	// 查询区块时间
	blockTime, ok := chainData.blockNumberToTime[eventLog.BlockNumber]
	if !ok {
		return fmt.Errorf("区块时间不存在")
	}

	// 准备事件处理参数
	eventParams := &EventParams{
		EventLog:      eventLog,
		MarketInfo:    marketInfo,
		ChainData:     chainData,
		UserInfoMap:   userInfoMap,
		BlockTime:     blockTime,
		BaseTokenType: p.getBaseTokenType(marketInfo),
		OptionPrices:  p.buildOptionPrices(ctx, marketInfo, chainData, eventLog.BlockNumber),
	}

	// 根据事件类型处理
	return p.routeEvent(ctx, predictionEvent, eventParams)
}

// EventParams 事件处理参数
type EventParams struct {
	EventLog      *model.EventLog
	MarketInfo    *marketcenterPb.GetMarketsAndOptionsForBlockListenerResponse_Market
	ChainData     *ChainData
	UserInfoMap   map[string]*usercenterPb.GetUsersInfoByAddressesReply_User
	BlockTime     uint64
	BaseTokenType marketcenterPb.BaseTokenType
	OptionPrices  []*marketcenterPb.UpdateOptionPrice
}

// getBaseTokenType 获取基础代币类型
func (p *EventProcessor) getBaseTokenType(marketInfo *marketcenterPb.GetMarketsAndOptionsForBlockListenerResponse_Market) marketcenterPb.BaseTokenType {
	if marketInfo.BaseTokenType == marketcenterPb.BaseTokenType_BASE_TOKEN_TYPE_USDC {
		return marketcenterPb.BaseTokenType_BASE_TOKEN_TYPE_USDC
	}
	return marketcenterPb.BaseTokenType_BASE_TOKEN_TYPE_POINTS
}

// buildOptionPrices 构建选项价格列表
func (p *EventProcessor) buildOptionPrices(
	ctx com.Ctx,
	marketInfo *marketcenterPb.GetMarketsAndOptionsForBlockListenerResponse_Market,
	chainData *ChainData,
	blockNumber uint64,
) []*marketcenterPb.UpdateOptionPrice {
	optionPrices := make([]*marketcenterPb.UpdateOptionPrice, 0)

	for _, option := range marketInfo.Options {
		priceKey := fmt.Sprintf(PriceKeyFormat, marketInfo.Address, option.Index, blockNumber)
		priceInfo, hasPriceInfo := chainData.optionPrices[priceKey]
		if hasPriceInfo {
			optionPrices = append(optionPrices, &marketcenterPb.UpdateOptionPrice{
				Address: option.Address,
				Price:   priceInfo.Price.String(),
				Decimal: option.Decimal,
			})
		} else {
			ctx.Log.Warnf("option price not found. market: %s, option: %d, blockNumber: %d", marketInfo.Address, option.Index, blockNumber)
		}
	}

	return optionPrices
}

// routeEvent 分发事件处理器
func (p *EventProcessor) routeEvent(ctx com.Ctx, predictionEvent interface{}, params *EventParams) error {
	switch e := predictionEvent.(type) {
	case *contract.DepositedEvent:
		ctx.Log.Infof("处理存款事件: %s", params.EventLog.TxHash)
		return p.handleDepositEvent(ctx, e, params)
	case *contract.WithdrawnEvent:
		ctx.Log.Infof("处理提款事件: %s", params.EventLog.TxHash)
		return p.handleWithdrawEvent(ctx, e, params)
	case *contract.SwappedEvent:
		ctx.Log.Infof("处理交换事件: %s", params.EventLog.TxHash)
		return p.handleSwapEvent(ctx, e, params)
	case *contract.ClaimedEvent:
		ctx.Log.Infof("处理认领事件: %s", params.EventLog.TxHash)
		return p.handleClaimEvent(ctx, e, params)
	case *contract.SettlingEvent:
		ctx.Log.Infof("处理结算事件: %s", params.EventLog.TxHash)
		return p.handleSettlingEvent(ctx, e, params)
	case *contract.AssertionDisputedEvent:
		ctx.Log.Infof("处理断言争议事件: %s", params.EventLog.TxHash)
		return p.handleDisputeEvent(ctx, e, params)
	case *contract.AssertionResolvedEvent:
		ctx.Log.Infof("处理断言解决事件: %s", params.EventLog.TxHash)
		return p.handleResolvedEvent(ctx, e, params)
	default:
		ctx.Log.Infof("未处理的预测事件类型: %T", e)
		return nil
	}
}

// updatePricesOnly 非平台用户的买卖事件 只更新选项价格
func (p *EventProcessor) updatePricesOnly(ctx com.Ctx, params *EventParams) error {
	updateReq := &marketcenterPb.BatchUpdateOptionPriceRequest{
		OptionPrices: make([]*marketcenterPb.BatchUpdateOptionPriceRequest_OptionPrice, 0),
	}

	for _, oneOptionPrice := range params.OptionPrices {
		rspOne := &marketcenterPb.BatchUpdateOptionPriceRequest_OptionPrice{
			OptionAddress: oneOptionPrice.Address,
			Price:         oneOptionPrice.Price,
			Decimal:       oneOptionPrice.Decimal,
			BaseTokenType: params.BaseTokenType,
			BlockTime:     params.BlockTime,
			BlockNumber:   params.EventLog.BlockNumber,
		}
		updateReq.OptionPrices = append(updateReq.OptionPrices, rspOne)
	}

	_, err := p.rpcClient.MarketcenterClient.BatchUpdateOptionPrice(ctx.Ctx, updateReq)
	if err != nil {
		ctx.Log.Errorf("批量更新选项价格失败: %v", err)
		return fmt.Errorf("批量更新选项价格失败: %w", err)
	}

	return nil
}

// getBalance 获取用户代币余额，
func (p *EventProcessor) getBalance(ctx com.Ctx, userAddress, tokenAddress string, blockNumber uint64, chainData *ChainData) (*big.Int, error) {
	balanceKey := fmt.Sprintf(BalanceKeyFormat, userAddress, tokenAddress, blockNumber)
	if balance, hasBalance := chainData.userTokenBalances[balanceKey]; hasBalance {
		return balance.Balance, nil
	}

	ctx.Log.Warnf("用户代币余额不存在: %s", balanceKey)
	return big.NewInt(0), fmt.Errorf("用户代币余额不存在: %s", balanceKey)
}

// findOptionAddress 根据选项索引查找选项地址
func (p *EventProcessor) findOptionAddress(optionIndex uint32, marketInfo *marketcenterPb.GetMarketsAndOptionsForBlockListenerResponse_Market) (string, uint32, error) {
	for _, option := range marketInfo.Options {
		if option.Index == optionIndex {
			return option.Address, option.Decimal, nil
		}
	}
	return "", 0, fmt.Errorf("option address not found. market: %s, option: %d", marketInfo.Address, optionIndex)
}

func (p *EventProcessor) handleSettlingEvent(ctx com.Ctx, event *contract.SettlingEvent, params *EventParams) error {
	finalOptionAddress, _, err := p.findOptionAddress(uint32(event.FinalOption), params.MarketInfo)
	if err != nil || finalOptionAddress == "" {
		ctx.Log.Warnf("最终选项地址不存在: %s", params.EventLog.Address)
		return fmt.Errorf("最终选项地址不存在")
	}

	_, err = p.rpcClient.MarketcenterClient.ProcessMarketSettingEvent(ctx.Ctx, &marketcenterPb.ProcessMarketSettingEventRequest{
		MarketAddress:      params.EventLog.Address,
		FinalOptionAddress: finalOptionAddress,
		AssertionId:        event.AssertionId,
		BlockNumber:        params.EventLog.BlockNumber,
		TxHash:             params.EventLog.TxHash,
	})
	if err != nil {
		ctx.Log.Errorf("处理市场设置事件失败: %v", err)
		return fmt.Errorf("处理市场设置事件失败: %w", err)
	}

	return nil
}

func (p *EventProcessor) handleDisputeEvent(ctx com.Ctx, event *contract.AssertionDisputedEvent, params *EventParams) error {
	_, err := p.rpcClient.MarketcenterClient.ProcessMarketAssertDisputedEvent(ctx.Ctx, &marketcenterPb.ProcessMarketAssertDisputedEventRequest{
		MarketAddress: params.EventLog.Address,
		AssertionId:   event.AssertionId.Bytes(),
		BlockNumber:   params.EventLog.BlockNumber,
		TxHash:        params.EventLog.TxHash,
	})
	if err != nil {
		ctx.Log.Errorf("处理市场断言争议事件失败: %v", err)
		return fmt.Errorf("处理市场断言争议事件失败: %w", err)
	}
	return nil
}

func (p *EventProcessor) handleResolvedEvent(ctx com.Ctx, event *contract.AssertionResolvedEvent, params *EventParams) error {
	_, err := p.rpcClient.MarketcenterClient.ProcessMarketAssertionResolvedEvent(ctx.Ctx, &marketcenterPb.ProcessMarketAssertionResolvedEventRequest{
		MarketAddress:      params.EventLog.Address,
		AssertedTruthfully: event.AssertedTruthfully,
		AssertionId:        event.AssertionId.Bytes(),
		BlockNumber:        params.EventLog.BlockNumber,
		TxHash:             params.EventLog.TxHash,
	})
	if err != nil {
		ctx.Log.Errorf("处理市场断言解决事件失败: %v", err)
		return fmt.Errorf("处理市场断言解决事件失败: %w", err)
	}

	return nil
}

func (p *EventProcessor) handleClaimEvent(ctx com.Ctx, event *contract.ClaimedEvent, params *EventParams) error {
	userAddress := event.User.Hex()
	userInfo, exists := params.UserInfoMap[userAddress]
	if !exists || userInfo.Uid == "" {
		ctx.Log.Debugf("非平台用户，忽略: %s", userAddress)
		return nil // claim 事件 非平台用户什么也不做  忽略
	}

	optionAddress, _, err := p.findOptionAddress(uint32(event.Option), params.MarketInfo)
	if err != nil {
		ctx.Log.Errorf("处理用户代币领取事件失败: %v market: %s, txhash: %s, option: %s, amount: %s", err, params.EventLog.Address, params.EventLog.TxHash, optionAddress, event.Amount.String())
		return err
	}

	userBalance, err := p.getBalance(ctx, userAddress, optionAddress, params.EventLog.BlockNumber, params.ChainData)
	if err != nil {
		return err
	}

	_, err = p.rpcClient.MarketcenterClient.ProcessMarketClaimResultEvent(ctx.Ctx, &marketcenterPb.ProcessMarketClaimResultEventRequest{
		Uid:           userInfo.Uid,
		UserAddress:   userAddress,
		MarketAddress: params.EventLog.Address,
		Amount:        event.Amount.String(),
		OptionAddress: optionAddress,
		OptionBalance: userBalance.String(),
		BaseTokenType: params.BaseTokenType,
		BlockTime:     params.BlockTime,
		TxHash:        params.EventLog.TxHash,
		BlockNumber:   params.EventLog.BlockNumber,
	})
	if err != nil {
		ctx.Log.Errorf("处理用户代币领取事件失败: %v market: %s, txhash: %s, option: %s, amount: %s", err, params.EventLog.Address, params.EventLog.TxHash, optionAddress, event.Amount.String())
		return fmt.Errorf("处理用户代币领取事件失败: %w", err)
	}

	return nil
}

// handleDepositEvent 处理存款事件
func (p *EventProcessor) handleDepositEvent(ctx com.Ctx, event *contract.DepositedEvent, params *EventParams) error {
	userAddress := event.User.Hex()
	userInfo, exists := params.UserInfoMap[userAddress]
	if !exists || userInfo.Uid == "" {
		return p.updatePricesOnly(ctx, params) // 非平台用户，只更新价格
	}

	optionOutAddress, decimal, err := p.findOptionAddress(uint32(event.OptionOut), params.MarketInfo)
	if err != nil || optionOutAddress == "" {
		return err
	}

	userBalance, err := p.getBalance(ctx, userAddress, optionOutAddress, params.EventLog.BlockNumber, params.ChainData)
	if err != nil {
		return err
	}

	// 调用RPC处理存款事件
	_, err = p.rpcClient.MarketcenterClient.ProcessMarketDepositOrWithdrawEvent(ctx.Ctx, &marketcenterPb.ProcessMarketDepositOrWithdrawEventRequest{
		Uid:                    userInfo.Uid,
		TxHash:                 params.EventLog.TxHash,
		BlockNumber:            params.EventLog.BlockNumber,
		UserAddress:            userAddress,
		UserOptionTokenAddress: optionOutAddress,
		UserOptionTokenBalance: userBalance.String(),
		MarketAddress:          params.EventLog.Address,
		AmountIn:               event.AmountIn.String(),
		AmountOut:              event.AmountOut.String(),
		OptionPrices:           params.OptionPrices,
		BaseTokenType:          params.BaseTokenType,
		Decimal:                decimal,
		BlockTime:              params.BlockTime,
		Side:                   marketcenterPb.ProcessMarketDepositOrWithdrawEventRequest_SIDE_DEPOSIT,
	})

	if err != nil {
		ctx.Log.Errorf("处理存款事件失败: %v", err)
		return fmt.Errorf("处理存款事件失败: %w", err)
	}

	return nil
}

// handleWithdrawEvent 处理提款事件
func (p *EventProcessor) handleWithdrawEvent(ctx com.Ctx, event *contract.WithdrawnEvent, params *EventParams) error {
	userAddress := event.User.Hex()
	userInfo, exists := params.UserInfoMap[userAddress]
	if !exists || userInfo.Uid == "" {
		return p.updatePricesOnly(ctx, params) // 非平台用户，只更新价格
	}

	optionInAddress, decimal, err := p.findOptionAddress(uint32(event.OptionIn), params.MarketInfo)
	if err != nil || optionInAddress == "" {
		return err
	}

	userBalance, err := p.getBalance(ctx, userAddress, optionInAddress, params.EventLog.BlockNumber, params.ChainData)
	if err != nil {
		return err
	}

	// 调用RPC处理提款事件
	_, err = p.rpcClient.MarketcenterClient.ProcessMarketDepositOrWithdrawEvent(ctx.Ctx, &marketcenterPb.ProcessMarketDepositOrWithdrawEventRequest{
		Uid:                    userInfo.Uid,
		TxHash:                 params.EventLog.TxHash,
		BlockNumber:            params.EventLog.BlockNumber,
		UserAddress:            userAddress,
		UserOptionTokenAddress: optionInAddress,
		UserOptionTokenBalance: userBalance.String(),
		MarketAddress:          params.EventLog.Address,
		AmountIn:               event.AmountIn.String(),
		AmountOut:              event.AmountOut.String(),
		OptionPrices:           params.OptionPrices,
		BaseTokenType:          params.BaseTokenType,
		Decimal:                decimal,
		BlockTime:              params.BlockTime,
		Side:                   marketcenterPb.ProcessMarketDepositOrWithdrawEventRequest_SIDE_WITHDRAW,
	})

	if err != nil {
		ctx.Log.Errorf("处理提款事件失败: %v", err)
		return fmt.Errorf("处理提款事件失败: %w", err)
	}

	return nil
}

// handleSwapEvent 处理交换事件
func (p *EventProcessor) handleSwapEvent(ctx com.Ctx, event *contract.SwappedEvent, params *EventParams) error {
	userAddress := event.User.Hex()
	_, exists := params.UserInfoMap[userAddress]
	if !exists {
		ctx.Log.Debugf("swap非平台用户: %s", userAddress)
	}

	// 调用RPC处理交换事件
	_, err := p.rpcClient.MarketcenterClient.ProcessMarketSwapEvent(ctx.Ctx, &marketcenterPb.ProcessMarketSwapEventRequest{
		TxHash:        params.EventLog.TxHash,
		BlockNumber:   params.EventLog.BlockNumber,
		OptionPrices:  params.OptionPrices,
		BaseTokenType: params.BaseTokenType,
		BlockTime:     params.BlockTime,
	})

	if err != nil {
		ctx.Log.Errorf("处理交换事件失败: %v", err)
		return fmt.Errorf("处理交换事件失败: %w", err)
	}

	return nil
}
