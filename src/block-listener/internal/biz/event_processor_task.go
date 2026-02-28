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

	if len(eventLogs) == 0 {
		return nil
	}

	// 过滤已删除的事件
	validEvents := make([]*model.EventLog, 0, len(eventLogs))
	for _, eventLog := range eventLogs {
		if eventLog.Removed {
			c.Log.Debugf("事件已删除: %s", eventLog.TxHash)
			continue
		}
		validEvents = append(validEvents, eventLog)
	}

	if len(validEvents) == 0 {
		return nil
	}

	c.Log.Infof("处理CTF事件，数量: %d", len(validEvents))

	err = p.handleOperationEvents(c, validEvents, endBlockNumber)
	if err != nil {
		c.Log.Errorf("处理CTF事件失败: %v", err)
		return err
	}

	c.Log.Infof("CTF事件处理完成")
	return nil
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
		if eventLog.Type == model.TypePredictionCTF {
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
		if eventLog.Type != model.TypePredictionCTF {
			continue
		}

		// 检查是否是已知市场
		_, marketExists := marketMap[eventLog.Address]
		if !marketExists {
			ctx.Log.Debugf("过滤 市场不存在: %s", eventLog.Address)
			continue
		}

		predictionEvent, err := p.arbClient.Contract.PredictionCTFContract.ParseEvent(*eventLog.ToChainLog())
		if err != nil {
			ctx.Log.Errorf("解析PredictionCTF事件失败: %v", err)
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
		default:
			// 其他事件不用收集用户地址
		}

		if userAddr != "" && !userAddrMap[userAddr] {
			userAddrMap[userAddr] = true
			userAddresses = append(userAddresses, userAddr)
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
		case model.TypeCTF:
			// CTF合约事件直接入队处理
			queryParams.BlockNumberList = append(queryParams.BlockNumberList, eventLog.BlockNumber)
			p.addEventToChannel(eventChannels, eventLog.Address, eventLog)
		case model.TypePredictionCTF:
			if p.handlePredictionCTFEvent(ctx, eventLog, queryParams, eventChannels, processingCtx) {
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

// handlePredictionCTFEvent 准备PredictionCTF事件的查询参数
func (p *EventProcessor) handlePredictionCTFEvent(
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
	predictionCTFEvent, err := p.arbClient.Contract.PredictionCTFContract.ParseEvent(*eventLog.ToChainLog())
	if err != nil {
		ctx.Log.Errorf("解析PredictionCTF事件失败: %v %s", err, eventLog.TxHash)
		return false
	}

	// 只有swap、deposit、withdraw三个事件才收集选项价格查询参数
	switch predictionCTFEvent.(type) {
	case *contract.DepositedEvent, *contract.WithdrawnEvent, *contract.SwappedEvent:
		for _, option := range marketInfo.Options {
			queryParams.MarketOptionPairs = append(queryParams.MarketOptionPairs, [3]interface{}{eventLog.Address, option.Index, eventLog.BlockNumber})
		}
	}

	// 解析事件并收集用户代币对
	if p.collectCTFTokenPairs(ctx, eventLog, marketInfo, queryParams, processingCtx) {
		p.addEventToChannel(eventChannels, eventLog.Address, eventLog)
		return true
	}

	return false
}

// collectCTFTokenPairs 从PredictionCTF事件中收集用户代币对
func (p *EventProcessor) collectCTFTokenPairs(
	ctx com.Ctx,
	eventLog *model.EventLog,
	marketInfo *marketcenterPb.GetMarketsAndOptionsForBlockListenerResponse_Market,
	queryParams *QueryParams,
	processingCtx *ProcessingContext,
) bool {
	predictionCTFEvent, err := p.arbClient.Contract.PredictionCTFContract.ParseEvent(*eventLog.ToChainLog())
	if err != nil {
		ctx.Log.Errorf("解析PredictionCTF事件失败: %v %s", err, eventLog.TxHash)
		return false
	}

	switch e := predictionCTFEvent.(type) {
	case *contract.DepositedEvent:
		return p.prepareDepositQuery(e, marketInfo, queryParams, processingCtx)
	case *contract.WithdrawnEvent:
		return p.prepareWithdrawQuery(e, marketInfo, queryParams, processingCtx)
	case *contract.SwappedEvent, *contract.CTFMarketResolvedEvent, *contract.CTFLiquidityRemovedEvent,
		*contract.LiquidityAddedEvent, *contract.FeeSetEvent, *contract.CTFFeeCollectedEvent:
		return true // 这些事件不需要收集用户代币对
	default:
		ctx.Log.Infof("忽略未知PredictionCTF事件: %s", eventLog.TxHash)
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
				case model.TypeCTF:
					err = p.handleCTFEvent(ctx, eventLog, processingCtx.ChainData)
				case model.TypePredictionCTF:
					err = p.handlePredictionCTFMarketEvent(ctx, eventLog, processingCtx.ChainData, processingCtx.MarketMap, processingCtx.UserInfoMap)
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

// handleCTFEvent 处理ConditionalTokens合约事件
func (p *EventProcessor) handleCTFEvent(
	ctx com.Ctx,
	eventLog *model.EventLog,
	chainData *ChainData,
) error {
	ctfEvent, err := p.arbClient.Contract.ConditionalTokensContract.ParseEvent(*eventLog.ToChainLog())
	if err != nil {
		return fmt.Errorf("解析CTF事件失败: %w", err)
	}

	blockTime, _ := chainData.blockNumberToTime[eventLog.BlockNumber]

	switch e := ctfEvent.(type) {
	case *contract.ConditionPreparationEvent:
		ctx.Log.Infof("CTF ConditionPreparation: conditionId=%s, oracle=%s, questionId=%s, outcomes=%d",
			e.ConditionId.Hex(), e.Oracle.Hex(), e.QuestionId.Hex(), e.OutcomeSlotCount)
		_, err := p.rpcClient.MarketcenterClient.ProcessCTFConditionEvent(ctx.Ctx, &marketcenterPb.ProcessCTFConditionEventRequest{
			EventType:        marketcenterPb.ProcessCTFConditionEventRequest_CONDITION_PREPARATION,
			ConditionId:      e.ConditionId.Hex(),
			Oracle:           e.Oracle.Hex(),
			QuestionId:       e.QuestionId.Hex(),
			OutcomeSlotCount: uint32(e.OutcomeSlotCount),
			TxHash:           eventLog.TxHash,
			BlockNumber:      eventLog.BlockNumber,
			BlockTime:        blockTime,
		})
		if err != nil {
			return fmt.Errorf("ProcessCTFConditionEvent(Preparation) failed: %w", err)
		}
	case *contract.ConditionResolutionEvent:
		ctx.Log.Infof("CTF ConditionResolution: conditionId=%s, oracle=%s, outcomes=%d",
			e.ConditionId.Hex(), e.Oracle.Hex(), e.OutcomeSlotCount)
		payoutNumerators := make([]string, 0, len(e.PayoutNumerators))
		for _, pn := range e.PayoutNumerators {
			payoutNumerators = append(payoutNumerators, pn.String())
		}
		_, err := p.rpcClient.MarketcenterClient.ProcessCTFConditionEvent(ctx.Ctx, &marketcenterPb.ProcessCTFConditionEventRequest{
			EventType:        marketcenterPb.ProcessCTFConditionEventRequest_CONDITION_RESOLUTION,
			ConditionId:      e.ConditionId.Hex(),
			Oracle:           e.Oracle.Hex(),
			QuestionId:       e.QuestionId.Hex(),
			OutcomeSlotCount: uint32(e.OutcomeSlotCount),
			PayoutNumerators: payoutNumerators,
			TxHash:           eventLog.TxHash,
			BlockNumber:      eventLog.BlockNumber,
			BlockTime:        blockTime,
		})
		if err != nil {
			return fmt.Errorf("ProcessCTFConditionEvent(Resolution) failed: %w", err)
		}
	case *contract.PositionSplitEvent:
		ctx.Log.Infof("CTF PositionSplit: stakeholder=%s, conditionId=%s, amount=%s",
			e.Stakeholder.Hex(), e.ConditionId.Hex(), e.Amount.String())
		partition := make([]string, 0, len(e.Partition))
		for _, part := range e.Partition {
			partition = append(partition, part.String())
		}
		_, err := p.rpcClient.MarketcenterClient.ProcessCTFPositionEvent(ctx.Ctx, &marketcenterPb.ProcessCTFPositionEventRequest{
			EventType:          marketcenterPb.ProcessCTFPositionEventRequest_POSITION_SPLIT,
			Stakeholder:        e.Stakeholder.Hex(),
			CollateralToken:    e.CollateralToken.Hex(),
			ParentCollectionId: e.ParentCollectionId.Hex(),
			ConditionId:        e.ConditionId.Hex(),
			Partition:          partition,
			Amount:             e.Amount.String(),
			TxHash:             eventLog.TxHash,
			BlockNumber:        eventLog.BlockNumber,
			BlockTime:          blockTime,
		})
		if err != nil {
			return fmt.Errorf("ProcessCTFPositionEvent(Split) failed: %w", err)
		}
	case *contract.PositionsMergeEvent:
		ctx.Log.Infof("CTF PositionsMerge: stakeholder=%s, conditionId=%s, amount=%s",
			e.Stakeholder.Hex(), e.ConditionId.Hex(), e.Amount.String())
		partition := make([]string, 0, len(e.Partition))
		for _, part := range e.Partition {
			partition = append(partition, part.String())
		}
		_, err := p.rpcClient.MarketcenterClient.ProcessCTFPositionEvent(ctx.Ctx, &marketcenterPb.ProcessCTFPositionEventRequest{
			EventType:          marketcenterPb.ProcessCTFPositionEventRequest_POSITIONS_MERGE,
			Stakeholder:        e.Stakeholder.Hex(),
			CollateralToken:    e.CollateralToken.Hex(),
			ParentCollectionId: e.ParentCollectionId.Hex(),
			ConditionId:        e.ConditionId.Hex(),
			Partition:          partition,
			Amount:             e.Amount.String(),
			TxHash:             eventLog.TxHash,
			BlockNumber:        eventLog.BlockNumber,
			BlockTime:          blockTime,
		})
		if err != nil {
			return fmt.Errorf("ProcessCTFPositionEvent(Merge) failed: %w", err)
		}
	case *contract.PayoutRedemptionEvent:
		ctx.Log.Infof("CTF PayoutRedemption: redeemer=%s, conditionId=%s, payout=%s",
			e.Redeemer.Hex(), e.ConditionId.Hex(), e.Payout.String())
		indexSets := make([]string, 0, len(e.IndexSets))
		for _, is := range e.IndexSets {
			indexSets = append(indexSets, is.String())
		}
		_, err := p.rpcClient.MarketcenterClient.ProcessCTFPositionEvent(ctx.Ctx, &marketcenterPb.ProcessCTFPositionEventRequest{
			EventType:          marketcenterPb.ProcessCTFPositionEventRequest_PAYOUT_REDEMPTION,
			Stakeholder:        e.Redeemer.Hex(),
			CollateralToken:    e.CollateralToken.Hex(),
			ParentCollectionId: e.ParentCollectionId.Hex(),
			ConditionId:        e.ConditionId.Hex(),
			IndexSets:          indexSets,
			Amount:             e.Payout.String(),
			TxHash:             eventLog.TxHash,
			BlockNumber:        eventLog.BlockNumber,
			BlockTime:          blockTime,
		})
		if err != nil {
			return fmt.Errorf("ProcessCTFPositionEvent(Redemption) failed: %w", err)
		}
	case *contract.TransferSingleEvent:
		ctx.Log.Debugf("CTF TransferSingle: from=%s, to=%s, id=%s, value=%s",
			e.From.Hex(), e.To.Hex(), e.Id.String(), e.Value.String())
		_, err := p.rpcClient.MarketcenterClient.ProcessCTFTransferEvent(ctx.Ctx, &marketcenterPb.ProcessCTFTransferEventRequest{
			EventType:   marketcenterPb.ProcessCTFTransferEventRequest_TRANSFER_SINGLE,
			Operator:    e.Operator.Hex(),
			From:        e.From.Hex(),
			To:          e.To.Hex(),
			Ids:         []string{e.Id.String()},
			Values:      []string{e.Value.String()},
			TxHash:      eventLog.TxHash,
			BlockNumber: eventLog.BlockNumber,
			BlockTime:   blockTime,
		})
		if err != nil {
			return fmt.Errorf("ProcessCTFTransferEvent(Single) failed: %w", err)
		}
	case *contract.TransferBatchEvent:
		ctx.Log.Debugf("CTF TransferBatch: from=%s, to=%s, ids=%d",
			e.From.Hex(), e.To.Hex(), len(e.Ids))
		ids := make([]string, 0, len(e.Ids))
		for _, id := range e.Ids {
			ids = append(ids, id.String())
		}
		values := make([]string, 0, len(e.Values))
		for _, v := range e.Values {
			values = append(values, v.String())
		}
		_, err := p.rpcClient.MarketcenterClient.ProcessCTFTransferEvent(ctx.Ctx, &marketcenterPb.ProcessCTFTransferEventRequest{
			EventType:   marketcenterPb.ProcessCTFTransferEventRequest_TRANSFER_BATCH,
			Operator:    e.Operator.Hex(),
			From:        e.From.Hex(),
			To:          e.To.Hex(),
			Ids:         ids,
			Values:      values,
			TxHash:      eventLog.TxHash,
			BlockNumber: eventLog.BlockNumber,
			BlockTime:   blockTime,
		})
		if err != nil {
			return fmt.Errorf("ProcessCTFTransferEvent(Batch) failed: %w", err)
		}
	default:
		ctx.Log.Debugf("未处理的CTF事件类型: %T", e)
	}

	return nil
}

// handlePredictionCTFMarketEvent 处理PredictionCTF市场事件
func (p *EventProcessor) handlePredictionCTFMarketEvent(
	ctx com.Ctx,
	eventLog *model.EventLog,
	chainData *ChainData,
	marketMap map[string]*marketcenterPb.GetMarketsAndOptionsForBlockListenerResponse_Market,
	userInfoMap map[string]*usercenterPb.GetUsersInfoByAddressesReply_User,
) error {
	marketAddress := eventLog.Address
	marketInfo, exists := marketMap[marketAddress]
	if !exists {
		return fmt.Errorf("PredictionCTF市场信息不存在: %s", marketAddress)
	}

	predictionCTFEvent, err := p.arbClient.Contract.PredictionCTFContract.ParseEvent(*eventLog.ToChainLog())
	if err != nil {
		return fmt.Errorf("解析PredictionCTF事件失败: %w", err)
	}

	// 查询区块时间
	blockTime, ok := chainData.blockNumberToTime[eventLog.BlockNumber]
	if !ok {
		return fmt.Errorf("区块时间不存在: %d", eventLog.BlockNumber)
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

	return p.routeCTFEvent(ctx, predictionCTFEvent, eventParams)
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

// routeCTFEvent 分发PredictionCTF事件处理器
func (p *EventProcessor) routeCTFEvent(ctx com.Ctx, predictionCTFEvent interface{}, params *EventParams) error {
	switch e := predictionCTFEvent.(type) {
	case *contract.DepositedEvent:
		ctx.Log.Infof("处理CTF存款事件: %s", params.EventLog.TxHash)
		return p.handleDepositEvent(ctx, e, params)
	case *contract.WithdrawnEvent:
		ctx.Log.Infof("处理CTF提款事件: %s", params.EventLog.TxHash)
		return p.handleWithdrawEvent(ctx, e, params)
	case *contract.SwappedEvent:
		ctx.Log.Infof("处理CTF交换事件: %s", params.EventLog.TxHash)
		return p.handleSwapEvent(ctx, e, params)
	case *contract.CTFMarketResolvedEvent:
		ctx.Log.Infof("处理CTF市场解决事件: %s, address: %s", params.EventLog.TxHash, params.EventLog.Address)
		_, err := p.rpcClient.MarketcenterClient.ProcessCTFMarketResolvedEvent(ctx.Ctx, &marketcenterPb.ProcessCTFMarketResolvedEventRequest{
			MarketAddress: params.EventLog.Address,
			TxHash:        params.EventLog.TxHash,
			BlockNumber:   params.EventLog.BlockNumber,
			BlockTime:     params.BlockTime,
			OptionPrices:  params.OptionPrices,
			BaseTokenType: params.BaseTokenType,
		})
		if err != nil {
			ctx.Log.Errorf("ProcessCTFMarketResolvedEvent failed: %v", err)
			return fmt.Errorf("ProcessCTFMarketResolvedEvent failed: %w", err)
		}
		return nil
	case *contract.LiquidityAddedEvent:
		ctx.Log.Infof("处理CTF流动性添加事件: %s", params.EventLog.TxHash)
		userAddress := e.User.Hex()
		uid := ""
		if userInfo, exists := params.UserInfoMap[userAddress]; exists && userInfo.Uid != "" {
			uid = userInfo.Uid
		}
		_, err := p.rpcClient.MarketcenterClient.ProcessCTFLiquidityEvent(ctx.Ctx, &marketcenterPb.ProcessCTFLiquidityEventRequest{
			EventType:     marketcenterPb.ProcessCTFLiquidityEventRequest_LIQUIDITY_ADDED,
			Uid:           uid,
			UserAddress:   userAddress,
			MarketAddress: params.EventLog.Address,
			Amount:        e.Amount.String(),
			LpAmount:      e.LpAmount.String(),
			TxHash:        params.EventLog.TxHash,
			BlockNumber:   params.EventLog.BlockNumber,
			BlockTime:     params.BlockTime,
			OptionPrices:  params.OptionPrices,
			BaseTokenType: params.BaseTokenType,
		})
		if err != nil {
			ctx.Log.Errorf("ProcessCTFLiquidityEvent(Added) failed: %v", err)
			return fmt.Errorf("ProcessCTFLiquidityEvent(Added) failed: %w", err)
		}
		return nil
	case *contract.CTFLiquidityRemovedEvent:
		ctx.Log.Infof("处理CTF流动性移除事件: %s", params.EventLog.TxHash)
		userAddress := e.User.Hex()
		uid := ""
		if userInfo, exists := params.UserInfoMap[userAddress]; exists && userInfo.Uid != "" {
			uid = userInfo.Uid
		}
		excessPositions := make([]string, 0, len(e.ExcessPositions))
		for _, ep := range e.ExcessPositions {
			excessPositions = append(excessPositions, ep.String())
		}
		_, err := p.rpcClient.MarketcenterClient.ProcessCTFLiquidityEvent(ctx.Ctx, &marketcenterPb.ProcessCTFLiquidityEventRequest{
			EventType:       marketcenterPb.ProcessCTFLiquidityEventRequest_LIQUIDITY_REMOVED,
			Uid:             uid,
			UserAddress:     userAddress,
			MarketAddress:   params.EventLog.Address,
			Amount:          e.Amount.String(),
			LpAmount:        e.LpAmount.String(),
			ExcessPositions: excessPositions,
			TxHash:          params.EventLog.TxHash,
			BlockNumber:     params.EventLog.BlockNumber,
			BlockTime:       params.BlockTime,
			OptionPrices:    params.OptionPrices,
			BaseTokenType:   params.BaseTokenType,
		})
		if err != nil {
			ctx.Log.Errorf("ProcessCTFLiquidityEvent(Removed) failed: %v", err)
			return fmt.Errorf("ProcessCTFLiquidityEvent(Removed) failed: %w", err)
		}
		return nil
	case *contract.FeeSetEvent:
		ctx.Log.Infof("处理CTF费率设置事件: %s", params.EventLog.TxHash)
		return nil
	case *contract.CTFFeeCollectedEvent:
		ctx.Log.Infof("处理CTF费用收集事件: %s, collector: %s, amount: %s",
			params.EventLog.TxHash, e.Collector.Hex(), e.Amount.String())
		return nil
	default:
		ctx.Log.Infof("未处理的PredictionCTF事件类型: %T", e)
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

// getBalance 获取用户代币余额
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
