package biz

import (
	"context"
	"fmt"
	usercenterPb "market-proto/proto/market-service/usercenter/v1"
	"runtime/debug"
	"sync"
	"time"

	"block-listener/internal/conf"
	"block-listener/internal/contract"
	"block-listener/internal/data"
	"block-listener/internal/model"
	"block-listener/pkg/alarm"
	com "block-listener/pkg/common"

	"github.com/ethereum/go-ethereum/common"
	ethereumType "github.com/ethereum/go-ethereum/core/types"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/google/uuid"
)

// const FactoryContractAddress = "0x2279B7A0a67DB372996a5FaB50D91eAA73d2eBe6"

// BlockScanner
type BlockScanner struct {
	db          *data.DbClient
	arbClient   *data.ArbClient
	redisClient *data.RedisClient
	rpcClient   *data.RpcClient

	cfg    *conf.Data_Blockchain
	custom *conf.Custom
	log    log.Logger

	// 解析器
	factoryParser    *contract.FactoryContract
	predictionParser *contract.PredictionContract
	erc20Parser      *contract.ERC20Contract

	// 控制监听的通道
	stopCh    chan struct{}
	doneCh    chan struct{} // 用于通知监听已完全停止
	mu        sync.RWMutex  // 保护并发访问的互斥锁
	isRunning bool          // 监听服务运行状态
}

// NewBlockScanner
func NewBlockScanner(
	db *data.DbClient,
	arbClient *data.ArbClient,
	redisClient *data.RedisClient,
	rpcClient *data.RpcClient,
	bc *conf.Bootstrap,
	logger log.Logger,
) *BlockScanner {
	return &BlockScanner{
		db:               db,
		arbClient:        arbClient,
		redisClient:      redisClient,
		rpcClient:        rpcClient,
		cfg:              bc.Data.Blockchain,
		custom:           bc.Custom,
		log:              logger,
		factoryParser:    arbClient.Contract.FactoryContract,
		predictionParser: arbClient.Contract.PredictionContract,
		erc20Parser:      arbClient.Contract.Erc20Contract,
		stopCh:           make(chan struct{}),
		doneCh:           make(chan struct{}),
		mu:               sync.RWMutex{},
		isRunning:        false,
	}
}

// Start 开始区块监听
func (scanner *BlockScanner) Start(ctx context.Context) {
	defer func() {
		if err := recover(); err != nil {
			log.Errorf("block scanner panic: %v, stack: %s", err, string(debug.Stack()))
		}
	}()

	scanner.mu.Lock()
	if scanner.isRunning {
		scanner.mu.Unlock()
		return
	}
	scanner.isRunning = true
	scanner.mu.Unlock()

	scanner.scan(ctx)
}

// Stop 停止区块监听
func (scanner *BlockScanner) Stop(ctx context.Context) error {
	scanner.mu.Lock()
	if !scanner.isRunning {
		scanner.mu.Unlock()
		return nil
	}
	scanner.mu.Unlock()

	close(scanner.stopCh)

	select {
	case <-scanner.doneCh:
		log.Info("区块监听服务已正常停止")
	case <-time.After(time.Second * 30):
		log.Warn("等待区块监听停止超时")
	case <-ctx.Done():
		log.Warn("上下文取消，区块监听停止")
	}

	scanner.mu.Lock()
	scanner.isRunning = false
	scanner.mu.Unlock()
	return nil
}

// scan 区块扫描主循环
func (scanner *BlockScanner) scan(c context.Context) {
	defer close(scanner.doneCh)

	batchScanSize := scanner.cfg.BatchScanSize

	for {
		c = context.WithValue(c, "traceId", uuid.New().String())
		ctx := com.NewBaseCtx(c, scanner.log)
		select {
		case <-scanner.stopCh:
			ctx.Log.Info("收到停止信号，退出循环爬块")
			return
		case <-c.Done():
			ctx.Log.Info("上下文取消，退出循环爬块")
			return
		default:
			if lastProcessedBlock, err := scanner.run(ctx); err != nil {
				ctx.Log.Errorf("处理区块扫描失败: %v", err)
				traceId, _ := ctx.Ctx.Value("traceId").(string)
				msg := fmt.Sprintf("扫描区块[%d-%d] 失败: [%+v], traceId: [%s]", lastProcessedBlock, lastProcessedBlock+batchScanSize, err, traceId)
				alarm.Lark.Send(msg)
				time.Sleep(1 * time.Second)
			}
		}
	}
}

func (scanner *BlockScanner) run(ctx com.Ctx) (uint64, error) {
	lockKey := "block_scanner_lock"
	lockID, ok, err := scanner.redisClient.AcquireLock(ctx.Ctx, lockKey, 30*time.Second)
	if err != nil {
		ctx.Log.Errorf("获取锁失败: %v", err)
		return 0, err
	}
	if !ok {
		time.Sleep(2 * time.Second)
		ctx.Log.Info("未获取到锁")
		return 0, nil
	}
	defer scanner.redisClient.ReleaseLock(ctx.Ctx, lockKey, lockID)

	return scanner.processScanBlock(ctx)
}

func (scanner *BlockScanner) processScanBlock(ctx com.Ctx) (uint64, error) {

	lastProcessedBlock, err := scanner.redisClient.GetLastProcessedBlock(ctx.Ctx)
	if err != nil {
		ctx.Log.Errorf("get last processed block from redisfailed: %v", err)
		return 0, err
	}
	if lastProcessedBlock == 0 {
		lastProcessedBlock = scanner.cfg.StartBlock
	}

	latestBlock, err := scanner.arbClient.GetLatestBlockNumber(ctx.Ctx)
	if err != nil {
		return lastProcessedBlock, err
	}

	if latestBlock <= lastProcessedBlock {
		// 如果没有新区块，短暂休眠后继续
		time.Sleep(1 * time.Second)
		return lastProcessedBlock, nil
	}

	batchScanSize := scanner.cfg.BatchScanSize
	if batchScanSize == 0 {
		batchScanSize = 100
	}

	// 计算待处理区块数量
	blocksToProcess := latestBlock - lastProcessedBlock

	ctx.Log.Infof("当前进度: 已处理至 %d, 链上最新 %d, 相差 %d 个区块",
		lastProcessedBlock, latestBlock, blocksToProcess)

	// 分批处理区块
	startBlock := lastProcessedBlock + 1
	endBlock := startBlock

	// 确定本次处理的结束区块
	if blocksToProcess > batchScanSize {
		// 如果相差太多，只处理一批
		endBlock = startBlock + batchScanSize - 1
		ctx.Log.Infof("区块差距较大，分批处理：%d - %d", startBlock, endBlock)
	} else {
		// 否则处理所有待处理区块
		endBlock = latestBlock
	}

	// 处理当前批次的区块
	startTime := time.Now()
	err = scanner.processBlocks(ctx, startBlock, endBlock)
	processingTime := time.Since(startTime)

	if err != nil {
		ctx.Log.Errorf("处理区块 %d - %d 失败: %v", startBlock, endBlock, err)
		return lastProcessedBlock, err
	}

	// 记录处理耗时
	ctx.Log.Infof("处理区块 %d - %d 成功，耗时: %v, 平均每块: %v",
		startBlock, endBlock, processingTime,
		processingTime/time.Duration(endBlock-startBlock+1))

	// 更新最后处理的区块高度
	lastProcessedBlock = endBlock

	// 更新处理进度到Redis
	if err := scanner.redisClient.UpdateLastProcessedBlock(ctx.Ctx, lastProcessedBlock); err != nil {
		ctx.Log.Errorf("更新处理进度失败: %v", err)
		return lastProcessedBlock, err
	}

	// 如果一次处理的区块数量达到批次上限，不要休眠，立即处理下一批
	if endBlock-startBlock+1 >= batchScanSize {
		return lastProcessedBlock, nil
	}

	// 如果处理完所有积压区块，短暂休眠
	time.Sleep(1 * time.Second)
	return lastProcessedBlock, nil
}

func (scanner *BlockScanner) processBlocks(ctx com.Ctx, fromBlock, toBlock uint64) error {
	// 处理工厂合约事件
	factoryLogs, err := scanner.getFactoryEvents(ctx, fromBlock, toBlock)
	if err != nil {
		ctx.Log.Errorf("获取工厂合约事件失败: %v", err)
		return err
	}

	// 初始化事件日志切片，用于后续批量保存
	eventLogs := make([]*model.EventLog, 0, len(factoryLogs))

	// 处理工厂合约的创建事件
	for _, log := range factoryLogs {
		if log.Removed {
			ctx.Log.Infof("工厂合约事件已删除: %s", log.TxHash.Hex())
			continue
		}

		eventLog := model.EventLog{}
		eventLog.ToEventLog(&log)
		eventLog.Type = model.TypeFactory
		eventLogs = append(eventLogs, &eventLog)
	}

	// 处理预测市场合约事件
	predictionLogs, err := scanner.getPredictionEvents(ctx, fromBlock, toBlock)
	if err != nil {
		ctx.Log.Errorf("获取预测市场合约事件失败: %v", err)
		return err
	}

	for _, log := range predictionLogs {
		if log.Removed {
			ctx.Log.Infof("预测市场合约事件已删除: %s", log.TxHash.Hex())
			continue
		}

		eventLog := model.EventLog{}
		eventLog.ToEventLog(&log)
		eventLog.Type = model.TypePrediction
		eventLogs = append(eventLogs, &eventLog)
	}

	transferLogs, err := scanner.getErc20TransferEvents(ctx, fromBlock, toBlock)
	if err != nil {
		ctx.Log.Errorf("获取erc20代币转移事件失败: %v", err)
		return err
	}

	filteredTransferLogs, err := scanner.filterTransferLogs(ctx, transferLogs)
	if err != nil {
		ctx.Log.Errorf("过滤erc20代币转移事件失败: %v", err)
		return err
	}

	for _, log := range filteredTransferLogs {

		eventLog := model.EventLog{}
		eventLog.ToEventLog(&log)
		eventLog.Type = model.TypeErc20Transfer
		eventLogs = append(eventLogs, &eventLog)
	}

	// 批量保存事件
	return scanner.saveEventLogs(ctx, eventLogs)
}

func (scanner *BlockScanner) filterTransferLogs(ctx com.Ctx, transferLogs []ethereumType.Log) ([]ethereumType.Log, error) {
	queryUserAddressList := make([]string, 0)
	for _, log := range transferLogs {
		transferEvent, err := scanner.erc20Parser.ParseTransferEvent(log)
		if err != nil {
			ctx.Log.Errorf("解析ERC20 Transfer事件失败: %v", err)
			continue
		}
		fromAddress := transferEvent.From.Hex()
		toAddress := transferEvent.To.Hex()
		queryUserAddressList = append(queryUserAddressList, fromAddress)
		queryUserAddressList = append(queryUserAddressList, toAddress)
	}

	userInfoMap := make(map[string]*usercenterPb.GetUsersInfoByAddressesReply_User)

	if len(queryUserAddressList) == 0 {
		return make([]ethereumType.Log, 0), nil
	}

	resp, err := scanner.rpcClient.UsercenterClient.GetUsersInfoByAddresses(ctx.Ctx, &usercenterPb.GetUsersInfoByAddressesRequest{
		Addresses: queryUserAddressList,
	})
	if err != nil {
		return nil, err
	}

	for _, user := range resp.Users {
		userInfoMap[user.Address] = user
	}

	filteredTransferLogs := make([]ethereumType.Log, 0, len(transferLogs))

	for _, log := range transferLogs {
		transferEvent, err := scanner.erc20Parser.ParseTransferEvent(log)
		if err != nil {
			ctx.Log.Errorf("解析ERC20 Transfer事件失败: %v", err)
			continue
		}
		fromAddress := transferEvent.From.Hex()
		toAddress := transferEvent.To.Hex()

		if _, ok := userInfoMap[fromAddress]; ok {
			filteredTransferLogs = append(filteredTransferLogs, log)
			continue
		}
		if _, ok := userInfoMap[toAddress]; ok {
			filteredTransferLogs = append(filteredTransferLogs, log)
			continue
		}
	}

	return filteredTransferLogs, nil
}

// getFactoryEvents 获取工厂合约事件
func (scanner *BlockScanner) getFactoryEvents(ctx com.Ctx, fromBlock, toBlock uint64) ([]ethereumType.Log, error) {
	// 获取工厂合约的PredictionCreated事件签名和地址
	factoryCreatedEventHash := common.HexToHash(scanner.factoryParser.GetEventSignature(contract.EventTypePredictionCreated))
	factoryAddress := common.HexToAddress(scanner.cfg.FactoryContractAddress)

	ctx.Log.Infof("start GetBlockRangeLogsWithAddress fromBlock: %d, toBlock: %d, factoryCreatedEventHash: %s, factoryAddress: %s",
		fromBlock, toBlock, factoryCreatedEventHash.Hex(), factoryAddress.Hex())

	// 查询工厂合约在指定区块范围内的创建事件
	createPredictionLogs, err := scanner.arbClient.GetBlockRangeLogsWithAddress(
		ctx.Ctx, fromBlock, toBlock, []common.Hash{factoryCreatedEventHash}, []common.Address{factoryAddress})
	if err != nil {
		ctx.Log.Errorf("获取工厂合约创建事件失败: %v", err)
		return nil, err
	}

	ctx.Log.Infof("GetBlockRangeLogsWithAddress result: %d", len(createPredictionLogs))

	return createPredictionLogs, nil
}

// getPredictionEvents 获取预测市场合约事件
func (scanner *BlockScanner) getPredictionEvents(ctx com.Ctx, fromBlock, toBlock uint64) ([]ethereumType.Log, error) {
	// 定义需要监听的预测市场事件类型
	eventHashes := []common.Hash{
		common.HexToHash(scanner.predictionParser.GetEventSignature(contract.EventTypeSwapped)),           // 交换事件
		common.HexToHash(scanner.predictionParser.GetEventSignature(contract.EventTypeWithdrawn)),         // 提现事件
		common.HexToHash(scanner.predictionParser.GetEventSignature(contract.EventTypeDeposited)),         // 存款事件
		common.HexToHash(scanner.predictionParser.GetEventSignature(contract.EventTypeClaimed)),           // 认领事件
		common.HexToHash(scanner.predictionParser.GetEventSignature(contract.EventTypeSettling)),          // 结算事件
		common.HexToHash(scanner.predictionParser.GetEventSignature(contract.EventTypeAssertionDisputed)), // 争议事件
		common.HexToHash(scanner.predictionParser.GetEventSignature(contract.EventTypeAssertionResolved)), // 解决事件
	}

	// 查询预测市场合约的事件日志
	txLogs, err := scanner.arbClient.GetBlockRangeLogsWithAddress(ctx.Ctx, fromBlock, toBlock, eventHashes, []common.Address{})
	if err != nil {
		ctx.Log.Errorf("获取预测市场交易事件失败: %v", err)
		return nil, err
	}

	return txLogs, nil
}

// getErc20TransferEvents 获取erc20代币转移事件
func (scanner *BlockScanner) getErc20TransferEvents(ctx com.Ctx, fromBlock, toBlock uint64) ([]ethereumType.Log, error) {
	transferEventHash, err := scanner.erc20Parser.GetTransferEventSignature()
	if err != nil {
		ctx.Log.Errorf("获取erc20代币转移事件签名失败: %v", err)
		return nil, err
	}

	pointsAddress := common.HexToAddress(scanner.custom.AssetTokens.Points.Address)
	usdcAddress := common.HexToAddress(scanner.custom.AssetTokens.Usdc.Address)
	txLogs, err := scanner.arbClient.GetBlockRangeLogsWithAddress(ctx.Ctx, fromBlock, toBlock, []common.Hash{transferEventHash}, []common.Address{pointsAddress, usdcAddress})
	if err != nil {
		ctx.Log.Errorf("获取erc20代币转移事件失败: %v", err)
		return nil, err
	}

	return txLogs, nil
}

// saveEventLogs 批量保存事件日志
func (scanner *BlockScanner) saveEventLogs(ctx com.Ctx, eventLogs []*model.EventLog) error {
	if len(eventLogs) > 0 {
		if err := scanner.db.BatchCreateEventLogs(ctx.Ctx, eventLogs); err != nil {
			ctx.Log.Errorf("批量创建事件失败: %v", err)
			return err
		}
	}
	return nil
}
