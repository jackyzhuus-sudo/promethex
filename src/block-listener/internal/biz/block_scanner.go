package biz

import (
	"context"
	"fmt"
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

// BlockScanner
type BlockScanner struct {
	db          *data.DbClient
	arbClient   *data.ArbClient
	redisClient *data.RedisClient
	rpcClient   *data.RpcClient

	cfg *conf.Data_Blockchain
	log log.Logger

	// 解析器
	conditionalTokensParser *contract.ConditionalTokensContract
	predictionCTFParser     *contract.PredictionCTFContract

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
		db:                      db,
		arbClient:               arbClient,
		redisClient:             redisClient,
		rpcClient:               rpcClient,
		cfg:                     bc.Data.Blockchain,
		log:                     logger,
		conditionalTokensParser: arbClient.Contract.ConditionalTokensContract,
		predictionCTFParser:     arbClient.Contract.PredictionCTFContract,
		stopCh:                  make(chan struct{}),
		doneCh:                  make(chan struct{}),
		mu:                      sync.RWMutex{},
		isRunning:               false,
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
	eventLogs := make([]*model.EventLog, 0)

	// 处理ConditionalTokens合约事件
	ctfLogs, err := scanner.getCTFEvents(ctx, fromBlock, toBlock)
	if err != nil {
		ctx.Log.Errorf("获取ConditionalTokens合约事件失败: %v", err)
		return err
	}

	for _, log := range ctfLogs {
		if log.Removed {
			ctx.Log.Infof("ConditionalTokens合约事件已删除: %s", log.TxHash.Hex())
			continue
		}

		eventLog := model.EventLog{}
		eventLog.ToEventLog(&log)
		eventLog.Type = model.TypeCTF
		eventLogs = append(eventLogs, &eventLog)
	}

	// 处理PredictionCTF池合约事件
	predictionCTFLogs, err := scanner.getPredictionCTFEvents(ctx, fromBlock, toBlock)
	if err != nil {
		ctx.Log.Errorf("获取PredictionCTF池合约事件失败: %v", err)
		return err
	}

	for _, log := range predictionCTFLogs {
		if log.Removed {
			ctx.Log.Infof("PredictionCTF池合约事件已删除: %s", log.TxHash.Hex())
			continue
		}

		eventLog := model.EventLog{}
		eventLog.ToEventLog(&log)
		eventLog.Type = model.TypePredictionCTF
		eventLogs = append(eventLogs, &eventLog)
	}

	// 批量保存事件
	return scanner.saveEventLogs(ctx, eventLogs)
}

// getCTFEvents 获取ConditionalTokens合约事件
func (scanner *BlockScanner) getCTFEvents(ctx com.Ctx, fromBlock, toBlock uint64) ([]ethereumType.Log, error) {
	ctfAddress := scanner.cfg.ConditionalTokensAddress
	if ctfAddress == "" {
		return nil, nil // 未配置CTF合约地址，跳过
	}

	// 监听所有CTF事件
	eventHashes := scanner.conditionalTokensParser.GetAllEventSignatures()

	txLogs, err := scanner.arbClient.GetBlockRangeLogsWithAddress(
		ctx.Ctx, fromBlock, toBlock, eventHashes, []common.Address{common.HexToAddress(ctfAddress)})
	if err != nil {
		ctx.Log.Errorf("获取ConditionalTokens合约事件失败: %v", err)
		return nil, err
	}

	return txLogs, nil
}

// getPredictionCTFEvents 获取PredictionCTF池合约事件
func (scanner *BlockScanner) getPredictionCTFEvents(ctx com.Ctx, fromBlock, toBlock uint64) ([]ethereumType.Log, error) {
	ctfAddresses := scanner.cfg.PredictionCtfAddresses
	if len(ctfAddresses) == 0 {
		return nil, nil // 未配置PredictionCTF池地址，跳过
	}

	// 定义需要监听的PredictionCTF事件类型
	eventHashes := []common.Hash{
		common.HexToHash(scanner.predictionCTFParser.GetEventSignature(contract.EventTypeCTFSwapped)),
		common.HexToHash(scanner.predictionCTFParser.GetEventSignature(contract.EventTypeCTFWithdrawn)),
		common.HexToHash(scanner.predictionCTFParser.GetEventSignature(contract.EventTypeCTFDeposited)),
		common.HexToHash(scanner.predictionCTFParser.GetEventSignature(contract.EventTypeCTFLiquidityAdded)),
		common.HexToHash(scanner.predictionCTFParser.GetEventSignature(contract.EventTypeCTFLiquidityRemoved)),
		common.HexToHash(scanner.predictionCTFParser.GetEventSignature(contract.EventTypeCTFMarketResolved)),
		common.HexToHash(scanner.predictionCTFParser.GetEventSignature(contract.EventTypeCTFFeeCollected)),
	}

	// 构建地址列表
	addresses := make([]common.Address, len(ctfAddresses))
	for i, addr := range ctfAddresses {
		addresses[i] = common.HexToAddress(addr)
	}

	txLogs, err := scanner.arbClient.GetBlockRangeLogsWithAddress(ctx.Ctx, fromBlock, toBlock, eventHashes, addresses)
	if err != nil {
		ctx.Log.Errorf("获取PredictionCTF池合约事件失败: %v", err)
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
