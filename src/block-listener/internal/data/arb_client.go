package data

import (
	"context"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"time"

	"block-listener/internal/conf"
	"block-listener/internal/contract"
	"block-listener/pkg/alarm"
	com "block-listener/pkg/common"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/go-kratos/kratos/v2/log"
)

const (
	// MultiCallABI
	MultiCallABI = `[{"inputs":[{"internalType":"bool","name":"requireSuccess","type":"bool"},{"components":[{"internalType":"address","name":"target","type":"address"},{"internalType":"bytes","name":"callData","type":"bytes"}],"internalType":"struct Multicall2.Call[]","name":"calls","type":"tuple[]"}],"name":"tryAggregate","outputs":[{"components":[{"internalType":"bool","name":"success","type":"bool"},{"internalType":"bytes","name":"returnData","type":"bytes"}],"internalType":"struct Multicall2.Result[]","name":"returnData","type":"tuple[]"}],"stateMutability":"view","type":"function"}]`
	// MultiCallAddress
	MultiCallAddress = "0xcA11bde05977b3631167028862bE2a173976CA11"
)

// ArbClient Arbitrum链客户端，支持后台自动切换RPC节点
type ArbClient struct {
	// 当前使用的连接
	client    *ethclient.Client
	rpcClient *rpc.Client
	mutex     sync.RWMutex

	// 配置和日志
	log      log.Logger
	Contract *contract.Contract

	// 多RPC支持
	rpcUrls    []string
	currentURL string
	stopCh     chan bool
}

// NewArbClient 创建新的Arbitrum客户端
func NewArbClient(bc *conf.Bootstrap, logger log.Logger, contract *contract.Contract) (*ArbClient, error) {
	log := log.NewHelper(logger)

	// 准备RPC URL列表
	var urls []string
	if len(bc.Data.Blockchain.ArbRpcUrls) > 0 {
		urls = bc.Data.Blockchain.ArbRpcUrls
	} else {
		return nil, fmt.Errorf("未配置任何RPC URL")
	}

	client := &ArbClient{
		log:      logger,
		Contract: contract,
		rpcUrls:  urls,
		stopCh:   make(chan bool, 1),
	}

	// 尝试连接第一个可用的RPC节点
	if err := client.connectToFirstAvailableNode(); err != nil {
		return nil, fmt.Errorf("无法连接到任何RPC节点: %v", err)
	}

	// 启动后台健康检查
	go client.startHealthMonitoring()

	log.Infof("Arbitrum客户端初始化完成，共%d个RPC节点，当前使用: %s",
		len(client.rpcUrls), client.currentURL)
	return client, nil
}

// connectToFirstAvailableNode 连接到第一个可用的节点
func (c *ArbClient) connectToFirstAvailableNode() error {
	for _, url := range c.rpcUrls {
		if err := c.connectToNode(url); err == nil {
			return nil
		} else {
			log := log.NewHelper(c.log)
			log.Errorf("连接RPC节点 %s 失败: %v", url, err)
		}
	}
	return fmt.Errorf("所有RPC节点都不可用")
}

// connectToNode 连接到指定的RPC节点
func (c *ArbClient) connectToNode(url string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 创建RPC客户端
	rpcClient, err := rpc.DialContext(ctx, url)
	if err != nil {
		return fmt.Errorf("连接RPC失败: %v", err)
	}

	// 创建以太坊客户端
	client := ethclient.NewClient(rpcClient)

	// 验证连接
	_, err = client.BlockNumber(ctx)
	if err != nil {
		rpcClient.Close()
		return fmt.Errorf("验证连接失败: %v", err)
	}

	// 原子替换当前连接
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// 关闭旧连接
	if c.rpcClient != nil {
		c.rpcClient.Close()
	}

	c.client = client
	c.rpcClient = rpcClient
	c.currentURL = url

	return nil
}

// startHealthMonitoring 启动健康监控
func (c *ArbClient) startHealthMonitoring() {
	ticker := time.NewTicker(30 * time.Second) // 每30秒检查一次
	defer ticker.Stop()

	for {
		select {
		case <-c.stopCh:
			return
		case <-ticker.C:
			c.checkAndSwitchIfNeeded()
		}
	}
}

// checkAndSwitchIfNeeded 检查当前节点健康状态，必要时切换
func (c *ArbClient) checkAndSwitchIfNeeded() {
	// 检查当前节点是否健康
	if c.isCurrentNodeHealthy() {
		return // 当前节点正常，不需要切换
	}

	alarm.Lark.Send(fmt.Sprintf("当前RPC节点 %s 不可用，尝试切换", c.currentURL))

	log := log.NewHelper(c.log)
	log.Warnf("当前RPC节点 %s 不可用，尝试切换", c.currentURL)

	// 尝试切换到其他可用节点
	for _, url := range c.rpcUrls {
		if url == c.currentURL {
			continue // 跳过当前节点
		}

		if err := c.connectToNode(url); err == nil {
			log.Infof("RPC节点切换成功: %s -> %s", c.getCurrentURL(), url)
			alarm.Lark.Send(fmt.Sprintf("RPC节点切换成功: %s -> %s", c.getCurrentURL(), url))
			return
		}
	}

	// 如果没有其他可用节点，尝试重连当前节点
	if err := c.connectToNode(c.currentURL); err == nil {
		log.Infof("当前RPC节点重连成功: %s", c.currentURL)
		alarm.Lark.Send(fmt.Sprintf("当前RPC节点重连成功: %s", c.currentURL))
	} else {
		log.Errorf("所有RPC节点都不可用")
		alarm.Lark.Send("所有RPC节点都不可用")
	}
}

// isCurrentNodeHealthy 检查当前节点是否健康
func (c *ArbClient) isCurrentNodeHealthy() bool {
	c.mutex.RLock()
	client := c.client
	c.mutex.RUnlock()

	if client == nil {
		return false
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := client.BlockNumber(ctx)
	return err == nil
}

// getCurrentURL 获取当前使用的RPC URL
func (c *ArbClient) getCurrentURL() string {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.currentURL
}

// getClient 获取当前客户端连接（线程安全）
func (c *ArbClient) getClient() *ethclient.Client {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.client
}

// getRPCClient 获取当前RPC客户端连接（线程安全）
func (c *ArbClient) getRPCClient() *rpc.Client {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.rpcClient
}

// Close 关闭客户端连接
func (c *ArbClient) Close() {
	c.stopCh <- true
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.rpcClient != nil {
		c.rpcClient.Close()
	}
}

// GetLatestBlockNumber 获取最新区块号
func (c *ArbClient) GetLatestBlockNumber(ctx context.Context) (uint64, error) {
	com.NewBaseCtx(ctx, c.log).Log.Infof("GetLatestBlockNumber")
	header, err := c.getClient().HeaderByNumber(ctx, nil)
	if err != nil {
		return 0, err
	}
	return header.Number.Uint64(), nil
}

// GetBlockByNumber 根据区块号获取区块
func (c *ArbClient) GetBlockByNumber(ctx context.Context, blockNum uint64) (*types.Block, error) {
	com.NewBaseCtx(ctx, c.log).Log.Infof("GetBlockByNumber")
	return c.getClient().BlockByNumber(ctx, big.NewInt(int64(blockNum)))
}

// GetBlockRangeLogsWithAddress 获取指定区块范围内、指定事件签名和地址的日志
func (c *ArbClient) GetBlockRangeLogsWithAddress(ctx context.Context, fromBlock, toBlock uint64, eventHashes []common.Hash, addresses []common.Address) ([]types.Log, error) {

	comCtx := com.NewBaseCtx(ctx, c.log)
	comCtx.Log.Infof("GetBlockRangeLogsWithAddress")
	var topics [][]common.Hash

	// 将事件hash添加到topics
	if len(eventHashes) > 0 {
		topics = append(topics, eventHashes)
	}

	// 构建查询过滤器
	query := ethereum.FilterQuery{
		FromBlock: big.NewInt(int64(fromBlock)),
		ToBlock:   big.NewInt(int64(toBlock)),
		Addresses: addresses,
		Topics:    topics,
	}

	comCtx.Log.Infof("FilterLogs: 区块范围[%d-%d], 事件数量[%d], 地址数量[%d], 事件hash[%s] 地址[%s]",
		fromBlock, toBlock, len(eventHashes), len(addresses), eventHashes, addresses)
	// 发送请求
	logs, err := c.getClient().FilterLogs(ctx, query)
	if err != nil {
		comCtx.Log.Errorf("获取区块日志失败: from=%d, to=%d, error=%v", fromBlock, toBlock, err)
		return nil, err
	}

	return logs, nil
}

// multiCallInput 表示一个multi_call调用的输入
type multiCallInput struct {
	Target   common.Address // 目标合约地址
	CallData []byte         // 调用数据
}

// multiCallResult 表示一个multi_call调用的结果
type multiCallResult struct {
	Success    bool   // 调用是否成功
	ReturnData []byte // 返回数据
}

// BatchEthCall 使用RPC batch模式执行批量eth_call调用
func (c *ArbClient) BatchEthCall(ctx context.Context, args [][]interface{}) ([]string, error) {
	comCtx := com.NewBaseCtx(ctx, c.log)
	// 创建批量请求
	batch := []rpc.BatchElem{}

	// 遍历所有调用参数,构建批量请求元素
	for _, arg := range args {
		var result string
		elem := rpc.BatchElem{
			Method: "eth_call",
			Args:   arg,
			Result: &result,
		}
		batch = append(batch, elem)
	}

	err := c.getRPCClient().BatchCallContext(ctx, batch)
	if err != nil {
		comCtx.Log.Errorf("批量eth_call调用失败: %v", err)
		return nil, fmt.Errorf("批量eth_call调用失败: %v", err)
	}

	// 收集结果
	results := make([]string, len(batch))
	for i, elem := range batch {
		if elem.Error != nil {
			comCtx.Log.Errorf("第 %d 个eth_call: [%+v] 调用失败: %v", i, elem.Args, elem.Error)
			return nil, fmt.Errorf("第 %d 个eth_call调用失败: %v", i, elem.Error)
		}
		results[i] = *elem.Result.(*string)
	}

	return results, nil
}

// BatchMutiCall 通过multi_call执行批量合约调用
func (c *ArbClient) BatchMutiCall(ctx context.Context, args [][]interface{}) ([]string, error) {
	comCtx := com.NewBaseCtx(ctx, c.log)
	comCtx.Log.Infof("BatchMutiCall")
	// 解析ABI
	parsedABI, err := abi.JSON(strings.NewReader(MultiCallABI))
	if err != nil {
		return nil, fmt.Errorf("解析MultiCall ABI失败: %v", err)
	}

	// 按区块号分组
	blockCalls := make(map[string][]struct {
		index int            // 原始索引
		input multiCallInput // 调用参数
	})
	latestCalls := make([]struct {
		index int
		input multiCallInput
	}, 0)

	// 遍历参数,按区块号分组
	for i, arg := range args {
		callMap := arg[0].(map[string]interface{})
		to := callMap["to"].(string)
		data := callMap["data"].(string)

		input := multiCallInput{
			Target:   common.HexToAddress(to),
			CallData: common.FromHex(data),
		}

		// 获取区块号参数
		if len(arg) > 1 {
			if blockNumStr, ok := arg[1].(string); ok {
				if strings.HasPrefix(blockNumStr, "0x") {
					blockCalls[blockNumStr] = append(blockCalls[blockNumStr], struct {
						index int
						input multiCallInput
					}{i, input})
					continue
				}
			}
		}
		// 没有区块号,加入最新区块查询组
		latestCalls = append(latestCalls, struct {
			index int
			input multiCallInput
		}{i, input})
	}

	// 存储所有结果
	allResults := make([]string, len(args))

	// 处理指定区块号的调用
	for blockNumStr, calls := range blockCalls {
		// 解析区块号
		blockNum, err := hexutil.DecodeBig(blockNumStr)
		if err != nil {
			return nil, fmt.Errorf("解析区块号失败: %v", err)
		}

		// 准备本区块的调用参数
		inputs := make([]multiCallInput, len(calls))
		for i, call := range calls {
			inputs[i] = call.input
		}

		// 打包调用参数
		callData, err := parsedABI.Pack("tryAggregate", false, inputs)
		if err != nil {
			return nil, fmt.Errorf("打包调用参数失败: %v", err)
		}

		// 执行调用
		multiCallAddr := common.HexToAddress(MultiCallAddress)
		msg := ethereum.CallMsg{
			To:   &multiCallAddr,
			Data: callData,
		}
		output, err := c.client.CallContract(ctx, msg, blockNum)
		if err != nil {
			comCtx.Log.Errorf("区块 %s multi_call调用失败: %v", blockNumStr, err)
			return nil, fmt.Errorf("区块 %s multi_call调用失败: %v", blockNumStr, err)
		}

		// 解析返回数据
		var results []struct {
			Success    bool
			ReturnData []byte
		}
		if err := parsedABI.UnpackIntoInterface(&results, "tryAggregate", output); err != nil {
			return nil, fmt.Errorf("解析返回数据失败: %v", err)
		}

		// 保存结果
		for i, result := range results {
			if !result.Success {
				comCtx.Log.Errorf("区块 %s 第 %d 个调用失败", blockNumStr, i)
				return nil, fmt.Errorf("区块 %s 第 %d 个调用失败", blockNumStr, i)
			}
			allResults[calls[i].index] = hexutil.Encode(result.ReturnData)
		}
	}

	// 处理最新区块的调用
	if len(latestCalls) > 0 {
		// 准备最新区块的调用参数
		inputs := make([]multiCallInput, len(latestCalls))
		for i, call := range latestCalls {
			inputs[i] = call.input
		}

		// 打包调用参数
		callData, err := parsedABI.Pack("tryAggregate", false, inputs)
		if err != nil {
			return nil, fmt.Errorf("打包调用参数失败: %v", err)
		}

		// 执行调用
		multiCallAddr := common.HexToAddress(MultiCallAddress)
		msg := ethereum.CallMsg{
			To:   &multiCallAddr,
			Data: callData,
		}
		output, err := c.client.CallContract(ctx, msg, nil) // 查询最新区块
		if err != nil {
			comCtx.Log.Errorf("最新区块multi_call调用失败: %v", err)
			return nil, fmt.Errorf("最新区块multi_call调用失败: %v", err)
		}

		// 解析返回数据
		var results []struct {
			Success    bool
			ReturnData []byte
		}
		if err := parsedABI.UnpackIntoInterface(&results, "tryAggregate", output); err != nil {
			return nil, fmt.Errorf("解析返回数据失败: %v", err)
		}

		// 保存结果
		for i, result := range results {
			if !result.Success {
				comCtx.Log.Errorf("最新区块第 %d 个调用失败", i)
				return nil, fmt.Errorf("最新区块第 %d 个调用失败", i)
			}
			allResults[latestCalls[i].index] = hexutil.Encode(result.ReturnData)
		}
	}

	return allResults, nil
}

// BatchQueryERC20BalancesByPairs 批量查询指定[用户地址,代币地址]对的余额
// 参数:
// - pairs: 每个元素是[用户地址(common.Address),代币地址(common.Address),区块号(uint64)]对
// 返回:
// - 返回一个map,key为"用户地址-代币地址",value为对应的余额

type UserTokenBalance struct {
	UserAddr    common.Address
	TokenAddr   common.Address
	Balance     *big.Int
	BlockNumber uint64
}

func (c *ArbClient) BatchQueryERC20BalancesByPairs(ctx context.Context, pairs [][3]interface{}, endBlockNumber uint64) (map[string]*UserTokenBalance, error) {
	comCtx := com.NewBaseCtx(ctx, c.log)
	comCtx.Log.Infof("BatchQueryERC20BalancesByPairs")
	if len(pairs) == 0 {
		return nil, fmt.Errorf("查询对列表为空")
	}

	ctfContract := c.Contract.PredictionCTFContract

	// 准备批量调用的参数
	callArgs := [][]interface{}{}

	// 为每个[用户,代币]对构建调用参数
	for _, pair := range pairs {

		holderAddr := pair[0].(common.Address)
		tokenAddr := pair[1].(common.Address)
		// 始终使用最新区块查询，避免公共 RPC 无法读取旧区块状态（missing trie node）
		queryBlockNumber := endBlockNumber

		// 打包balanceOf调用数据
		data, err := ctfContract.GetABI().Pack(contract.MethodBalanceOf, holderAddr)
		if err != nil {
			comCtx.Log.Errorf("打包balanceOf调用数据失败 [token=%s, holder=%s]: %v",
				tokenAddr.Hex(), holderAddr.Hex(), err)
			return nil, fmt.Errorf("打包balanceOf调用数据失败 [token=%s, holder=%s]: %v",
				tokenAddr.Hex(), holderAddr.Hex(), err)
		}

		callArgs = append(callArgs, []interface{}{
			map[string]interface{}{
				"to":   tokenAddr.Hex(),
				"data": hexutil.Encode(data),
			},
			hexutil.EncodeUint64(queryBlockNumber),
		})
	}

	// 执行批量调用
	results, err := c.BatchMutiCall(ctx, callArgs)
	if err != nil {
		return nil, fmt.Errorf("批量查询ERC20余额失败: %v", err)
	}

	// 解析结果
	balances := make(map[string]*UserTokenBalance)
	for i, result := range results {
		if i >= len(pairs) {
			break
		}

		holderAddr := pairs[i][0].(common.Address)
		tokenAddr := pairs[i][1].(common.Address)
		origBlockNumber := pairs[i][2].(uint64)
		// 生成key（使用原始区块号以匹配 getBalance 查找）
		key := fmt.Sprintf("%s-%s-%d", holderAddr.Hex(), tokenAddr.Hex(), origBlockNumber)

		// 解析余额
		var balance *big.Int
		if err := ctfContract.GetABI().UnpackIntoInterface(&balance, contract.MethodBalanceOf, common.FromHex(result)); err != nil {
			comCtx.Log.Errorf("解析余额失败 [token=%s, holder=%s]: %v", tokenAddr.Hex(), holderAddr.Hex(), err)
			return nil, fmt.Errorf("解析余额失败 [token=%s, holder=%s]: %v", tokenAddr.Hex(), holderAddr.Hex(), err)
		} else {
			balances[key] = &UserTokenBalance{
				UserAddr:    holderAddr,
				TokenAddr:   tokenAddr,
				Balance:     balance,
				BlockNumber: origBlockNumber,
			}
		}
	}

	return balances, nil
}

// BatchQueryOptionPrices 批量查询预测市场中选项的价格
// 参数:
// - pairs: 每个元素是[预测市场地址(string), 选项索引(uint32), 区块号(uint64)]对
// - endBlockNumber: 查询的区块高度，如果pairs中的区块号为0则使用此值
// 返回:
// - 返回一个map，key为"预测市场地址-选项索引-区块号"格式的字符串，value为对应的价格信息
// - 价格信息包含预测市场地址、选项索引、价格(以基础代币计价)和区块号

type OptionPrice struct {
	PredictionAddr string
	OptionIndex    uint32
	Price          *big.Int
	BlockNumber    uint64
}

func (c *ArbClient) BatchQueryOptionPrices(ctx context.Context, pairs [][3]interface{}, endBlockNumber uint64) (map[string]*OptionPrice, error) {
	comCtx := com.NewBaseCtx(ctx, c.log)
	comCtx.Log.Infof("BatchQueryOptionPrices")
	if len(pairs) == 0 {
		return nil, fmt.Errorf("查询对列表为空")
	}

	ctfContract := c.Contract.PredictionCTFContract

	// 准备批量调用的参数
	callArgs := [][]interface{}{}

	// 为每个[预测市场,选项索引]对构建调用参数
	for _, pair := range pairs {
		predictionAddr := pair[0].(string)
		optionIndex := pair[1].(uint32)
		// 始终使用最新区块查询
		queryBlockNumber := endBlockNumber
		// 打包getPrice调用数据
		data, err := ctfContract.GetABI().Pack(contract.MethodPrice, big.NewInt(int64(optionIndex)))
		if err != nil {
			comCtx.Log.Errorf("打包getPrice调用数据失败 [market=%s, index=%d]: %v",
				predictionAddr, optionIndex, err)
			return nil, fmt.Errorf("打包getPrice调用数据失败 [market=%s, index=%d]: %v",
				predictionAddr, optionIndex, err)
		}

		callArgs = append(callArgs, []interface{}{
			map[string]interface{}{
				"to":   predictionAddr,
				"data": hexutil.Encode(data),
			},
			hexutil.EncodeUint64(queryBlockNumber),
		})
	}

	// 执行批量调用
	results, err := c.BatchMutiCall(ctx, callArgs)
	if err != nil {
		return nil, fmt.Errorf("批量查询选项价格失败: %v", err)
	}

	// 解析结果
	prices := make(map[string]*OptionPrice)
	for i, result := range results {
		if i >= len(pairs) {
			break
		}

		predictionAddr := pairs[i][0].(string)
		optionIndex := pairs[i][1].(uint32)
		origBlockNumber := pairs[i][2].(uint64)
		key := fmt.Sprintf("%s-%d-%d", predictionAddr, optionIndex, origBlockNumber)
		// 解析价格
		var price *big.Int
		if err := ctfContract.GetABI().UnpackIntoInterface(&price, contract.MethodPrice, common.FromHex(result)); err != nil {
			comCtx.Log.Errorf("解析价格失败 [market=%s, index=%v]: %v", predictionAddr, optionIndex, err)
			return nil, fmt.Errorf("解析价格失败 [market=%s, index=%v]: %v", predictionAddr, optionIndex, err)
		} else {
			prices[key] = &OptionPrice{
				PredictionAddr: predictionAddr,
				OptionIndex:    optionIndex,
				Price:          price,
				BlockNumber:    origBlockNumber,
			}
		}
	}

	return prices, nil
}

// BatchQueryBlockTimestamps 批量查询区块号对应的区块时间
// 参数:
// - blockNumbers: 需要查询的区块号列表
// 返回:
// - 返回一个map,key为区块号,value为对应的区块时间戳(Unix时间戳)
func (c *ArbClient) BatchQueryBlockTimestamps(ctx context.Context, blockNumbers []uint64) (map[uint64]uint64, error) {
	comCtx := com.NewBaseCtx(ctx, c.log)
	comCtx.Log.Infof("BatchQueryBlockTimestamps")
	if len(blockNumbers) == 0 {
		return nil, fmt.Errorf("区块号列表为空")
	}

	// 准备批量调用的参数
	callArgs := [][]interface{}{}

	// 为每个区块号构建调用参数
	for _, blockNum := range blockNumbers {
		callArgs = append(callArgs, []interface{}{
			"eth_getBlockByNumber",
			[]interface{}{
				hexutil.EncodeUint64(blockNum),
				false,
			},
		})
	}

	// 创建批量请求
	batch := []rpc.BatchElem{}

	// 遍历所有调用参数,构建批量请求元素
	type blockResponse struct {
		Timestamp string `json:"timestamp"`
	}

	responses := make([]*blockResponse, len(blockNumbers))
	for i, arg := range callArgs {
		batch = append(batch, rpc.BatchElem{
			Method: arg[0].(string),
			Args:   arg[1].([]interface{}),
			Result: &blockResponse{},
		})
		responses[i] = &blockResponse{}
		batch[i].Result = responses[i]
	}

	// 执行批量调用
	err := c.getRPCClient().BatchCallContext(ctx, batch)
	if err != nil {
		comCtx.Log.Errorf("批量查询区块时间失败: %v", err)
		return nil, fmt.Errorf("批量查询区块时间失败: %v", err)
	}

	// 解析结果
	results := make(map[uint64]uint64)
	for i, elem := range batch {
		if elem.Error != nil {
			comCtx.Log.Errorf("查询区块[%d]时间失败: %v", blockNumbers[i], elem.Error)
			return nil, fmt.Errorf("查询区块[%d]时间失败: %v", blockNumbers[i], elem.Error)
		}

		resp := responses[i]
		if resp == nil || resp.Timestamp == "" {
			comCtx.Log.Warnf("区块[%d]时间返回为空", blockNumbers[i])
			continue
		}

		// 转换十六进制时间戳为uint64
		timestamp, err := hexutil.DecodeUint64(resp.Timestamp)
		if err != nil {
			comCtx.Log.Errorf("解析区块[%d]时间戳失败: %v", blockNumbers[i], err)
			return nil, fmt.Errorf("解析区块[%d]时间戳失败: %v", blockNumbers[i], err)
		}

		results[blockNumbers[i]] = timestamp
	}

	return results, nil
}

// GetBlockTimestamp 获取指定区块号的时间戳
func (c *ArbClient) GetBlockTimestamp(ctx context.Context, blockNumber uint64) (uint64, error) {
	comCtx := com.NewBaseCtx(ctx, c.log)
	comCtx.Log.Infof("GetBlockTimestamp")
	block, err := c.getClient().BlockByNumber(ctx, big.NewInt(int64(blockNumber)))
	if err != nil {
		return 0, fmt.Errorf("获取区块[%d]失败: %v", blockNumber, err)
	}
	return block.Time(), nil
}
