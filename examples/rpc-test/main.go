package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"sync"
	"time"
)

// RPC 地址列表（硬编码，与 config.yaml 中的配置一致，按延迟排序）
var rpcList = []string{
	"https://sepolia.drpc.org", // 216ms ✅
	"https://aged-dry-cloud.ethereum-sepolia.quiknode.pro/0150d8e76e408eb637acc978ba23785a51412d50/", // 237ms ✅
	"https://eth-sepolia.g.alchemy.com/v2/uxieumKna2lrn85rTZ-cS",                                     // 301ms ✅
	"https://ethereum-sepolia-rpc.publicnode.com",                                                    // 668ms ✅
	"https://0xrpc.io/sep", // 833ms ✅
	// 以下节点超时已移除:
	// "https://rpc.sepolia.org",                                // ❌ 超时
	// "https://ethereum-sepolia.blockpi.network/v1/rpc/public", // ❌ 超时
}

// RPCResult 测试结果
type RPCResult struct {
	URL         string
	Success     bool
	BlockNumber int64
	Latency     time.Duration
	Error       string
}

// JSONRPCRequest JSON-RPC 请求结构
type JSONRPCRequest struct {
	JSONRPC string        `json:"jsonrpc"`
	Method  string        `json:"method"`
	Params  []interface{} `json:"params"`
	ID      int           `json:"id"`
}

// JSONRPCResponse JSON-RPC 响应结构
type JSONRPCResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      int             `json:"id"`
	Result  json.RawMessage `json:"result"`
	Error   *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func main() {
	fmt.Println("=" + repeat("=", 78))
	fmt.Println("Sepolia RPC 节点连通性测试")
	fmt.Println("=" + repeat("=", 78))
	fmt.Printf("测试时间: %s\n", time.Now().Format("2006-01-02 15:04:05"))
	fmt.Printf("测试节点数: %d\n\n", len(rpcList))

	results := testAllRPCs(rpcList)

	// 按延迟排序
	sort.Slice(results, func(i, j int) bool {
		if results[i].Success != results[j].Success {
			return results[i].Success // 成功的排前面
		}
		return results[i].Latency < results[j].Latency
	})

	// 打印结果
	printResults(results)

	// 统计
	printSummary(results)
}

func testAllRPCs(urls []string) []RPCResult {
	var wg sync.WaitGroup
	results := make([]RPCResult, len(urls))

	for i, url := range urls {
		wg.Add(1)
		go func(idx int, rpcURL string) {
			defer wg.Done()
			results[idx] = testRPC(rpcURL)
		}(i, url)
	}

	wg.Wait()
	return results
}

func testRPC(url string) RPCResult {
	result := RPCResult{URL: url}

	// 构建 eth_blockNumber 请求
	reqBody := JSONRPCRequest{
		JSONRPC: "2.0",
		Method:  "eth_blockNumber",
		Params:  []interface{}{},
		ID:      1,
	}

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		result.Error = fmt.Sprintf("marshal error: %v", err)
		return result
	}

	// 发送请求并计时
	client := &http.Client{Timeout: 10 * time.Second}
	start := time.Now()

	resp, err := client.Post(url, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		result.Latency = time.Since(start)
		result.Error = fmt.Sprintf("request error: %v", err)
		return result
	}
	defer resp.Body.Close()

	result.Latency = time.Since(start)

	// 读取响应
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		result.Error = fmt.Sprintf("read body error: %v", err)
		return result
	}

	// 检查 HTTP 状态码
	if resp.StatusCode != http.StatusOK {
		result.Error = fmt.Sprintf("HTTP %d: %s", resp.StatusCode, string(body))
		return result
	}

	// 解析 JSON-RPC 响应
	var rpcResp JSONRPCResponse
	if err := json.Unmarshal(body, &rpcResp); err != nil {
		result.Error = fmt.Sprintf("unmarshal error: %v, body: %s", err, string(body))
		return result
	}

	// 检查 RPC 错误
	if rpcResp.Error != nil {
		result.Error = fmt.Sprintf("RPC error %d: %s", rpcResp.Error.Code, rpcResp.Error.Message)
		return result
	}

	// 解析区块号
	var blockHex string
	if err := json.Unmarshal(rpcResp.Result, &blockHex); err != nil {
		result.Error = fmt.Sprintf("parse block number error: %v", err)
		return result
	}

	// 将十六进制转换为十进制
	var blockNum int64
	fmt.Sscanf(blockHex, "0x%x", &blockNum)

	result.Success = true
	result.BlockNumber = blockNum
	return result
}

func printResults(results []RPCResult) {
	fmt.Println("测试结果:")
	fmt.Println("-" + repeat("-", 78))
	fmt.Printf("%-4s %-55s %-10s %-10s\n", "状态", "RPC URL", "延迟", "区块高度")
	fmt.Println("-" + repeat("-", 78))

	for _, r := range results {
		status := "❌"
		latencyStr := fmt.Sprintf("%dms", r.Latency.Milliseconds())
		blockStr := "-"

		if r.Success {
			status = "✅"
			blockStr = fmt.Sprintf("%d", r.BlockNumber)
		}

		// 截断过长的 URL
		url := r.URL
		if len(url) > 53 {
			url = url[:50] + "..."
		}

		fmt.Printf("%-4s %-55s %-10s %-10s\n", status, url, latencyStr, blockStr)

		if !r.Success && r.Error != "" {
			// 截断错误信息
			errMsg := r.Error
			if len(errMsg) > 70 {
				errMsg = errMsg[:67] + "..."
			}
			fmt.Printf("     └─ 错误: %s\n", errMsg)
		}
	}
	fmt.Println("-" + repeat("-", 78))
}

func printSummary(results []RPCResult) {
	var successCount int
	var totalLatency time.Duration
	var minLatency, maxLatency time.Duration
	var bestURL string

	for _, r := range results {
		if r.Success {
			successCount++
			totalLatency += r.Latency

			if minLatency == 0 || r.Latency < minLatency {
				minLatency = r.Latency
				bestURL = r.URL
			}
			if r.Latency > maxLatency {
				maxLatency = r.Latency
			}
		}
	}

	fmt.Println()
	fmt.Println("统计摘要:")
	fmt.Println("-" + repeat("-", 78))
	fmt.Printf("  可用节点: %d/%d (%.1f%%)\n", successCount, len(results), float64(successCount)/float64(len(results))*100)

	if successCount > 0 {
		avgLatency := totalLatency / time.Duration(successCount)
		fmt.Printf("  平均延迟: %dms\n", avgLatency.Milliseconds())
		fmt.Printf("  最低延迟: %dms\n", minLatency.Milliseconds())
		fmt.Printf("  最高延迟: %dms\n", maxLatency.Milliseconds())
		fmt.Printf("  推荐节点: %s\n", bestURL)
	}
	fmt.Println("-" + repeat("-", 78))

	// 建议
	if successCount < len(results)/2 {
		fmt.Println()
		fmt.Println("⚠️  警告: 超过一半的 RPC 节点不可用!")
		fmt.Println("   建议:")
		fmt.Println("   1. 检查网络连接")
		fmt.Println("   2. 更换不可用的 RPC 节点")
		fmt.Println("   3. 考虑使用付费 RPC 服务（如 Alchemy, Infura, QuickNode）")
	}

	if successCount == 0 {
		fmt.Println()
		fmt.Println("❌ 严重: 所有 RPC 节点均不可用!")
	}
}

func repeat(s string, n int) string {
	result := ""
	for i := 0; i < n; i++ {
		result += s
	}
	return result
}
