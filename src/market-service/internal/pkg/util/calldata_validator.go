package util

import (
	"encoding/hex"
	"errors"
	"strings"

	"golang.org/x/crypto/sha3"
)

// 从calldata中提取市场合约地址
func ExtractMarketContractFromCalldata(calldata string) (string, error) {
	// 移除0x前缀
	if strings.HasPrefix(calldata, "0x") {
		calldata = calldata[2:]
	}

	// 检查calldata长度
	if len(calldata) < 8 {
		return "", errors.New("invalid calldata length")
	}

	// 获取函数选择器
	selector := calldata[:8]

	switch selector {
	case "47e1da2a":
		// 买固定：AA钱包批量调用
		return extractFromBuyCall(calldata)
	case "b61d27f6":
		// 卖固定：execute调用
		return extractFromSellCall(calldata)
	default:
		return "", errors.New("unsupported calldata format")
	}
}

// 从买固定的批量调用中提取市场合约地址
func extractFromBuyCall(calldata string) (string, error) {
	// 从批量调用数据中搜索地址
	// 先尝试位置352 (真实市场合约地址位置)
	if len(calldata) > 392 {
		start := 352
		if start+40 <= len(calldata) {
			addrHex := calldata[start : start+40]
			if addrHex != "0000000000000000000000000000000000000000" {
				return NormalizeAddress("0x" + addrHex), nil
			}
		}
	}

	// 备用：从固定位置88提取
	if len(calldata) > 150 {
		start := 88
		if start+40 <= len(calldata) {
			addrHex := calldata[start : start+40]
			if addrHex != "0000000000000000000000000000000000000000" {
				addr := strings.TrimLeft(addrHex, "0")
				if addr != "" {
					return NormalizeAddress("0x" + addr), nil
				}
			}
		}
	}

	return "", errors.New("cannot extract market contract from buy call")
}

// 从卖固定的execute调用中提取市场合约地址
func extractFromSellCall(calldata string) (string, error) {
	// execute(address target, uint256 value, bytes data)
	if len(calldata) < 72 {
		return "", errors.New("sell fixed calldata length is too short")
	}

	// 提取target地址（第一个参数）
	start := 8 + 24 // 函数选择器 + 24个字符的填充
	if start+40 <= len(calldata) {
		addrHex := calldata[start : start+40]
		if addrHex != "0000000000000000000000000000000000000000" {
			return NormalizeAddress("0x" + addrHex), nil
		}
	}

	return "", errors.New("cannot extract market contract from sell call")
}

// 标准化地址格式：转为EIP-55校验和地址
func NormalizeAddress(addr string) string {
	// 去除0x前缀
	if strings.HasPrefix(addr, "0x") || strings.HasPrefix(addr, "0X") {
		addr = addr[2:]
	}

	// 转为小写并补齐到40位
	addr = strings.ToLower(addr)
	for len(addr) < 40 {
		addr = "0" + addr
	}

	// 只取前40位
	if len(addr) > 40 {
		addr = addr[:40]
	}

	// 计算Keccak256哈希
	hash := sha3.NewLegacyKeccak256()
	hash.Write([]byte(addr))
	hashBytes := hash.Sum(nil)
	hashHex := hex.EncodeToString(hashBytes)

	// 根据哈希值生成校验和地址
	result := "0x"
	for i, char := range addr {
		if hashHex[i] >= '8' {
			result += strings.ToUpper(string(char))
		} else {
			result += string(char)
		}
	}

	return result
}
