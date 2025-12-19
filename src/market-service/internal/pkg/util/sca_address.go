package util

import (
	"bytes"
	"fmt"
	"regexp"
	"strings"
	"unicode"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"golang.org/x/crypto/sha3"
)

// ComputeSCAAddress 计算代理钱包地址
func ComputeSCAAddress(magicAddress string, factoryAddress string) (string, error) {
	// 1. 检查Magic地址格式
	if !common.IsHexAddress(magicAddress) {
		return "", fmt.Errorf("invalid magic address format")
	}

	// 2. 构造初始化数据
	factory := common.HexToAddress(factoryAddress)
	magic := common.HexToAddress(magicAddress)

	// 3. 编码owner地址 (对应solidity的abi.encode)
	ownerData := common.LeftPadBytes(magic.Bytes(), 32)

	// 4. 拼接factory地址和编码后的数据
	var initCode []byte
	initCode = append(initCode, factory.Bytes()...)
	initCode = append(initCode, ownerData...)

	// 5. 计算salt (这里使用固定值0)
	salt := common.Hash{}

	// 6. 计算代理钱包地址 (CREATE2)
	initCodeHash := crypto.Keccak256(initCode)

	// 拼接字节
	var buffer bytes.Buffer
	buffer.WriteByte(0xff)        // 0xff
	buffer.Write(factory.Bytes()) // factory address
	buffer.Write(salt.Bytes())    // salt
	buffer.Write(initCodeHash)    // keccak256(init_code)

	// 计算最终哈希
	hash := crypto.Keccak256(buffer.Bytes())

	// 取最后20字节作为地址
	scaAddress := common.BytesToAddress(hash[12:])

	return scaAddress.Hex(), nil
}

// ToChecksumAddress 将地址转换为标准的校验和地址格式
func ToChecksumAddress(addr string) (string, error) {
	// 移除 "0x" 前缀(如果存在)
	addr = strings.TrimPrefix(addr, "0x")

	// 检查地址长度是否为40个字符
	if len(addr) != 40 {
		return "", fmt.Errorf("地址长度必须为40个字符")
	}

	// 将地址转换为小写
	addr = strings.ToLower(addr)

	// 检查地址是否只包含有效的十六进制字符
	if !regexp.MustCompile("^[0-9a-f]{40}$").MatchString(addr) {
		return "", fmt.Errorf("地址包含无效字符")
	}

	// 计算地址的Keccak-256哈希
	hasher := sha3.NewLegacyKeccak256()
	hasher.Write([]byte(addr))
	hash := hasher.Sum(nil)

	result := make([]byte, 40)
	for i := 0; i < 40; i++ {
		result[i] = addr[i]
		// 根据哈希值确定字符是否需要大写
		if ((hash[i/2] >> (4 * (1 - i%2))) & 0xf) >= 8 {
			result[i] = byte(unicode.ToUpper(rune(addr[i])))
		}
	}

	return "0x" + string(result), nil
}
