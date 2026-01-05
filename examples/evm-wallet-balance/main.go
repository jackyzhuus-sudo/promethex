package main

import (
	"context"
	"fmt"
	"log"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
)

func main() {
	evmAddr := common.HexToAddress("0xE4a720a8c9Cfd28539F9a4800863dC37d1544c14")

	// Sepolia RPC URLs
	rpcUrls := []string{
		"https://sepolia.drpc.org",
		"https://aged-dry-cloud.ethereum-sepolia.quiknode.pro/0150d8e76e408eb637acc978ba23785a51412d50/",
		"https://eth-sepolia.g.alchemy.com/v2/uxieumKna2lrn85rTZ-cS",
		"https://ethereum-sepolia-rpc.publicnode.com",
		"https://0xrpc.io/sep",
	}

	// 尝试连接 RPC，直到成功
	var client *ethclient.Client
	var err error
	for _, rpcUrl := range rpcUrls {
		client, err = ethclient.Dial(rpcUrl)
		if err == nil {
			fmt.Printf("成功连接到: %s\n", rpcUrl)
			break
		}
		fmt.Printf("连接失败 %s: %v\n", rpcUrl, err)
	}
	if client == nil {
		log.Fatal("所有 RPC 都无法连接")
	}
	defer client.Close()

	// 获取最新区块的余额
	balance, err := client.BalanceAt(context.Background(), evmAddr, nil)
	if err != nil {
		log.Fatal(err)
	}

	// 将 Wei 转换为 ETH (1 ETH = 10^18 Wei)
	ethBalance := new(big.Float).Quo(
		new(big.Float).SetInt(balance),
		new(big.Float).SetInt(big.NewInt(1e18)),
	)

	fmt.Printf("地址: %s\n", evmAddr.Hex())
	fmt.Printf("余额: %s Wei\n", balance.String())
	fmt.Printf("余额: %s ETH\n", ethBalance.Text('f', 18))
}
