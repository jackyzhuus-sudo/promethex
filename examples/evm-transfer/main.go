package main

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"log"
	"math/big"
	"os"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/joho/godotenv"
)

// Hardcoded wallet addresses
const (
	// Wallet A address (sender)
	FromAddress = "0x5946781ee5913ad95c0eaefa86a95a7c77249fc0"
	// Wallet B address (receiver)
	ToAddress = "0xE4a720a8c9Cfd28539F9a4800863dC37d1544c14"
	// Transfer amount (ETH)
	TransferAmountETH = 0.0999999
)

// Sepolia RPC URLs
var rpcUrls = []string{
	"https://sepolia.drpc.org",
	"https://aged-dry-cloud.ethereum-sepolia.quiknode.pro/0150d8e76e408eb637acc978ba23785a51412d50/",
	"https://eth-sepolia.g.alchemy.com/v2/uxieumKna2lrn85rTZ-cS",
	"https://ethereum-sepolia-rpc.publicnode.com",
	"https://0xrpc.io/sep",
}

func main() {
	// Load .env file
	if err := godotenv.Load(); err != nil {
		log.Fatal("Failed to load .env file: ", err)
	}

	// Get private key from environment variable
	privateKeyHex := os.Getenv("evm_private_key")
	if privateKeyHex == "" {
		log.Fatal("Please set evm_private_key in .env file")
	}

	// Parse private key
	privateKey, err := crypto.HexToECDSA(privateKeyHex)
	if err != nil {
		log.Fatal("Invalid private key format: ", err)
	}

	// Get public key from private key
	publicKey := privateKey.Public()
	publicKeyECDSA, ok := publicKey.(*ecdsa.PublicKey)
	if !ok {
		log.Fatal("Failed to get public key")
	}

	// Get address from public key
	fromAddress := crypto.PubkeyToAddress(*publicKeyECDSA)
	fmt.Printf("Sender address: %s\n", fromAddress.Hex())

	// Verify address matches
	if fromAddress.Hex() != FromAddress {
		log.Printf("Warning: Private key address %s does not match configured sender address %s\n", fromAddress.Hex(), FromAddress)
	}

	// Connect to RPC
	client := connectToRPC()
	defer client.Close()

	// Get chain ID (Sepolia = 11155111)
	chainID, err := client.NetworkID(context.Background())
	if err != nil {
		log.Fatal("Failed to get chain ID: ", err)
	}
	fmt.Printf("Chain ID: %s\n", chainID.String())

	// Get nonce
	nonce, err := client.PendingNonceAt(context.Background(), fromAddress)
	if err != nil {
		log.Fatal("Failed to get nonce: ", err)
	}
	fmt.Printf("Nonce: %d\n", nonce)

	// Transfer amount (Wei)
	value := etherToWei(TransferAmountETH)
	fmt.Printf("Transfer amount: %s Wei (%.6f ETH)\n", value.String(), TransferAmountETH)

	// Get suggested gas price
	gasPrice, err := client.SuggestGasPrice(context.Background())
	if err != nil {
		log.Fatal("Failed to get gas price: ", err)
	}
	fmt.Printf("Gas Price: %s Wei\n", gasPrice.String())

	// Gas limit (21000 for simple transfer)
	gasLimit := uint64(21000)

	// Receiver address
	toAddress := common.HexToAddress(ToAddress)

	// Create transaction
	tx := types.NewTransaction(nonce, toAddress, value, gasLimit, gasPrice, nil)

	// Sign transaction
	signedTx, err := types.SignTx(tx, types.NewEIP155Signer(chainID), privateKey)
	if err != nil {
		log.Fatal("Failed to sign transaction: ", err)
	}

	// Print transaction hash
	fmt.Printf("Transaction hash: %s\n", signedTx.Hash().Hex())

	// Check sender balance
	balance, err := client.BalanceAt(context.Background(), fromAddress, nil)
	if err != nil {
		log.Fatal("Failed to get balance: ", err)
	}
	fmt.Printf("Sender balance: %s Wei (%.6f ETH)\n", balance.String(), weiToEther(balance))

	// Calculate total cost
	totalCost := new(big.Int).Add(value, new(big.Int).Mul(gasPrice, big.NewInt(int64(gasLimit))))
	fmt.Printf("Total cost (value + gas): %s Wei\n", totalCost.String())

	if balance.Cmp(totalCost) < 0 {
		log.Fatal("Insufficient balance!")
	}

	// Send transaction
	err = client.SendTransaction(context.Background(), signedTx)
	if err != nil {
		log.Fatal("Failed to send transaction: ", err)
	}

	fmt.Println("\n✅ Transaction sent!")
	fmt.Printf("Transaction hash: %s\n", signedTx.Hash().Hex())
	fmt.Printf("From: %s\n", fromAddress.Hex())
	fmt.Printf("To: %s\n", toAddress.Hex())
	fmt.Printf("Amount: %.6f ETH\n", TransferAmountETH)
	fmt.Printf("\nView on Etherscan: https://sepolia.etherscan.io/tx/%s\n", signedTx.Hash().Hex())
}

// Connect to RPC
func connectToRPC() *ethclient.Client {
	var client *ethclient.Client
	var err error
	for _, rpcUrl := range rpcUrls {
		client, err = ethclient.Dial(rpcUrl)
		if err == nil {
			fmt.Printf("Connected to: %s\n", rpcUrl)
			return client
		}
		fmt.Printf("Failed to connect %s: %v\n", rpcUrl, err)
	}
	log.Fatal("All RPCs failed to connect")
	return nil
}

// ETH to Wei
func etherToWei(eth float64) *big.Int {
	// 1 ETH = 10^18 Wei
	ethBig := new(big.Float).SetFloat64(eth)
	weiMultiplier := new(big.Float).SetInt(big.NewInt(1e18))
	weiBig := new(big.Float).Mul(ethBig, weiMultiplier)

	wei := new(big.Int)
	weiBig.Int(wei)
	return wei
}

// Wei to ETH
func weiToEther(wei *big.Int) float64 {
	weiBig := new(big.Float).SetInt(wei)
	weiMultiplier := new(big.Float).SetInt(big.NewInt(1e18))
	ethBig := new(big.Float).Quo(weiBig, weiMultiplier)

	eth, _ := ethBig.Float64()
	return eth
}
