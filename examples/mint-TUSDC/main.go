package main

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"log"
	"math/big"
	"os"
	"strings"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/joho/godotenv"
)

const (
	// TUSDC contract address on Sepolia
	TUSDCContractAddress = "0x4e3bDC4A6057B2ddd764123E9016D18163C85771"
	// Receiver address (who receives TUSDC)
	ToAddress = "0xea70CeF4bf2d1ed3046eBDe7ee34C5E1AE5BCE84" // 测试 db 中的 eoa 地址
	// Mint amount (1000 TUSDC, USDC has 6 decimals)
	MintAmount = 1000
	// TUSDC decimals
	Decimals = 6
)

// Sepolia RPC URLs
var rpcUrls = []string{
	"https://sepolia.drpc.org",
	"https://aged-dry-cloud.ethereum-sepolia.quiknode.pro/0150d8e76e408eb637acc978ba23785a51412d50/",
	"https://eth-sepolia.g.alchemy.com/v2/uxieumKna2lrn85rTZ-cS",
	"https://ethereum-sepolia-rpc.publicnode.com",
	"https://0xrpc.io/sep",
}

// TUSDC ABI (only mint function)
const tusdcABI = `[{"inputs":[{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"}],"name":"mint","outputs":[],"stateMutability":"nonpayable","type":"function"}]`

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

	// Get address from public key (sender who pays gas, derived from private key)
	fromAddress := crypto.PubkeyToAddress(*publicKeyECDSA)
	fmt.Printf("Sender address (pays gas): %s\n", fromAddress.Hex())

	// Receiver address (gets the minted TUSDC)
	toAddress := common.HexToAddress(ToAddress)
	fmt.Printf("Receiver address (gets TUSDC): %s\n", toAddress.Hex())

	// Connect to RPC
	client := connectToRPC()
	defer client.Close()

	// Get chain ID (Sepolia = 11155111)
	chainID, err := client.NetworkID(context.Background())
	if err != nil {
		log.Fatal("Failed to get chain ID: ", err)
	}
	fmt.Printf("Chain ID: %s\n", chainID.String())

	// Parse ABI
	parsedABI, err := abi.JSON(strings.NewReader(tusdcABI))
	if err != nil {
		log.Fatal("Failed to parse ABI: ", err)
	}

	// Calculate mint amount with decimals (1000 * 10^6)
	mintAmount := new(big.Int).Mul(
		big.NewInt(MintAmount),
		new(big.Int).Exp(big.NewInt(10), big.NewInt(Decimals), nil),
	)
	fmt.Printf("Mint amount: %s (raw), %d TUSDC\n", mintAmount.String(), MintAmount)

	// Encode mint function call: mint(address to, uint256 amount)
	data, err := parsedABI.Pack("mint", toAddress, mintAmount)
	if err != nil {
		log.Fatal("Failed to pack mint function: ", err)
	}
	fmt.Printf("Call data: 0x%x\n", data)

	// Get nonce
	nonce, err := client.PendingNonceAt(context.Background(), fromAddress)
	if err != nil {
		log.Fatal("Failed to get nonce: ", err)
	}
	fmt.Printf("Nonce: %d\n", nonce)

	// Get suggested gas price
	gasPrice, err := client.SuggestGasPrice(context.Background())
	if err != nil {
		log.Fatal("Failed to get gas price: ", err)
	}
	fmt.Printf("Gas Price: %s Wei\n", gasPrice.String())

	// Contract address
	contractAddress := common.HexToAddress(TUSDCContractAddress)

	// Estimate gas
	gasLimit, err := client.EstimateGas(context.Background(), ethereum.CallMsg{
		From: fromAddress,
		To:   &contractAddress,
		Data: data,
	})
	if err != nil {
		log.Printf("Failed to estimate gas, using default 100000: %v", err)
		gasLimit = 100000
	}
	fmt.Printf("Gas Limit: %d\n", gasLimit)

	// Create transaction (value = 0 for contract call)
	tx := types.NewTransaction(nonce, contractAddress, big.NewInt(0), gasLimit, gasPrice, data)

	// Sign transaction
	signedTx, err := types.SignTx(tx, types.NewEIP155Signer(chainID), privateKey)
	if err != nil {
		log.Fatal("Failed to sign transaction: ", err)
	}

	// Check sender ETH balance for gas
	balance, err := client.BalanceAt(context.Background(), fromAddress, nil)
	if err != nil {
		log.Fatal("Failed to get balance: ", err)
	}
	fmt.Printf("ETH balance: %s Wei\n", balance.String())

	// Calculate gas cost
	gasCost := new(big.Int).Mul(gasPrice, big.NewInt(int64(gasLimit)))
	fmt.Printf("Gas cost: %s Wei\n", gasCost.String())

	if balance.Cmp(gasCost) < 0 {
		log.Fatal("Insufficient ETH balance for gas!")
	}

	// Send transaction
	err = client.SendTransaction(context.Background(), signedTx)
	if err != nil {
		log.Fatal("Failed to send transaction: ", err)
	}

	fmt.Println("\n✅ Mint transaction sent!")
	fmt.Printf("Transaction hash: %s\n", signedTx.Hash().Hex())
	fmt.Printf("From (pays gas): %s\n", fromAddress.Hex())
	fmt.Printf("To (receives TUSDC): %s\n", toAddress.Hex())
	fmt.Printf("Amount: %d TUSDC\n", MintAmount)
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
