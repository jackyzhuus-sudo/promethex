package util

import (
	"math/big"
	"strings"
	"sync"

	"github.com/ethereum/go-ethereum/accounts/abi"
)

// PredictionCTF ABI (view functions only)
//
// Solidity signatures:
//   price(uint256 option)             returns (uint256)   -- APMM probability price
//   getAmountOut(uint256, uint256)    returns (int256)    -- SD59x18; unwrap() = amount in baseToken decimals
//   getAmountsOut(int256[])           returns (int256)    -- SD59x18; unwrap() = amount in baseToken decimals
//   numOutcomes()                     returns (uint256)
//
// Note: All amounts (inputs and outputs) are in baseToken decimals (6 for USDC).
// The contract internally uses SD59x18 fixed-point math with a scaling `factor`
// derived from baseToken.decimals(). Callers do NOT need to scale values.
//
// The State struct has dynamic arrays so the auto-generated state() getter
// omits them. It returns only: (uint256 fee, uint256 rewardPerShare).
const predictionCTFABIJSON = `[
	{
		"name": "price",
		"type": "function",
		"stateMutability": "view",
		"inputs": [{"name": "option", "type": "uint256"}],
		"outputs": [{"name": "", "type": "uint256"}]
	},
	{
		"name": "getAmountOut",
		"type": "function",
		"stateMutability": "view",
		"inputs": [
			{"name": "optionOut", "type": "uint256"},
			{"name": "delta", "type": "uint256"}
		],
		"outputs": [{"name": "amount", "type": "int256"}]
	},
	{
		"name": "getAmountsOut",
		"type": "function",
		"stateMutability": "view",
		"inputs": [{"name": "x", "type": "int256[]"}],
		"outputs": [{"name": "delta", "type": "int256"}]
	},
	{
		"name": "numOutcomes",
		"type": "function",
		"stateMutability": "view",
		"inputs": [],
		"outputs": [{"name": "", "type": "uint256"}]
	},
	{
		"name": "state",
		"type": "function",
		"stateMutability": "view",
		"inputs": [],
		"outputs": [
			{"name": "fee", "type": "uint256"},
			{"name": "rewardPerShare", "type": "uint256"}
		]
	}
]`

var (
	parsedCTFABI abi.ABI
	ctfABIOnce   sync.Once
)

// ParsePredictionCTFABI returns the parsed ABI for PredictionCTF view functions.
// The result is cached after the first call.
func ParsePredictionCTFABI() abi.ABI {
	ctfABIOnce.Do(func() {
		parsed, err := abi.JSON(strings.NewReader(predictionCTFABIJSON))
		if err != nil {
			panic("failed to parse PredictionCTF ABI: " + err.Error())
		}
		parsedCTFABI = parsed
	})
	return parsedCTFABI
}

// PackPriceCall packs a call to price(uint256 option).
func PackPriceCall(option *big.Int) ([]byte, error) {
	return ParsePredictionCTFABI().Pack("price", option)
}

// UnpackPriceResult unpacks the return value of price().
func UnpackPriceResult(data []byte) (*big.Int, error) {
	values, err := ParsePredictionCTFABI().Unpack("price", data)
	if err != nil {
		return nil, err
	}
	return values[0].(*big.Int), nil
}

// PackGetAmountOutCall packs a call to getAmountOut(uint256 optionOut, uint256 delta).
func PackGetAmountOutCall(optionOut, delta *big.Int) ([]byte, error) {
	return ParsePredictionCTFABI().Pack("getAmountOut", optionOut, delta)
}

// UnpackGetAmountOutResult unpacks the return value of getAmountOut() (int256 SD59x18, unwrap = baseToken decimals).
func UnpackGetAmountOutResult(data []byte) (*big.Int, error) {
	values, err := ParsePredictionCTFABI().Unpack("getAmountOut", data)
	if err != nil {
		return nil, err
	}
	return values[0].(*big.Int), nil
}

// PackGetAmountsOutCall packs a call to getAmountsOut(int256[] x).
func PackGetAmountsOutCall(deltas []*big.Int) ([]byte, error) {
	return ParsePredictionCTFABI().Pack("getAmountsOut", deltas)
}

// UnpackGetAmountsOutResult unpacks the return value of getAmountsOut() (int256 SD59x18, unwrap = baseToken decimals).
func UnpackGetAmountsOutResult(data []byte) (*big.Int, error) {
	values, err := ParsePredictionCTFABI().Unpack("getAmountsOut", data)
	if err != nil {
		return nil, err
	}
	return values[0].(*big.Int), nil
}

// PackNumOutcomesCall packs a call to numOutcomes().
func PackNumOutcomesCall() ([]byte, error) {
	return ParsePredictionCTFABI().Pack("numOutcomes")
}

// UnpackNumOutcomesResult unpacks the return value of numOutcomes().
func UnpackNumOutcomesResult(data []byte) (*big.Int, error) {
	values, err := ParsePredictionCTFABI().Unpack("numOutcomes", data)
	if err != nil {
		return nil, err
	}
	return values[0].(*big.Int), nil
}

// PackStateCall packs a call to state().
func PackStateCall() ([]byte, error) {
	return ParsePredictionCTFABI().Pack("state")
}

// UnpackStateFee unpacks the fee (first return value) from state().
// The fee is in basis points (10_000 = 100%).
func UnpackStateFee(data []byte) (*big.Int, error) {
	values, err := ParsePredictionCTFABI().Unpack("state", data)
	if err != nil {
		return nil, err
	}
	return values[0].(*big.Int), nil
}
