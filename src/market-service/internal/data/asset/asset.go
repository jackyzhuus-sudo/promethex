package asset

import (
	"bytes"
	"crypto/ecdsa"
	"encoding/json"
	"fmt"
	"io"
	"market-service/internal/data/base"
	"market-service/internal/pkg/common"
	"math/big"
	"net/http"
	"strings"
	"time"

	assetBiz "market-service/internal/biz/asset"
	assetModel "market-service/internal/model/marketcenter/asset"

	"github.com/bitly/go-simplejson"
	ethCommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/shopspring/decimal"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type AssetRepo struct {
	base.MarketcenterInfra
}

func NewAssetRepo(infra base.MarketcenterInfra) assetBiz.AssetRepoInterface {
	return &AssetRepo{
		infra,
	}
}

// GetSendTxsWithTotal 获取send_tx表的记录，并返回总数
func (r *AssetRepo) GetSendTxsWithTotal(ctx common.Ctx, query *assetBiz.SendTxQuery) ([]*assetBiz.SendTxEntity, int64, error) {
	var total int64
	db := query.Condition(r.GetDb().WithContext(ctx.Ctx).Model(&assetModel.SendTx{}), &total)
	var sendTxs []*assetModel.SendTx
	if err := db.Find(&sendTxs).Error; err != nil {
		ctx.Log.Errorf("GetSendTxsWithTotal sql failed, err: %v", err)
		return nil, 0, err
	}
	sendTxEntities := make([]*assetBiz.SendTxEntity, 0, len(sendTxs))
	for _, sendTx := range sendTxs {
		sendTxEntities = append(sendTxEntities, sendTx.ToEntity())
	}
	return sendTxEntities, total, nil
}

func (r *AssetRepo) GetUserClaimResult(ctx common.Ctx, query *assetBiz.UserClaimResultQuery) (*assetBiz.UserClaimResultEntity, error) {
	db := query.Condition(r.GetDb().WithContext(ctx.Ctx).Model(&assetModel.UserClaimResult{}), nil)
	var userClaimResult assetModel.UserClaimResult
	if err := db.First(&userClaimResult).Error; err != nil && err != gorm.ErrRecordNotFound {
		ctx.Log.Errorf("GetUserClaimResult sql failed, err: %v", err)
		return nil, err
	}
	return userClaimResult.ToEntity(), nil
}

func (r *AssetRepo) GetUserTokenBalance(ctx common.Ctx, query *assetBiz.UserTokenBalanceQuery) ([]*assetBiz.UserTokenBalanceEntity, error) {
	db := query.Condition(r.GetDb().WithContext(ctx.Ctx).Model(&assetModel.UserTokenBalance{}), nil)
	var userTokenBalances []*assetModel.UserTokenBalance
	if err := db.Find(&userTokenBalances).Error; err != nil {
		ctx.Log.Errorf("GetUserTokenBalance sql failed, err: %v", err)
		return nil, err
	}
	userTokenBalanceEntities := make([]*assetBiz.UserTokenBalanceEntity, 0, len(userTokenBalances))
	for _, userTokenBalance := range userTokenBalances {
		userTokenBalanceEntities = append(userTokenBalanceEntities, userTokenBalance.ToEntity())
	}
	return userTokenBalanceEntities, nil
}

func (r *AssetRepo) GetUserTokenBalanceWithTotal(ctx common.Ctx, query *assetBiz.UserTokenBalanceQuery) ([]*assetBiz.UserTokenBalanceEntity, int64, error) {
	var total int64
	db := query.Condition(r.GetDb().WithContext(ctx.Ctx).Model(&assetModel.UserTokenBalance{}), &total)
	var userTokenBalances []*assetModel.UserTokenBalance
	if err := db.Find(&userTokenBalances).Error; err != nil {
		ctx.Log.Errorf("GetUserTokenBalanceWithTotal sql failed, err: %v", err)
		return nil, 0, err
	}
	userTokenBalanceEntities := make([]*assetBiz.UserTokenBalanceEntity, 0, len(userTokenBalances))
	for _, userTokenBalance := range userTokenBalances {
		userTokenBalanceEntities = append(userTokenBalanceEntities, userTokenBalance.ToEntity())
	}
	return userTokenBalanceEntities, total, nil
}

func (r *AssetRepo) CreateOrUpdateUserTokenBalance(ctx common.Ctx, userTokenBalanceEntity *assetBiz.UserTokenBalanceEntity) error {
	userTokenBalanceModel := &assetModel.UserTokenBalance{}
	userTokenBalanceModel.FromEntity(userTokenBalanceEntity)
	db := common.GetDB(ctx.Ctx, r.GetDb())
	return db.Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: "uid"},
			{Name: "token_address"}, // 联合唯一的多个字段
		},
		DoUpdates: clause.AssignmentColumns([]string{"balance", "block_number"}), // 要更新的字段
	}).Create(&userTokenBalanceModel).Error
}

func (r *AssetRepo) CreateUserTokenBalance(ctx common.Ctx, userTokenBalanceEntity *assetBiz.UserTokenBalanceEntity) error {
	userTokenBalanceModel := &assetModel.UserTokenBalance{}
	userTokenBalanceModel.FromEntity(userTokenBalanceEntity)
	return r.Create(ctx, userTokenBalanceModel)
}

func (r *AssetRepo) UpdateUserTokenBalanceByUidAndTokenAddress(ctx common.Ctx, userTokenBalanceEntity *assetBiz.UserTokenBalanceEntity, updateMap map[string]interface{}) error {
	db := common.GetDB(ctx.Ctx, r.GetDb())
	return db.Model(&assetModel.UserTokenBalance{}).
		Where("uid = ?", userTokenBalanceEntity.UID).
		Where("token_address = ?", userTokenBalanceEntity.TokenAddress).
		Where("block_number < ?", updateMap["block_number"]).
		Updates(updateMap).Error
}

func (r *AssetRepo) MintERC20Token(ctx common.Ctx, tokenAddress string, to string, amount *big.Int) (string, error) {

	privateKey, err := crypto.HexToECDSA(r.GetAlchemyClient().SignTxPrivateKey)
	if err != nil {
		return "", fmt.Errorf("无法解析私钥: %v", err)
	}

	publicKey := privateKey.Public()
	publicKeyECDSA, ok := publicKey.(*ecdsa.PublicKey)
	if !ok {
		return "", fmt.Errorf("无法获取公钥")
	}
	fromAddress := crypto.PubkeyToAddress(*publicKeyECDSA)

	client := r.GetAlchemyClient().EthClient
	nonce, err := client.PendingNonceAt(ctx.Ctx, fromAddress)
	if err != nil {
		return "", fmt.Errorf("获取nonce失败: %v", err)
	}

	// 获取当前区块的baseFee
	header, err := client.HeaderByNumber(ctx.Ctx, nil)
	if err != nil {
		return "", fmt.Errorf("获取区块信息失败: %v", err)
	}

	// 计算合适的gas价格
	baseFee := header.BaseFee
	if baseFee == nil {
		return "", fmt.Errorf("获取baseFee失败")
	}

	// 设置maxFeePerGas为baseFee的1.5倍，确保有足够的余量
	maxFeePerGas := new(big.Int).Mul(baseFee, big.NewInt(15))
	maxFeePerGas = maxFeePerGas.Div(maxFeePerGas, big.NewInt(10))

	// 设置maxPriorityFeePerGas (小费)
	maxPriorityFeePerGas := big.NewInt(2000000000) // 2 Gwei

	// 构造mint方法的调用数据
	transferFnSignature := []byte("mint(address,uint256)")
	hash := crypto.Keccak256Hash(transferFnSignature)
	methodID := hash[:4]

	toAddr := ethCommon.HexToAddress(to)
	paddedAddress := ethCommon.LeftPadBytes(toAddr.Bytes(), 32)
	paddedAmount := ethCommon.LeftPadBytes(amount.Bytes(), 32)

	var data []byte
	data = append(data, methodID...)
	data = append(data, paddedAddress...)
	data = append(data, paddedAmount...)

	// 构造交易
	tx := types.NewTransaction(
		nonce,
		ethCommon.HexToAddress(tokenAddress),
		big.NewInt(0),
		uint64(300000), // gas limit
		maxFeePerGas,
		data,
	)

	// 签名交易
	chainID, err := client.NetworkID(ctx.Ctx)
	if err != nil {
		return "", fmt.Errorf("获取chainID失败: %v", err)
	}

	// 更新交易的gas设置
	signedTx, err := types.SignNewTx(privateKey, types.LatestSignerForChainID(chainID), &types.DynamicFeeTx{
		ChainID:   chainID,
		Nonce:     nonce,
		To:        tx.To(),
		Value:     tx.Value(),
		Gas:       tx.Gas(),
		Data:      tx.Data(),
		GasTipCap: maxPriorityFeePerGas,
		GasFeeCap: maxFeePerGas,
	})
	if err != nil {
		return "", fmt.Errorf("签名交易失败: %v", err)
	}

	// 发送交易
	err = client.SendTransaction(ctx.Ctx, signedTx)
	if err != nil {
		return "", fmt.Errorf("发送交易失败: %v", err)
	}

	return signedTx.Hash().Hex(), nil
}

func (r *AssetRepo) GetTransactionReceipt(ctx common.Ctx, txHash string) (*types.Receipt, error) {
	return r.GetAlchemyClient().EthClient.TransactionReceipt(ctx.Ctx, ethCommon.HexToHash(txHash))
}

func (r *AssetRepo) RequestGasAndPaymasterAndData(ctx common.Ctx, userOperation *assetBiz.UserOperation) (*assetBiz.UserOperation, error) {

	reqHeader := map[string]string{
		"content-type": "application/json",
		"accept":       "application/json",
	}

	reqData := map[string]interface{}{
		"id":      1,
		"jsonrpc": "2.0",
		"method":  "alchemy_requestGasAndPaymasterAndData",
		"params": []interface{}{
			"webhookData111",
			r.GetAlchemyClient().PolicyID,
			r.GetAlchemyClient().EntrypointAddress,
			userOperation,
		},
	}
	var requestBody io.Reader
	jsonBody, err := json.Marshal(reqData)
	if err != nil {
		return nil, err
	}
	ctx.Log.Infof("RequestGasAndPaymasterAndData request body: %s", string(jsonBody))
	requestBody = bytes.NewBuffer(jsonBody)

	req, err := http.NewRequest("POST", r.GetAlchemyClient().Url, requestBody)
	if err != nil {
		return nil, err
	}

	for key, value := range reqHeader {
		req.Header.Set(key, value)
	}
	req.Header.Set("Content-Type", "application/json")

	rsp, err := r.GetAlchemyClient().HttpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer rsp.Body.Close()

	// 只读取一次响应体
	respBody, err := io.ReadAll(rsp.Body)
	if err != nil {
		return nil, err
	}
	ctx.Log.Infof("RequestGasAndPaymasterAndData response body: %s", string(respBody))

	if rsp.StatusCode != 200 {
		ctx.Log.Errorf("requestGasAndPaymasterAndData HTTP request failed with status code: %d, body: %s", rsp.StatusCode, respBody)
		return nil, fmt.Errorf("requestGasAndPaymasterAndData HTTP request failed. HTTP status code: [%d]", rsp.StatusCode)
	}

	// 解析为simplejson
	rspJson, err := simplejson.NewJson(respBody)
	if err != nil {
		return nil, err
	}

	rspError := rspJson.Get("error")
	if rspError != nil {
		code := rspError.Get("code").MustInt()
		message := rspError.Get("message").MustString()
		if code != 0 {
			return nil, fmt.Errorf("alchemy get paymaster data failed. code: %d, message: %s", code, message)
		}
	}

	entrypointV07Response := rspJson.Get("result").Get("entrypointV07Response")

	paymaster := entrypointV07Response.Get("paymaster").MustString()
	paymasterData := entrypointV07Response.Get("paymasterData").MustString()
	paymasterVerificationGasLimit := entrypointV07Response.Get("paymasterVerificationGasLimit").MustString()
	paymasterPostOpGasLimit := entrypointV07Response.Get("paymasterPostOpGasLimit").MustString()

	userOperation.Paymaster = paymaster
	userOperation.PaymasterData = paymasterData
	userOperation.PaymasterVerificationGasLimit = paymasterVerificationGasLimit
	userOperation.PaymasterPostOpGasLimit = paymasterPostOpGasLimit
	return userOperation, nil

}

func (r *AssetRepo) GetPaymasterStubData(ctx common.Ctx, userOperation *assetBiz.UserOperation) (string, string, error) {

	reqHeader := map[string]string{
		"content-type": "application/json",
		"accept":       "application/json",
	}

	userOpJson, err := userOperation.MarshalJSON()
	if err != nil {
		ctx.Log.Errorf("GetPaymasterStubData userOperation MarshalJSON failed, err: %v", err)
		return "", "", err
	}
	reqData := map[string]interface{}{
		"id":      1,
		"jsonrpc": "2.0",
		"method":  "pm_getPaymasterStubData",
		"params": []interface{}{
			json.RawMessage(userOpJson),
			r.GetAlchemyClient().EntrypointAddress,
			hexutil.EncodeUint64(r.GetAlchemyClient().ChainID),
			map[string]string{
				"policyId":    r.GetAlchemyClient().PolicyID,
				"webhookData": "test",
			},
		},
	}
	var requestBody io.Reader
	jsonBody, err := json.Marshal(reqData)
	if err != nil {
		return "", "", err
	}
	requestBody = bytes.NewBuffer(jsonBody)
	ctx.Log.Infof("GetPaymasterStubData request body: %s", string(jsonBody))
	req, err := http.NewRequest("POST", r.GetAlchemyClient().Url, requestBody)
	if err != nil {
		return "", "", err
	}

	for key, value := range reqHeader {
		req.Header.Set(key, value)
	}
	req.Header.Set("Content-Type", "application/json")

	rsp, err := r.GetAlchemyClient().HttpClient.Do(req)
	if err != nil {
		return "", "", err
	}
	defer rsp.Body.Close()

	// 只读取一次响应体
	respBody, err := io.ReadAll(rsp.Body)
	if err != nil {
		return "", "", err
	}
	ctx.Log.Infof("GetPaymasterStubData response body: %s", string(respBody))

	if rsp.StatusCode != 200 {
		ctx.Log.Errorf("GetPaymasterStubData HTTP request failed with status code: %d, body: %s", rsp.StatusCode, respBody)
		return "", "", fmt.Errorf("GetPaymasterStubData HTTP request failed. HTTP status code: [%d]", rsp.StatusCode)
	}

	// 解析为simplejson
	rspJson, err := simplejson.NewJson(respBody)
	if err != nil {
		return "", "", err
	}

	rspError := rspJson.Get("error")
	if rspError != nil {
		code := rspError.Get("code").MustInt()
		message := rspError.Get("message").MustString()
		if code != 0 {
			return "", "", fmt.Errorf("alchemy get paymaster stub data failed. code: %d, message: %s", code, message)
		}
	}

	estimatePaymaster := rspJson.Get("result").Get("paymaster").MustString()
	estimatePaymasterData := rspJson.Get("result").Get("paymasterData").MustString()
	return estimatePaymaster, estimatePaymasterData, nil
}

func (r *AssetRepo) EstimateUserOperationGas(ctx common.Ctx, userOperation *assetBiz.UserOperation) (string, string, string, string, error) {

	reqHeader := map[string]string{
		"content-type": "application/json",
		"accept":       "application/json",
	}

	userOpJson, err := userOperation.MarshalJSON()
	if err != nil {
		ctx.Log.Errorf("EstimateUserOperationGas userOperation MarshalJSON failed, err: %v", err)
		return "", "", "", "", err
	}
	reqData := map[string]interface{}{
		"id":      1,
		"jsonrpc": "2.0",
		"method":  "eth_estimateUserOperationGas",
		"params": []interface{}{
			json.RawMessage(userOpJson),
			r.GetAlchemyClient().EntrypointAddress,
		},
	}
	var requestBody io.Reader
	jsonBody, err := json.Marshal(reqData)
	if err != nil {
		return "", "", "", "", err
	}
	requestBody = bytes.NewBuffer(jsonBody)
	ctx.Log.Infof("EstimateUserOperationGas request body: %s", string(jsonBody))
	req, err := http.NewRequest("POST", r.GetAlchemyClient().Url, requestBody)
	if err != nil {
		return "", "", "", "", err
	}

	for key, value := range reqHeader {
		req.Header.Set(key, value)
	}
	req.Header.Set("Content-Type", "application/json")

	rsp, err := r.GetAlchemyClient().HttpClient.Do(req)
	if err != nil {
		return "", "", "", "", err
	}
	defer rsp.Body.Close()

	// 只读取一次响应体
	respBody, err := io.ReadAll(rsp.Body)
	if err != nil {
		return "", "", "", "", err
	}
	ctx.Log.Infof("EstimateUserOperationGas response body: %s", string(respBody))

	if rsp.StatusCode != 200 {
		ctx.Log.Errorf("EstimateUserOperationGas HTTP request failed with status code: %d, body: %s", rsp.StatusCode, respBody)
		return "", "", "", "", fmt.Errorf("EstimateUserOperationGas HTTP request failed. HTTP status code: [%d]", rsp.StatusCode)
	}

	// 解析为simplejson
	rspJson, err := simplejson.NewJson(respBody)
	if err != nil {
		return "", "", "", "", err
	}

	rspError := rspJson.Get("error")
	if rspError != nil {
		code := rspError.Get("code").MustInt()
		message := rspError.Get("message").MustString()
		if code != 0 {
			return "", "", "", "", fmt.Errorf("alchemy estimate user operation gas failed. code: %d, message: %s", code, message)
		}
	}

	preVerificationGas := rspJson.Get("result").Get("preVerificationGas").MustString()
	verificationGasLimit := rspJson.Get("result").Get("verificationGasLimit").MustString()
	callGasLimit := rspJson.Get("result").Get("callGasLimit").MustString()
	paymasterVerificationGasLimit := rspJson.Get("result").Get("paymasterVerificationGasLimit").MustString()
	return preVerificationGas, verificationGasLimit, callGasLimit, paymasterVerificationGasLimit, nil

}

func (r *AssetRepo) GetPaymasterData(ctx common.Ctx, userOperation *assetBiz.UserOperation) (string, string, error) {

	reqHeader := map[string]string{
		"content-type": "application/json",
		"accept":       "application/json",
	}

	userOpJson, err := userOperation.MarshalJSON()
	if err != nil {
		ctx.Log.Errorf("GetPaymasterData userOperation MarshalJSON failed, err: %v", err)
		return "", "", err
	}
	reqData := map[string]interface{}{
		"id":      1,
		"jsonrpc": "2.0",
		"method":  "pm_getPaymasterData",
		"params": []interface{}{
			json.RawMessage(userOpJson),
			r.GetAlchemyClient().EntrypointAddress,
			hexutil.EncodeUint64(r.GetAlchemyClient().ChainID),
			map[string]string{
				"policyId":    r.GetAlchemyClient().PolicyID,
				"webhookData": "test",
			},
		},
	}

	var requestBody io.Reader
	jsonBody, err := json.Marshal(reqData)
	if err != nil {
		return "", "", err
	}
	requestBody = bytes.NewBuffer(jsonBody)
	ctx.Log.Infof("GetPaymasterData request body: %s", string(jsonBody))
	req, err := http.NewRequest("POST", r.GetAlchemyClient().Url, requestBody)
	if err != nil {
		return "", "", err
	}

	for key, value := range reqHeader {
		req.Header.Set(key, value)
	}
	req.Header.Set("Content-Type", "application/json")

	rsp, err := r.GetAlchemyClient().HttpClient.Do(req)
	if err != nil {
		return "", "", err
	}
	defer rsp.Body.Close()

	// 只读取一次响应体
	respBody, err := io.ReadAll(rsp.Body)
	if err != nil {
		return "", "", err
	}
	ctx.Log.Infof("GetPaymasterData response body: %s", string(respBody))

	if rsp.StatusCode != 200 {
		ctx.Log.Errorf("GetPaymasterData HTTP request failed with status code: %d, body: %s", rsp.StatusCode, respBody)
		return "", "", fmt.Errorf("GetPaymasterData HTTP request failed. HTTP status code: [%d]", rsp.StatusCode)
	}

	// 解析为simplejson
	rspJson, err := simplejson.NewJson(respBody)
	if err != nil {
		return "", "", err
	}

	rspError := rspJson.Get("error")
	if rspError != nil {
		code := rspError.Get("code").MustInt()
		message := rspError.Get("message").MustString()
		if code != 0 {
			return "", "", fmt.Errorf("alchemy get paymaster data failed. code: %d, message: %s", code, message)
		}
	}

	paymaster := rspJson.Get("result").Get("paymaster").MustString()
	paymasterData := rspJson.Get("result").Get("paymasterData").MustString()
	return paymaster, paymasterData, nil

}

func (r *AssetRepo) GetUserOperationReceipt(ctx common.Ctx, userOpHash string) (*simplejson.Json, error) {
	reqHeader := map[string]string{
		"content-type": "application/json",
		"accept":       "application/json",
	}

	reqData := map[string]interface{}{
		"id":      1,
		"jsonrpc": "2.0",
		"method":  "eth_getUserOperationReceipt",
		"params":  []interface{}{userOpHash},
	}

	var requestBody io.Reader
	jsonBody, err := json.Marshal(reqData)
	if err != nil {
		return nil, err
	}
	requestBody = bytes.NewBuffer(jsonBody)

	ctx.Log.Infof("GetUserOperationReceipt request body: %s", string(jsonBody))
	req, err := http.NewRequest("POST", r.GetAlchemyClient().Url, requestBody)
	if err != nil {
		return nil, err
	}

	for key, value := range reqHeader {
		req.Header.Set(key, value)
	}
	req.Header.Set("Content-Type", "application/json")

	rsp, err := r.GetAlchemyClient().HttpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer rsp.Body.Close()

	// 只读取一次响应体
	respBody, err := io.ReadAll(rsp.Body)
	if err != nil {
		return nil, err
	}
	ctx.Log.Infof("GetUserOperationReceipt response body: %s", string(respBody))
	if rsp.StatusCode != 200 {
		ctx.Log.Errorf("GetUserOperationReceipt HTTP request failed with status code: %d, body: %s", rsp.StatusCode, respBody)
		return nil, fmt.Errorf("GetUserOperationReceipt HTTP request failed. HTTP status code: [%d]", rsp.StatusCode)
	}

	// 解析为simplejson
	rspJson, err := simplejson.NewJson(respBody)
	if err != nil {
		return nil, err
	}

	rspError := rspJson.Get("error")
	if rspError != nil {
		code := rspError.Get("code").MustInt()
		message := rspError.Get("message").MustString()
		if code != 0 {
			return nil, fmt.Errorf("alchemy send user operation failed. code: %d, message: %s", code, message)
		}
	}

	return rspJson.Get("result"), nil
}

func (r *AssetRepo) SendUserOperationToAlchemy(ctx common.Ctx, userOperation *assetBiz.UserOperation) (string, error) {

	reqHeader := map[string]string{
		"content-type": "application/json",
		"accept":       "application/json",
	}

	userOpJson, err := userOperation.MarshalJSON()
	if err != nil {
		ctx.Log.Errorf("SendUserOperationToAlchemy userOperation MarshalJSON failed, err: %v", err)
		return "", err
	}
	reqData := map[string]interface{}{
		"id":      1,
		"jsonrpc": "2.0",
		"method":  "eth_sendUserOperation",
		"params": []interface{}{
			json.RawMessage(userOpJson),
			r.GetAlchemyClient().EntrypointAddress,
		},
	}

	var requestBody io.Reader
	jsonBody, err := json.Marshal(reqData)
	if err != nil {
		return "", err
	}
	requestBody = bytes.NewBuffer(jsonBody)

	ctx.Log.Infof("SendUserOperationToAlchemy request body: %s", string(jsonBody))
	req, err := http.NewRequest("POST", r.GetAlchemyClient().Url, requestBody)
	if err != nil {
		return "", err
	}

	for key, value := range reqHeader {
		req.Header.Set(key, value)
	}
	req.Header.Set("Content-Type", "application/json")

	rsp, err := r.GetAlchemyClient().HttpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer rsp.Body.Close()

	// 只读取一次响应体
	respBody, err := io.ReadAll(rsp.Body)
	if err != nil {
		return "", err
	}
	ctx.Log.Infof("SendUserOperationToAlchemy response body: %s", string(respBody))

	if rsp.StatusCode != 200 {
		ctx.Log.Errorf("HTTP request failed with status code: %d, body: %s", rsp.StatusCode, respBody)
		return "", fmt.Errorf("HTTP request failed. HTTP status code: [%d]", rsp.StatusCode)
	}

	// 解析为simplejson
	rspJson, err := simplejson.NewJson(respBody)
	if err != nil {
		return "", err
	}

	rspError := rspJson.Get("error")
	if rspError != nil {
		code := rspError.Get("code").MustInt()
		message := rspError.Get("message").MustString()
		if code != 0 {
			return "", fmt.Errorf("alchemy send user operation failed. code: %d, message: %s", code, message)
		}
	}

	opHash := rspJson.Get("result").MustString()
	return opHash, nil
}

func (r *AssetRepo) UpdateUserClaimResultByOpHash(ctx common.Ctx, userClaimResultEntity *assetBiz.UserClaimResultEntity, updateMap map[string]interface{}) error {
	db := common.GetDB(ctx.Ctx, r.GetDb())
	return db.Model(&assetModel.UserClaimResult{}).Where("op_hash = ?", userClaimResultEntity.OpHash).Updates(updateMap).Error
}

func (r *AssetRepo) UpdateUserClaimResultByTxHash(ctx common.Ctx, userClaimResultEntity *assetBiz.UserClaimResultEntity, updateMap map[string]interface{}) error {
	db := common.GetDB(ctx.Ctx, r.GetDb())
	return db.Model(&assetModel.UserClaimResult{}).Where("tx_hash = ?", userClaimResultEntity.TxHash).Updates(updateMap).Error
}

func (r *AssetRepo) CreateSendTx(ctx common.Ctx, sendTxEntity *assetBiz.SendTxEntity) error {
	txModel := &assetModel.SendTx{}
	txModel.FromEntity(sendTxEntity)
	return r.Create(ctx, txModel)
}

func (r *AssetRepo) UpdateOrderByTxHash(ctx common.Ctx, orderEntity *assetBiz.OrderEntity, updateMap map[string]interface{}) error {
	db := common.GetDB(ctx.Ctx, r.GetDb())
	return db.Model(&assetModel.Order{}).Where("tx_hash = ?", orderEntity.TxHash).Updates(updateMap).Error
}

func (r *AssetRepo) UpdateOrderByOpHash(ctx common.Ctx, orderEntity *assetBiz.OrderEntity, updateMap map[string]interface{}) error {
	db := common.GetDB(ctx.Ctx, r.GetDb())
	return db.Model(&assetModel.Order{}).Where("op_hash = ?", orderEntity.OpHash).Updates(updateMap).Error
}

func (r *AssetRepo) UpdateSendTxByOpHash(ctx common.Ctx, sendTxEntity *assetBiz.SendTxEntity, updateMap map[string]interface{}) error {
	db := common.GetDB(ctx.Ctx, r.GetDb())
	return db.Model(&assetModel.SendTx{}).Where("op_hash = ?", sendTxEntity.OpHash).Updates(updateMap).Error
}

func (r *AssetRepo) CreateOrder(ctx common.Ctx, orderEntity *assetBiz.OrderEntity) error {
	orderModel := &assetModel.Order{}
	orderModel.FromEntity(orderEntity)
	return r.Create(ctx, orderModel)
}

func (r *AssetRepo) CreateOrUpdateOrder(ctx common.Ctx, orderEntity *assetBiz.OrderEntity, updateColumns []string) error {
	orderModel := &assetModel.Order{}
	orderModel.FromEntity(orderEntity)
	db := common.GetDB(ctx.Ctx, r.GetDb())
	return db.Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: "tx_hash"},
		},
		DoUpdates: clause.AssignmentColumns(updateColumns),
	}).Create(&orderModel).Error
}

func (r *AssetRepo) CreateOrUpdateUserClaimResult(ctx common.Ctx, claimResultEntity *assetBiz.UserClaimResultEntity, updateColumns []string) error {
	claimResultModel := &assetModel.UserClaimResult{}
	claimResultModel.FromEntity(claimResultEntity)
	db := common.GetDB(ctx.Ctx, r.GetDb())
	return db.Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: "tx_hash"},
		},
		DoUpdates: clause.AssignmentColumns(updateColumns),
	}).Create(&claimResultModel).Error
}

func (r *AssetRepo) CreateOrUpdateUserTransferTokens(ctx common.Ctx, userTransferTokensEntity *assetBiz.UserTransferTokensEntity, updateColumns []string) error {
	userTransferTokensModel := &assetModel.UserTransferTokens{}
	userTransferTokensModel.FromEntity(userTransferTokensEntity)
	db := common.GetDB(ctx.Ctx, r.GetDb())
	return db.Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: "tx_hash"},
		},
		DoUpdates: clause.AssignmentColumns(updateColumns),
	}).Create(&userTransferTokensModel).Error
}

func (r *AssetRepo) CreateUserClaimResult(ctx common.Ctx, userClaimResultEntity *assetBiz.UserClaimResultEntity) error {
	userClaimResultModel := &assetModel.UserClaimResult{}
	userClaimResultModel.FromEntity(userClaimResultEntity)
	return r.Create(ctx, userClaimResultModel)
}

func (r *AssetRepo) GetOrdersWithTotal(ctx common.Ctx, query *assetBiz.OrderQuery) ([]*assetBiz.OrderEntity, int64, error) {
	var total int64
	db := query.Condition(r.GetDb().WithContext(ctx.Ctx).Model(&assetModel.Order{}), &total)
	var orders []*assetModel.Order
	if err := db.Find(&orders).Error; err != nil {
		ctx.Log.Errorf("GetOrdersWithTotal sql failed, err: %v", err)
		return nil, 0, err
	}
	orderEntities := make([]*assetBiz.OrderEntity, 0, len(orders))
	for _, order := range orders {
		orderEntities = append(orderEntities, order.ToEntity())
	}
	return orderEntities, total, nil
}

func (r *AssetRepo) GetUserMarketPositionsWithTotal(ctx common.Ctx, query *assetBiz.UserMarketPositionQuery) ([]*assetBiz.UserMarketPositionEntity, int64, error) {
	var total int64
	db := query.Condition(r.GetDb().WithContext(ctx.Ctx).Model(&assetModel.UserMarketPosition{}), &total)
	var userMarketPositions []*assetModel.UserMarketPosition
	err := db.Find(&userMarketPositions).Error
	if err != nil {
		ctx.Log.Errorf("GetUserMarketPositionsWithTotal sql failed, err: %v", err)
		return nil, 0, err
	}
	userMarketPositionEntities := make([]*assetBiz.UserMarketPositionEntity, 0, len(userMarketPositions))
	for _, userMarketPosition := range userMarketPositions {
		userMarketPositionEntities = append(userMarketPositionEntities, userMarketPosition.ToEntity())
	}
	return userMarketPositionEntities, total, nil
}

func (r *AssetRepo) GetUserAssetValue(ctx common.Ctx, query *assetBiz.UserAssetValueQuery) (*assetBiz.UserAssetValueEntity, error) {
	db := query.Condition(r.GetDb().WithContext(ctx.Ctx).Model(&assetModel.UserAssetValue{}), nil)
	userAssetValueModel := &assetModel.UserAssetValue{}
	if err := db.First(userAssetValueModel).Error; err != nil && err != gorm.ErrRecordNotFound {
		ctx.Log.Errorf("GetUserAssetValue sql failed, err: %v", err)
		return nil, err
	}
	return userAssetValueModel.ToEntity(), nil
}

func (r *AssetRepo) CreateOrUpdateUserMarketPosition(ctx common.Ctx, userMarketPositionEntity *assetBiz.UserMarketPositionEntity) error {
	userMarketPositionModel := &assetModel.UserMarketPosition{}
	userMarketPositionModel.FromEntity(userMarketPositionEntity)
	db := common.GetDB(ctx.Ctx, r.GetDb())
	return db.Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: "uid"},
			{Name: "market_address"},
		},
		DoUpdates: clause.Assignments(map[string]interface{}{
			"total_value": gorm.Expr(userMarketPositionModel.TableName()+".total_value + ?", userMarketPositionEntity.TotalValue),
		}),
	}).Create(&userMarketPositionModel).Error
}

func (r *AssetRepo) UpdateUserMarketPositionDecrease(ctx common.Ctx, userMarketPositionEntity *assetBiz.UserMarketPositionEntity) error {
	db := common.GetDB(ctx.Ctx, r.GetDb())
	return db.Model(&assetModel.UserMarketPosition{}).Where("uid = ?", userMarketPositionEntity.UID).Where("market_address = ?", userMarketPositionEntity.MarketAddress).Updates(map[string]interface{}{
		"total_value": gorm.Expr(assetModel.UserMarketPosition{}.TableName()+".total_value - ?", userMarketPositionEntity.TotalValue),
	}).Error
}

func (r *AssetRepo) UpdateUserMarketPositionIncrease(ctx common.Ctx, userMarketPositionEntity *assetBiz.UserMarketPositionEntity) error {
	db := common.GetDB(ctx.Ctx, r.GetDb())
	return db.Model(&assetModel.UserMarketPosition{}).Where("uid = ?", userMarketPositionEntity.UID).Where("market_address = ?", userMarketPositionEntity.MarketAddress).Updates(map[string]interface{}{
		"total_value": gorm.Expr(assetModel.UserMarketPosition{}.TableName()+".total_value + ?", userMarketPositionEntity.TotalValue),
	}).Error
}

func (r *AssetRepo) GetUserAssetHistory(ctx common.Ctx, uid string, baseTokenType uint8, timeRange string) ([]*assetBiz.UserAssetValueEntity, error) {
	now := time.Now()
	var fromTime time.Time
	var interval string

	db := common.GetDB(ctx.Ctx, r.GetDb())
	// 根据不同时间范围使用合适的间隔
	switch timeRange {
	case "7d":
		fromTime = now.AddDate(0, 0, -7)
		interval = "hour"
	case "1m":
		fromTime = now.AddDate(0, -1, 0)
		interval = "day"
	case "3m":
		fromTime = now.AddDate(0, -3, 0)
		interval = "day"
	case "all":
		if err := db.Model(&assetModel.UserAssetValue{}).
			Where("uid = ?", uid).
			Where("base_token_type = ?", baseTokenType).
			Order("time ASC").
			Limit(1).
			Pluck("time", &fromTime).Error; err != nil {
			return nil, err
		}
		// 根据时间跨度选择合适的间隔
		duration := now.Sub(fromTime)
		switch {
		case duration <= 24*time.Hour:
			interval = "minute"
		case duration <= 7*24*time.Hour:
			interval = "hour"
		case duration <= 30*24*time.Hour:
			interval = "day"
		case duration <= 2*360*24*time.Hour:
			interval = "week"
		default:
			interval = "month"
		}
	default:
		return nil, fmt.Errorf("invalid time range: %s", timeRange)
	}

	query := `
        WITH sample_times AS (
            SELECT 
                date_trunc($1, time) as timestamp,
                FIRST_VALUE(value) OVER (
                    PARTITION BY date_trunc($1, time)
                    ORDER BY time DESC
                ) as value,
                FIRST_VALUE(balance) OVER (
                    PARTITION BY date_trunc($1, time)
                    ORDER BY time DESC
                ) as balance,
                FIRST_VALUE(portfolio) OVER (
                    PARTITION BY date_trunc($1, time)
                    ORDER BY time DESC
                ) as portfolio,
                FIRST_VALUE(pnl) OVER (
                    PARTITION BY date_trunc($1, time)
                    ORDER BY time DESC
                ) as pnl,
                FIRST_VALUE(base_token_type) OVER (
                    PARTITION BY date_trunc($1, time)
                    ORDER BY time DESC
                ) as base_token_type,
                FIRST_VALUE(asset_address) OVER (
                    PARTITION BY date_trunc($1, time)
                    ORDER BY time DESC
                ) as asset_address
            FROM t_user_asset_value
            WHERE uid = $2
                AND time >= $3
                AND time <= $4
				AND base_token_type = $5
        )
        SELECT DISTINCT
            timestamp as time,
            $2 as uid,
            asset_address,
            value,
            balance,
            portfolio,
            pnl,
            base_token_type
        FROM sample_times
        ORDER BY timestamp
    `

	var result []*assetModel.UserAssetValue
	err := db.Raw(query, interval, uid, fromTime, now, baseTokenType).Scan(&result).Error
	if err != nil {
		ctx.Log.Errorf("GetUserAssetHistory query failed: %v", err)
		return nil, err
	}

	userAssetValueEntities := make([]*assetBiz.UserAssetValueEntity, 0, len(result))
	for _, value := range result {
		userAssetValueEntities = append(userAssetValueEntities, value.ToEntity())
	}

	return userAssetValueEntities, nil
}

func (r *AssetRepo) GetUserTokenBalancesByQueryItems(ctx common.Ctx, queryItems []*assetBiz.UserTokenBalanceQueryItem) ([]*assetBiz.UserTokenBalanceEntity, error) {
	db := r.GetDb().WithContext(ctx.Ctx).Model(&assetModel.UserTokenBalance{})

	if len(queryItems) == 0 {
		return []*assetBiz.UserTokenBalanceEntity{}, nil
	}

	// 构建VALUES子句
	valuesParts := make([]string, len(queryItems))
	params := make([]interface{}, 0, len(queryItems)*2)

	for i, item := range queryItems {
		valuesParts[i] = "(?, ?)"
		params = append(params, item.MarketAddress, item.UID)
	}

	// 组装完整SQL
	query := fmt.Sprintf(`
		SELECT * 
		FROM t_user_token_balance 
		WHERE (market_address, uid) IN (VALUES %s)
		AND balance > %d
	`, strings.Join(valuesParts, ", "), assetBiz.MinPointBalance.IntPart())

	userTokenBalanceModels := []*assetModel.UserTokenBalance{}
	err := db.Raw(query, params...).Scan(&userTokenBalanceModels).Error

	if err != nil {
		ctx.Log.Errorf("GetUserTokenBalancesByQueryItems sql failed, err: %v", err)
		return nil, err
	}

	userTokenBalanceEntities := make([]*assetBiz.UserTokenBalanceEntity, 0, len(userTokenBalanceModels))
	for _, userTokenBalanceModel := range userTokenBalanceModels {
		userTokenBalanceEntities = append(userTokenBalanceEntities, userTokenBalanceModel.ToEntity())
	}
	return userTokenBalanceEntities, nil
}

func (r *AssetRepo) BatchCreateUserAssetValue(ctx common.Ctx, userAssetValueEntities []*assetBiz.UserAssetValueEntity) error {
	db := common.GetDB(ctx.Ctx, r.GetDb())
	userAssetValueModels := make([]*assetModel.UserAssetValue, 0, len(userAssetValueEntities))
	for _, userAssetValueEntity := range userAssetValueEntities {
		userAssetValueModel := &assetModel.UserAssetValue{}
		userAssetValueModel.FromEntity(userAssetValueEntity)
		userAssetValueModels = append(userAssetValueModels, userAssetValueModel)
	}
	return db.
		Clauses(clause.OnConflict{
			Columns: []clause.Column{
				{Name: "uid"},
				{Name: "asset_address"},
				{Name: "time"},
			},
			DoNothing: true,
		}).CreateInBatches(userAssetValueModels, 100).Error
}

func (r *AssetRepo) UpdateSendTxByTxHash(ctx common.Ctx, sendTxEntity *assetBiz.SendTxEntity, updateMap map[string]interface{}) error {
	db := common.GetDB(ctx.Ctx, r.GetDb())
	return db.Model(&assetModel.SendTx{}).Where("tx_hash = ?", sendTxEntity.TxHash).Updates(updateMap).Error
}

func (r *AssetRepo) GetSendTx(ctx common.Ctx, query *assetBiz.SendTxQuery) (*assetBiz.SendTxEntity, error) {
	db := query.Condition(common.GetDB(ctx.Ctx, r.GetDb()).Model(&assetModel.SendTx{}), nil)
	sendTxModel := &assetModel.SendTx{}
	if err := db.First(sendTxModel).Error; err != nil && err != gorm.ErrRecordNotFound {
		return nil, err
	}
	return sendTxModel.ToEntity(), nil
}

// UpdateUserPositionTokenEndStatus
func (r *AssetRepo) UpdateUserPositionTokenEndStatus(ctx common.Ctx, marketAddress string, winOptionAddress string) error {
	db := common.GetDB(ctx.Ctx, r.GetDb())

	result := db.Exec(`
			UPDATE t_user_token_balance
			SET 
				status = (CASE WHEN token_address = ? THEN ?::smallint ELSE ?::smallint END),
				updated_at = NOW()
			WHERE market_address = ? AND type = ? AND status = ?
		`, winOptionAddress, assetBiz.UserTokenBalanceStatusEndWin, assetBiz.UserTokenBalanceStatusEndLose,
		marketAddress, assetBiz.TypeUserTokenBalanceOption, assetBiz.UserTokenBalanceStatusHolding,
	)

	if result.Error != nil {
		ctx.Log.Errorf("update user token balance for market %s failed: %v", marketAddress, result.Error)
		return result.Error
	}

	ctx.Log.Infof("successfully updated total %d user token balances for market %s, win option: %s", result.RowsAffected, marketAddress, winOptionAddress)
	return nil
}

func (r *AssetRepo) UpdateUserMarketPositionStatus(ctx common.Ctx, marketAddress string, winOptionAddress string) error {
	db := common.GetDB(ctx.Ctx, r.GetDb())
	var totalUpdated int64
	batchSize := 200

	for {
		result := db.Model(&assetModel.UserMarketPosition{}).
			Where("market_address = ?", marketAddress).
			Where("status = ?", assetBiz.UserMarketPositionStatusHolding).
			Limit(batchSize).
			Updates(map[string]interface{}{
				"status": assetBiz.UserMarketPositionStatusEnd,
			})

		if result.Error != nil {
			ctx.Log.Errorf("update user market position for market %s failed: %v", marketAddress, result.Error)
			return result.Error
		}

		rowsAffected := result.RowsAffected
		totalUpdated += rowsAffected

		if rowsAffected < int64(batchSize) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	ctx.Log.Infof("successfully updated total %d user market positions for market %s", totalUpdated, marketAddress)
	return nil
}

func (r *AssetRepo) GetUserTotalValue(ctx common.Ctx, query *assetBiz.UserTokenBalanceQuery) (decimal.Decimal, error) {
	db := common.GetDB(ctx.Ctx, r.GetDb())
	uid := query.UID
	baseTokenType := query.BaseTokenType

	// 计算用户所有持仓的总价值
	var totalValue decimal.Decimal
	totalValueQuery := fmt.Sprintf(`
		SELECT COALESCE(SUM(utb.balance * latest_prices.price), 0) AS total_value
		FROM t_user_token_balance utb
		INNER JOIN LATERAL (
			SELECT price FROM t_option_token_price
			WHERE token_address = utb.token_address
			ORDER BY block_time DESC
			LIMIT 1
		) latest_prices ON true
		WHERE utb.uid = '%s' AND utb.type = %d AND utb.base_token_type = %d AND utb.status != %d AND utb.balance > 0
	`, uid, assetBiz.TypeUserTokenBalanceOption, baseTokenType, assetBiz.UserTokenBalanceStatusEndLose)

	if err := db.Raw(totalValueQuery).Scan(&totalValue).Error; err != nil {
		ctx.Log.Errorf("GetUserMarketPositionsByValue total value query failed: %v", err)
		return decimal.Zero, err
	}
	return totalValue, nil
}

// GetUserMarketPositionsByValue 获取用户持仓的市场列表，并按持仓总价值排序
func (r *AssetRepo) GetUserMarketPositionsByValue(ctx common.Ctx, query *assetBiz.UserTokenBalanceQuery) ([]*assetBiz.MarketValue, int64, error) {
	db := common.GetDB(ctx.Ctx, r.GetDb())

	uid := query.UID
	offset := query.Offset
	limit := query.Limit
	baseTokenType := query.BaseTokenType

	marketValues := make([]*assetBiz.MarketValue, 0)
	// 1. 首先获取符合条件的市场总数
	var totalCount int64
	countQuery := fmt.Sprintf(`
		SELECT COUNT(DISTINCT utb.market_address) AS total
		FROM t_user_token_balance utb
		INNER JOIN LATERAL (
			SELECT 1 FROM t_option_token_price
			WHERE token_address = utb.token_address
			LIMIT 1
		) has_price ON true
		WHERE utb.uid = '%s' AND utb.type = %d AND utb.base_token_type = %d AND utb.balance > 0 AND utb.status != %d
	`, uid, assetBiz.TypeUserTokenBalanceOption, baseTokenType, assetBiz.UserTokenBalanceStatusEndLose)

	if err := db.Raw(countQuery).Scan(&totalCount).Error; err != nil {
		ctx.Log.Errorf("GetUserMarketPositionsByValue count query failed: %v", err)
		return nil, 0, err
	}

	if totalCount == 0 {
		return marketValues, 0, nil
	}

	valueQuery := fmt.Sprintf(`
		SELECT 
			utb.market_address,
			COALESCE(SUM(utb.balance * latest_prices.price), 0) AS total_value
		FROM t_user_token_balance utb
		INNER JOIN LATERAL (
			SELECT price FROM t_option_token_price
			WHERE token_address = utb.token_address
			ORDER BY block_time DESC
			LIMIT 1
		) latest_prices ON true
		WHERE utb.uid = '%s' AND utb.type = %d AND utb.base_token_type = %d AND utb.balance > 0 AND utb.status != %d
		GROUP BY utb.market_address
		ORDER BY total_value DESC
		LIMIT %d OFFSET %d
	`, uid, assetBiz.TypeUserTokenBalanceOption, baseTokenType, assetBiz.UserTokenBalanceStatusEndLose,
		limit, offset)

	if err := db.Raw(valueQuery).Scan(&marketValues).Error; err != nil {
		ctx.Log.Errorf("GetUserMarketPositionsByValue value query failed: %v", err)
		return nil, 0, err
	}

	return marketValues, totalCount, nil
}

func (r *AssetRepo) CreateUserMintPoints(ctx common.Ctx, userMintPointsEntity *assetBiz.UserMintPointsEntity) error {
	db := common.GetDB(ctx.Ctx, r.GetDb())
	userMintPointsModel := &assetModel.UserMintPoints{}
	userMintPointsModel.FromEntity(userMintPointsEntity)
	return db.Create(userMintPointsModel).Error
}

func (r *AssetRepo) CreateUserTransferTokens(ctx common.Ctx, userTransferTokensEntity *assetBiz.UserTransferTokensEntity) error {
	db := common.GetDB(ctx.Ctx, r.GetDb())
	userTransferTokensModel := &assetModel.UserTransferTokens{}
	userTransferTokensModel.FromEntity(userTransferTokensEntity)
	return db.Create(userTransferTokensModel).Error
}

func (r *AssetRepo) GetSendTxs(ctx common.Ctx, query *assetBiz.SendTxQuery) ([]*assetBiz.SendTxEntity, error) {
	db := query.Condition(common.GetDB(ctx.Ctx, r.GetDb()).Model(&assetModel.SendTx{}), nil)
	sendTxModels := make([]*assetModel.SendTx, 0)
	if err := db.Find(&sendTxModels).Error; err != nil {
		ctx.Log.Errorf("GetSendTxs sql query failed: %v", err)
		return nil, err
	}
	sendTxEntities := make([]*assetBiz.SendTxEntity, 0, len(sendTxModels))
	for _, sendTxModel := range sendTxModels {
		sendTxEntities = append(sendTxEntities, sendTxModel.ToEntity())
	}
	return sendTxEntities, nil
}

func (r *AssetRepo) GetOrders(ctx common.Ctx, query *assetBiz.OrderQuery) ([]*assetBiz.OrderEntity, error) {
	db := query.Condition(common.GetDB(ctx.Ctx, r.GetDb()).Model(&assetModel.Order{}), nil)
	orderModels := make([]*assetModel.Order, 0)
	if err := db.Find(&orderModels).Error; err != nil {
		return []*assetBiz.OrderEntity{}, nil
	}

	orderEntities := make([]*assetBiz.OrderEntity, 0, len(orderModels))
	for _, orderModel := range orderModels {
		orderEntities = append(orderEntities, orderModel.ToEntity())
	}
	return orderEntities, nil
}

func (r *AssetRepo) GetUserClaimResults(ctx common.Ctx, query *assetBiz.UserClaimResultQuery) ([]*assetBiz.UserClaimResultEntity, error) {
	db := query.Condition(common.GetDB(ctx.Ctx, r.GetDb()).Model(&assetModel.UserClaimResult{}), nil)
	claimModels := make([]*assetModel.UserClaimResult, 0)
	if err := db.Find(&claimModels).Error; err != nil {
		ctx.Log.Errorf("GetUserClaimResults sql query failed: %v", err)
		return nil, err
	}

	claimEntities := make([]*assetBiz.UserClaimResultEntity, 0, len(claimModels))
	for _, claimModel := range claimModels {
		claimEntities = append(claimEntities, claimModel.ToEntity())
	}
	return claimEntities, nil
}

func (r *AssetRepo) GetUserMintPoints(ctx common.Ctx, query *assetBiz.UserMintPointsQuery) ([]*assetBiz.UserMintPointsEntity, error) {
	db := query.Condition(common.GetDB(ctx.Ctx, r.GetDb()).Model(&assetModel.UserMintPoints{}), nil)
	mintModels := make([]*assetModel.UserMintPoints, 0)
	if err := db.Find(&mintModels).Error; err != nil {
		ctx.Log.Errorf("GetUserMintPoints sql query failed: %v", err)
		return nil, err
	}

	mintEntities := make([]*assetBiz.UserMintPointsEntity, 0, len(mintModels))
	for _, mintModel := range mintModels {
		mintEntities = append(mintEntities, mintModel.ToEntity())
	}
	return mintEntities, nil
}

func (r *AssetRepo) GetUserTransferTokens(ctx common.Ctx, query *assetBiz.UserTransferTokensQuery) ([]*assetBiz.UserTransferTokensEntity, error) {
	db := query.Condition(common.GetDB(ctx.Ctx, r.GetDb()).Model(&assetModel.UserTransferTokens{}), nil)
	transferModels := make([]*assetModel.UserTransferTokens, 0)
	if err := db.Find(&transferModels).Error; err != nil {
		ctx.Log.Errorf("GetUserTransferTokens sql query failed: %v", err)
		return nil, err
	}

	transferEntities := make([]*assetBiz.UserTransferTokensEntity, 0, len(transferModels))
	for _, transferModel := range transferModels {
		transferEntities = append(transferEntities, transferModel.ToEntity())
	}
	return transferEntities, nil
}

func (r *AssetRepo) GetOrdersDistinctMarkets(ctx common.Ctx, query *assetBiz.OrderQuery) ([]string, error) {
	db := query.Condition(common.GetDB(ctx.Ctx, r.GetDb()).Model(&assetModel.Order{}).Select("DISTINCT market_address"), nil)
	marketAddresses := make([]string, 0)
	if err := db.Find(&marketAddresses).Error; err != nil {
		ctx.Log.Errorf("GetOrdersDistinctMarkets sql query failed: %v", err)
		return nil, err
	}
	return marketAddresses, nil
}

func (r *AssetRepo) GetUserEarnedPoints(ctx common.Ctx, uid string, source uint8) (decimal.Decimal, error) {
	db := common.GetDB(ctx.Ctx, r.GetDb()).Model(&assetModel.UserMintPoints{})
	db = db.Where("uid = ?", uid).Where("status = ?", assetBiz.UserMintPointsStatusSuccess)
	if source != 0 {
		db = db.Where("source = ?", source)
	}
	var total decimal.Decimal
	err := db.Select("COALESCE(SUM(amount), 0)").Scan(&total).Error
	if err != nil {
		ctx.Log.Errorf("GetUserEarnedPoints sql query failed: %v", err)
		return decimal.Zero, err
	}
	return total, nil
}

func (r *AssetRepo) UpdateUserTokenBalanceIsClaimed(ctx common.Ctx, uid string, optionAddressList []string) error {
	db := common.GetDB(ctx.Ctx, r.GetDb())
	err := db.Model(&assetModel.UserTokenBalance{}).
		Where("uid = ? AND token_address in ?", uid, optionAddressList).
		Update("is_claimed", assetBiz.UserTokenBalanceIsClaimedYes).Error
	if err != nil {
		ctx.Log.Errorf("UpdateUserTokenBalanceIsClaimed sql query failed: %v", err)
		return err
	}
	return nil
}
