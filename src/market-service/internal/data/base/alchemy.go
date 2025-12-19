package base

import (
	"context"
	"fmt"
	"market-service/internal/conf"
	"net/http"
	"time"

	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/go-kratos/kratos/v2/log"
)

// Alchemy客户端定义
type AlchemyClient struct {
	HttpClient        *http.Client
	EthClient         *ethclient.Client
	Url               string
	ApiKey            string
	ChainID           uint64
	PolicyID          string
	Network           string
	EntrypointAddress string
	FallbackAddress   string
	SignTxFromAddress string
	SignTxPrivateKey  string
}

func newAlchemyClient(c *conf.Data) *AlchemyClient {
	httpClient := http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        10,
			MaxIdleConnsPerHost: 5,
			MaxConnsPerHost:     10,
			IdleConnTimeout:     90 * time.Second,
			TLSHandshakeTimeout: 2 * time.Second,
		},
	}

	arbRpcUrl := fmt.Sprintf("https://%s.g.alchemy.com/v2/%s", c.Alchemy.Network, c.Alchemy.ApiKey)
	rpcClient, err := rpc.DialContext(context.Background(), arbRpcUrl)
	if err != nil {
		log.Fatal(err)
	}
	ethClient := ethclient.NewClient(rpcClient)

	return &AlchemyClient{
		HttpClient:        &httpClient,
		EthClient:         ethClient,
		Url:               arbRpcUrl,
		ApiKey:            c.Alchemy.ApiKey,
		ChainID:           c.Alchemy.ChainId,
		PolicyID:          c.Alchemy.PolicyId,
		Network:           c.Alchemy.Network,
		EntrypointAddress: c.Alchemy.EntrypointAddress,
		FallbackAddress:   c.Alchemy.FallbackAddress,
		SignTxFromAddress: c.Alchemy.SignTxFromAddress,
		SignTxPrivateKey:  c.Alchemy.SignTxPrivateKey,
	}
}
