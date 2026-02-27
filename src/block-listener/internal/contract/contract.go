package contract

import (
	"github.com/go-kratos/kratos/v2/log"
	"github.com/google/wire"
)

var ProviderSet = wire.NewSet(NewContract)

type Contract struct {
	log                       log.Logger
	ConditionalTokensContract *ConditionalTokensContract
	PredictionCTFContract     *PredictionCTFContract
}

func NewContract(logger log.Logger) (*Contract, error) {
	conditionalTokensContract, err := NewConditionalTokensContract(logger)
	if err != nil {
		return nil, err
	}
	predictionCTFContract, err := NewPredictionCTFContract(logger)
	if err != nil {
		return nil, err
	}

	return &Contract{
		log:                       logger,
		ConditionalTokensContract: conditionalTokensContract,
		PredictionCTFContract:     predictionCTFContract,
	}, nil
}
