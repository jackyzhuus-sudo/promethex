package contract

import (
	"github.com/go-kratos/kratos/v2/log"
	"github.com/google/wire"
)

var ProviderSet = wire.NewSet(NewContract)

type Contract struct {
	log                log.Logger
	FactoryContract    *FactoryContract
	PredictionContract *PredictionContract
	OptionContract     *OptionContract
	Erc20Contract      *ERC20Contract
}

func NewContract(logger log.Logger) (*Contract, error) {
	factoryContract, err := NewFactoryContract(logger)
	if err != nil {
		return nil, err
	}
	predictionContract, err := NewPredictionContract(logger)
	if err != nil {
		return nil, err
	}
	optionContract, err := NewOptionContract(logger)
	if err != nil {
		return nil, err
	}
	erc20Contract, err := NewERC20Contract(logger)
	if err != nil {
		return nil, err
	}

	return &Contract{
		log:                logger,
		FactoryContract:    factoryContract,
		PredictionContract: predictionContract,
		OptionContract:     optionContract,
		Erc20Contract:      erc20Contract,
	}, nil
}
