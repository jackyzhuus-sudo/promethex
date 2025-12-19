package server

import (
	"github.com/google/wire"
)

// ProviderSet 是server providers
var ProviderSet = wire.NewSet(NewBlockScannerServer, NewEventProcessorServer, NewSchedulerServer)
