package crontask

import (
	"github.com/google/wire"
)

var ProviderSet = wire.NewSet(
	NewUpdateUserAssetProcessor,
)
