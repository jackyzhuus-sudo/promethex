package sse

const (
	ChannelBroadcast    = "sse:broadcast" // 广播频道 -> BroadcastToAll
	ChannelUserPrefix   = "sse:user:"     // 用户频道 -> SendToUser 或 BroadcastToLoggedInUsers
	ChannelMarketPrefix = "sse:market:"   // 市场频道 -> SendToMarketTopic
)

// Msg
type Msg struct {
	Event string      `json:"event"`
	Data  interface{} `json:"data"`
}

type SSEMsg struct {
	Msg
	ID    string `json:"id"`
	Retry int    `json:"retry"`
}

const (
	EventConnected = "base:connected"
	EventPing      = "base:ping"

	EventUserPositionChanged = "user:position_changed"
	EventUserAssetChanged    = "user:asset_changed"
	EventUserNewNotification = "user:new_notification"

	EventMarketNewTrades   = "market:new_trades"
	EventMarketPriceUpdate = "market:price_change"
	EventMarketInfoUpdate  = "market:info_update"
)

type ConnectionMsgData struct {
	ConnectionID string `json:"connection_id"`
}

type UserPositionChangedMsgData struct {
	Uid           string `json:"uid"`
	MarketAddress string `json:"market_address"`
	OptionAddress string `json:"option_address"`
}

type UserNewNotificationMsgData struct {
	Uid string `json:"uid"`
}

type UserAssetChangedMsgData struct {
	Uid           string `json:"uid"`
	BaseTokenType uint8  `json:"base_token_type"`
}

type MarketNewTradesMsgData struct {
	MarketAddress string `json:"market_address"`
	OptionAddress string `json:"option_address"`
}

type MarketPriceUpdateMsgData struct {
	MarketAddress string `json:"market_address"`
}

type MarketInfoUpdateMsgData struct {
	MarketAddress string `json:"market_address"`
}
