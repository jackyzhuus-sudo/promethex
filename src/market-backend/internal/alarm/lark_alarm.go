package alarm

import (
	"bytes"
	"encoding/json"
	"fmt"
	"market-backend/internal/conf"
	"net/http"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/google/wire"
)

const webhookURL = "https://open.larksuite.com/open-apis/bot/v2/hook/%s"

var ProviderSet = wire.NewSet(NewLarkAlarm)

type LarkAlarm struct {
	webhookURL string
	log        *log.Helper
	enabled    bool
}

func NewLarkAlarm(c *conf.Custom, logger log.Logger) *LarkAlarm {
	return &LarkAlarm{
		webhookURL: fmt.Sprintf(webhookURL, c.LarkWebhookKey),
		log:        log.NewHelper(logger),
		enabled:    c.GetLarkEnabled() && c.GetLarkWebhookKey() != "",
	}
}

func (l *LarkAlarm) SendLarkAlarm(message string) {
	if !l.enabled {
		return
	}
	// 构造消息
	messageMap := map[string]interface{}{
		"msg_type": "text",
		"content": map[string]string{
			"text": message,
		},
	}

	// 转换为JSON
	jsonData, err := json.Marshal(messageMap)
	if err != nil {
		l.log.Errorf("json.Marshal error: %v", err)
		return
	}

	// 发送请求
	resp, err := http.Post(
		l.webhookURL,
		"application/json",
		bytes.NewBuffer(jsonData),
	)
	if err != nil {
		l.log.Errorf("send lark alarm error: %v", err)
		return
	}
	defer resp.Body.Close()
}
