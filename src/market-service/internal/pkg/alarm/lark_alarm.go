package alarm

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"market-service/internal/conf"
	"net/http"
	"sync"

	"github.com/go-kratos/kratos/v2/log"
)

const webhookURL = "https://open.larksuite.com/open-apis/bot/v2/hook/%s"

var (
	Lark *LarkAlarm
	once sync.Once
)

func InitLarkAlarm(c *conf.Custom, logger log.Logger) {
	once.Do(func() {
		Lark = NewLarkAlarm(c, logger)
	})
}

type LarkAlarm struct {
	webhookURL string
	log        *log.Helper
	enabled    bool
}

func NewLarkAlarm(c *conf.Custom, logger log.Logger) *LarkAlarm {
	alarm := &LarkAlarm{
		webhookURL: fmt.Sprintf(webhookURL, c.LarkWebhookKey),
		log:        log.NewHelper(logger),
		enabled:    c.GetLarkEnabled() && c.GetLarkWebhookKey() != "",
	}
	if alarm.enabled {
		err := alarm.Send("init test msg")
		if err != nil {
			alarm.log.Errorf("init lark alarm error: %v", err)
		} else {
			alarm.log.Infof("init lark alarm success")
		}
	}
	return alarm
}

func (l *LarkAlarm) Send(message string) error {
	if !l.enabled {
		return nil
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
		return err
	}

	// 发送请求
	resp, err := http.Post(
		l.webhookURL,
		"application/json",
		bytes.NewBuffer(jsonData),
	)
	if err != nil {
		l.log.Errorf("send lark alarm error: %v", err)
		return err
	}
	if resp.StatusCode != http.StatusOK {
		l.log.Errorf("send lark alarm error: %v", resp.Status)
		return errors.New(resp.Status)
	}
	defer resp.Body.Close()
	return nil
}
