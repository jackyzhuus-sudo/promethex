package data

import (
	"fmt"
	"market-backend/internal/conf"
	"market-backend/internal/pkg/util"
	"strings"

	"github.com/openai/openai-go"
	"github.com/openai/openai-go/option"
)

type OpenAI struct {
	Client *openai.Client
	cfg    *conf.Data_OpenAI
}

func newOpenAiWithBase(cfg *conf.Data_OpenAI) *OpenAI {
	opts := []option.RequestOption{option.WithAPIKey(cfg.ApiKey)}
	if strings.TrimSpace(cfg.BaseUrl) != "" {
		opts = append(opts, option.WithBaseURL(cfg.BaseUrl))
	}
	client := openai.NewClient(opts...)

	return &OpenAI{
		Client: &client,
		cfg:    cfg,
	}
}

// TranslateByOpenAI 调用OpenAI进行翻译
func (o *OpenAI) TranslateByOpenAI(ctx util.Ctx, text, sourceLang, targetLang string) (string, int32, error) {
	// 构建高质量翻译提示词
	prompt := o.buildTranslationPrompt(text, sourceLang, targetLang)
	ctx.Log.Infof("translate prompt: %s", prompt)
	// 调用 Chat Completions API
	chatCompletion, err := o.Client.Chat.Completions.New(ctx.Ctx, openai.ChatCompletionNewParams{
		Messages: []openai.ChatCompletionMessageParamUnion{
			openai.UserMessage(prompt),
		},
		Model: o.cfg.Model,
		// 针对翻译任务的优化参数
		Temperature:      openai.Float(float64(o.cfg.Temperature)),      // 降低随机性，提高翻译一致性
		MaxTokens:        openai.Int(int64(o.cfg.MaxTokens)),            // 限制最大token数
		TopP:             openai.Float(float64(o.cfg.TopP)),             // 稍微限制词汇选择
		FrequencyPenalty: openai.Float(float64(o.cfg.FrequencyPenalty)), // 不使用频率惩罚
		PresencePenalty:  openai.Float(float64(o.cfg.PresencePenalty)),  // 不使用存在惩罚
	})
	if err != nil {
		return "", 0, err
	}

	if len(chatCompletion.Choices) == 0 {
		return "", 0, fmt.Errorf("OpenAI return empty response")
	}

	content := strings.TrimSpace(chatCompletion.Choices[0].Message.Content)
	tokensUsed := int32(0)
	if chatCompletion.Usage.TotalTokens > 0 {
		tokensUsed = int32(chatCompletion.Usage.TotalTokens)
	}

	return content, tokensUsed, nil
}

// buildTranslationPrompt 构建高质量翻译提示词
func (o *OpenAI) buildTranslationPrompt(text, sourceLang, targetLang string) string {
	var taskDescription string
	if sourceLang != "" {
		sourceName := getLanguageName(sourceLang)
		targetName := getLanguageName(targetLang)
		taskDescription = fmt.Sprintf("现在请将以下%s文本翻译成(%s)：", sourceName, targetName)
	} else {
		targetName := getLanguageName(targetLang)
		taskDescription = fmt.Sprintf("现在请将以下文本翻译成(%s)：", targetName)
	}

	return o.cfg.PromptTemplate + "\n\n" + taskDescription + "\n\n" + text
}

// 支持的语言映射表
var supportedLanguages = map[string]string{
	// 中文
	"zh":    "中文",
	"zh-cn": "简体中文",
	"zh-tw": "繁体中文",
	"zh-hk": "繁体中文(香港)",
	"zh-sg": "简体中文(新加坡)",

	// 英文
	"en":    "英文",
	"en-us": "英文(美国)",
	"en-gb": "英文(英国)",
	"en-au": "英文(澳大利亚)",
	"en-ca": "英文(加拿大)",
}

// getLanguageName 获取语言名称
func getLanguageName(langCode string) string {
	if name, exists := supportedLanguages[strings.ToLower(langCode)]; exists {
		return name
	}
	return langCode
}

// GetSupportedLanguages 获取支持的语言列表
func (o *OpenAI) GetSupportedLanguages() map[string]string {
	return supportedLanguages
}
