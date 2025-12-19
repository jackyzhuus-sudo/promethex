package bayes_http

import (
	"context"
	"crypto/md5"
	"fmt"
	"market-backend/internal/base_err"
	"market-backend/internal/pkg/util"
	bayespb "market-proto/proto/market-backend/v1"
	"strings"
	"time"

	"github.com/go-kratos/kratos/v2/errors"
)

// 内部缓存结构
type translateCacheData struct {
	TranslatedText string `json:"translated_text"`
	SourceLang     string `json:"source_lang"`
	TargetLang     string `json:"target_lang"`
	TokensUsed     int32  `json:"tokens_used"`
}

// 缓存相关常量
const (
	translateCachePrefix = "translate:"
	translateCacheTTL    = 7 * 24 * time.Hour
)

func (s *BayesHttpService) Translate(ctx context.Context, req *bayespb.TranslateRequest) (*bayespb.TranslateReply, error) {
	c := util.NewBaseCtx(ctx, s.logger)
	// 参数验证
	if err := s.validateTranslateRequest(req); err != nil {
		return nil, errors.New(int(bayespb.ErrorCode_PARAM), "PARAMS ERROR", err.Error())
	}

	// 生成缓存键
	cacheKey := generateTranslateCacheKey(req)

	cacheData := &translateCacheData{}
	err := s.data.GetJSONFromCache(ctx, cacheKey, cacheData)
	if err != nil {
		c.Log.Errorf("get json from cache error: %v", err)
		return nil, base_err.ErrRedis
	}

	if cacheData.TranslatedText != "" {
		c.Log.Infof("翻译缓存命中: %s -> %s", req.Text[:min(50, len(req.Text))], cacheData.TranslatedText[:min(50, len(cacheData.TranslatedText))])
		return &bayespb.TranslateReply{
			TranslatedText: cacheData.TranslatedText,
			SourceLang:     cacheData.SourceLang,
			TargetLang:     cacheData.TargetLang,
			Cached:         true,
			TokensUsed:     uint32(cacheData.TokensUsed),
		}, nil
	}

	// 缓存未命中，调用OpenAI翻译
	translatedText, tokensUsed, err := s.data.OpenAIClient.TranslateByOpenAI(c, req.Text, req.SourceLang, req.TargetLang)
	if err != nil {
		return nil, errors.New(int(bayespb.ErrorCode_OPENAI), "OPENAI ERROR", err.Error())
	}
	c.Log.Infof("translatedText: %s", translatedText)

	translatedText = strings.TrimSpace(translatedText)
	if translatedText == "" {
		return nil, errors.New(int(bayespb.ErrorCode_OPENAI), "OPENAI ERROR", "translated text is empty")
	}

	// 检测或使用提供的源语言
	sourceLang := req.SourceLang
	if sourceLang == "" {
		sourceLang = "auto" // 自动检测
	}

	// 构造响应
	resp := &bayespb.TranslateReply{
		TranslatedText: translatedText,
		SourceLang:     sourceLang,
		TargetLang:     req.TargetLang,
		Cached:         false,
		TokensUsed:     uint32(tokensUsed),
	}

	// 保存到缓存
	cacheData = &translateCacheData{
		TranslatedText: translatedText,
		SourceLang:     sourceLang,
		TargetLang:     req.TargetLang,
		TokensUsed:     tokensUsed,
	}
	if err := s.data.SetJSONToCache(ctx, cacheKey, cacheData, translateCacheTTL); err != nil {
		c.Log.Errorf("save translate cache error: %v", err)
	}

	c.Log.Infof("translate done: %s -> %s (tokens: %d)",
		req.Text[:min(50, len(req.Text))],
		translatedText[:min(50, len(translatedText))],
		tokensUsed)

	return resp, nil
}

// GetSupportedLanguages 获取支持的语言列表
func (s *BayesHttpService) GetSupportedLanguages() map[string]string {
	return s.data.OpenAIClient.GetSupportedLanguages()
}

// validateTranslateRequest 验证翻译请求
func (s *BayesHttpService) validateTranslateRequest(req *bayespb.TranslateRequest) error {
	text := strings.TrimSpace(req.Text)
	if text == "" {
		return fmt.Errorf("text is empty")
	}
	if len(text) > 5000 { // 限制文本长度
		return fmt.Errorf("text length is too long")
	}

	// 验证目标语言
	targetLang := strings.TrimSpace(req.TargetLang)
	if targetLang == "" {
		return fmt.Errorf("target language is empty")
	}

	// 检查语言代码是否支持
	supportedLangs := s.data.OpenAIClient.GetSupportedLanguages()
	targetLangLower := strings.ToLower(targetLang)
	if _, exists := supportedLangs[targetLangLower]; !exists {
		return fmt.Errorf("unsupported target language: %s", req.TargetLang)
	}

	// 验证源语言（如果提供）
	if req.SourceLang != "" {
		sourceLangLower := strings.ToLower(strings.TrimSpace(req.SourceLang))
		if _, exists := supportedLangs[sourceLangLower]; !exists {
			return fmt.Errorf("unsupported source language: %s", req.SourceLang)
		}
	}

	return nil
}

// generateTranslateCacheKey 生成翻译缓存键
func generateTranslateCacheKey(req *bayespb.TranslateRequest) string {
	// 标准化输入
	text := strings.TrimSpace(req.Text)
	sourceLang := strings.ToLower(strings.TrimSpace(req.SourceLang))
	targetLang := strings.ToLower(strings.TrimSpace(req.TargetLang))

	// 生成缓存键：translate:md5(text+source+target)
	input := fmt.Sprintf("%s|%s|%s", text, sourceLang, targetLang)
	hash := fmt.Sprintf("%x", md5.Sum([]byte(input)))
	return translateCachePrefix + hash
}
