package util

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/bitly/go-simplejson"
)

// HttpGet HttpGet请求
func HttpGet(baseUrl string, params map[string]string, headers map[string]string) ([]byte, error) {
	// 解析URL
	parsedUrl, err := url.Parse(baseUrl)
	if err != nil {
		return nil, err
	}

	// 添加查询参数
	queryParams := parsedUrl.Query()
	for key, value := range params {
		queryParams.Set(key, value)
	}
	parsedUrl.RawQuery = queryParams.Encode()

	// 创建GET请求
	req, err := http.NewRequest("GET", parsedUrl.String(), nil)
	if err != nil {
		return nil, err
	}

	// 设置请求头
	for key, value := range headers {
		req.Header.Set(key, value)
	}

	// 发送请求
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		fmt.Printf("HTTP request failed with status code: %d, body%s \n", resp.StatusCode, resp.Body)
		return nil, errors.New("HTTP request failed")
	}

	// 读取响应体
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	return body, nil
}

// HttpPost
func HttpPost(baseUrl string, data map[string]interface{}, headers map[string]string) (*simplejson.Json, error) {
	// 编码请求体
	var requestBody io.Reader
	if data != nil {
		jsonBody, err := json.Marshal(data)
		if err != nil {
			return nil, err
		}
		requestBody = bytes.NewBuffer(jsonBody)
	}

	// 创建POST请求
	req, err := http.NewRequest("POST", baseUrl, requestBody)
	if err != nil {
		return nil, err
	}

	// 设置请求头
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	req.Header.Set("Content-Type", "application/json")

	// 发送请求
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		fmt.Printf("HTTP request failed with status code: %d, body: %s \n", resp.StatusCode, resp.Body)
		return nil, fmt.Errorf("HTTP request failed. HTTP status code: [%d]", resp.StatusCode)
	}

	// 读取响应体
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	// 解析为simplejson
	js, err := simplejson.NewJson(respBody)
	if err != nil {
		return nil, err
	}

	return js, nil
}
