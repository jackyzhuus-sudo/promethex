package middleware

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	bayespb "market-proto/proto/market-backend/v1"

	marketcenterpb "market-proto/proto/market-service/marketcenter/v1"
	usercenterpb "market-proto/proto/market-service/usercenter/v1"

	"github.com/go-kratos/kratos/v2/errors"
	httpTransport "github.com/go-kratos/kratos/v2/transport/http"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// CustomCodec 自定义的编解码器，同时处理请求解码和响应编码
type CustomCodec struct {
	// 默认的请求解码器
	defaultDecoder httpTransport.DecodeRequestFunc
	// 支持的文件类型
	supportedFileTypes map[string]bool
}

// NewCustomCodec 创建一个新的自定义编解码器
func NewCustomCodec() *CustomCodec {
	// 初始化支持的文件类型
	supportedTypes := map[string]bool{
		"image/jpeg":       true,
		"image/jpg":        true,
		"image/png":        true,
		"image/gif":        true,
		"application/pdf":  true,
		"application/json": true,
		"text/plain":       true,
	}

	return &CustomCodec{
		defaultDecoder:     httpTransport.DefaultRequestDecoder,
		supportedFileTypes: supportedTypes,
	}
}

// RequestDecoder 用于注册到Kratos的请求解码器
func (c *CustomCodec) RequestDecoder() httpTransport.DecodeRequestFunc {
	return func(r *http.Request, v interface{}) error {
		return c.DecodeRequest(r, v)
	}
}

// ResponseEncoder 用于注册到Kratos的响应编码器
func (c *CustomCodec) ResponseEncoder() httpTransport.EncodeResponseFunc {
	return func(w http.ResponseWriter, r *http.Request, v interface{}) error {
		// 确保在处理任何响应之前设置状态码为 200
		w.WriteHeader(http.StatusOK)
		return c.EncodeResponse(w, r, v)
	}
}

// 请求解码实现
func (c *CustomCodec) DecodeRequest(r *http.Request, v interface{}) error {
	// 判断是否是上传文件接口
	if c.isUploadFileEndpoint(r) && strings.Contains(r.Header.Get("Content-Type"), "multipart/form-data") {
		return c.decodeMultipartForm(r, v)
	}

	// 其他接口使用Kratos默认的解码器
	return c.defaultDecoder(r, v)
}

// 响应编码实现
func (c *CustomCodec) EncodeResponse(w http.ResponseWriter, r *http.Request, v interface{}) error {
	// 检查是否是下载文件响应
	if c.isDownloadFileEndpoint(r) {
		return c.encodeFileResponse(w, r, v)
	}

	// 其他响应使用统一的JSON格式
	return c.encodeJSONResponse(w, r, v)
}

// 检查是否是上传文件的端点
func (c *CustomCodec) isUploadFileEndpoint(r *http.Request) bool {
	return strings.Contains(r.URL.Path, "/bayes/base/upload-file")
}

// 检查是否是下载文件请求
func (c *CustomCodec) isDownloadFileEndpoint(r *http.Request) bool {
	return strings.Contains(r.URL.Path, "/bayes/base/download-file")
}

// 解码multipart/form-data格式的请求
func (c *CustomCodec) decodeMultipartForm(r *http.Request, v interface{}) error {
	// 限制解析的文件大小，这里设置为32MB
	const maxMemory = 32 << 20
	if err := r.ParseMultipartForm(maxMemory); err != nil {
		return errors.BadRequest("DECODE_FAILED", fmt.Sprintf("failed to parse multipart form: %v", err))
	}

	// 获取文件
	file, fileHeader, err := r.FormFile("file")
	if err != nil {
		return errors.BadRequest("DECODE_FAILED", fmt.Sprintf("failed to get file from form: %v", err))
	}
	defer file.Close()

	// 检查内容类型是否支持
	contentType := fileHeader.Header.Get("Content-Type")
	if !c.supportedFileTypes[contentType] {
		return errors.BadRequest("UNSUPPORTED_FILE_TYPE", fmt.Sprintf("unsupported file type: %s", contentType))
	}

	// 读取文件内容
	fileBytes, err := io.ReadAll(file)
	if err != nil {
		return errors.BadRequest("DECODE_FAILED", fmt.Sprintf("failed to read file: %v", err))
	}

	// 处理proto消息
	if req, ok := v.(*bayespb.UploadFileRequest); ok {
		req.File = fileBytes
		req.FileName = fileHeader.Filename
		req.ContentType = contentType
		return nil
	}

	return errors.BadRequest("DECODE_FAILED", "decode to upload request failed")
}

// 处理文件下载响应
func (c *CustomCodec) encodeFileResponse(w http.ResponseWriter, r *http.Request, v interface{}) error {
	if resp, ok := v.(*bayespb.DownloadFileReply); ok && resp.FileData != nil {
		// 设置适当的Content-Type和Content-Disposition
		// 实际应用中可能需要根据文件类型设置不同的Content-Type
		w.Header().Set("Content-Type", resp.ContentType)
		w.Header().Set("Cache-Control", "public, max-age=604800")
		if resp.ContentType != "image/jpeg" && resp.ContentType != "image/png" && resp.ContentType != "image/gif" && resp.ContentType != "image/jpg" {
			w.Header().Set("Content-Disposition", "attachment")
		}

		_, err := w.Write(resp.FileData)
		return err
	}

	return c.encodeJSONResponse(w, r, v)
}

// StandardResponse 标准响应格式
type StandardResponse struct {
	Code   int         `json:"code"`
	Reason string      `json:"reason"`
	Msg    string      `json:"msg"`
	Data   interface{} `json:"data"`
}

var (
	MarshalOptions = protojson.MarshalOptions{
		EmitUnpopulated: true,
		UseProtoNames:   false,
		UseEnumNumbers:  true,
	}
)

func bizCodeToHttpCode(bizCode int) int {
	// bizCode 格式：aabcdd
	// aa: 服务编号
	// b: 服务内的领域编号
	// c: 错误类型 1-客户端错误 2-服务端错误 3-第三方错误
	// dd: 具体错误

	switch bizCode {
	case int(marketcenterpb.ErrorCode_NOT_FOUND), int(marketcenterpb.ErrorCode_MARKET_NOT_FOUND),
		int(usercenterpb.ErrorCode_USER_NOT_FOUND), int(usercenterpb.ErrorCode_POST_NOT_FOUND), int(usercenterpb.ErrorCode_COMMENT_NOT_FOUND):
		return http.StatusNotFound
	default:
	}

	// 提取错误类型（第三位数字）
	errorType := (bizCode / 10) % 10

	// 根据错误类型返回对应的 HTTP 状态码
	switch errorType {
	case 1: // 客户端错误
		return http.StatusBadRequest
	case 2: // 服务端错误
		return http.StatusInternalServerError
	case 3: // 第三方错误
		return http.StatusServiceUnavailable
	default:
		// 未知错误类型，默认返回 500
		return http.StatusInternalServerError
	}
}

// ErrorEncoder 自定义的错误编码器
func ErrorEncoder() httpTransport.EncodeErrorFunc {
	return func(w http.ResponseWriter, r *http.Request, err error) {

		// 系统错误关键词列表
		systemErrorKeywords := []string{
			"dial tcp",
			"timeout",
			"context canceled",
			"connection refused",
			"i/o timeout",
			"connection reset",
			"network unreachable",
			"service unavailable",
			"eof",
		}

		// 检查是否为系统错误
		isSystemError := func(errMsg string) bool {
			lowerMsg := strings.ToLower(errMsg)
			for _, keyword := range systemErrorKeywords {
				if strings.Contains(lowerMsg, keyword) {
					return true
				}
			}
			return false
		}

		// 提取错误信息
		code := 500
		reason := "INTERNAL_ERROR"
		msg := "internal error"
		httpCode := 500

		if se := errors.FromError(err); se != nil {
			code = int(se.Code)
			if metadata := se.GetMetadata(); metadata != nil {
				if bizCodeStr, ok := metadata["bizCode"]; ok {
					code, _ = strconv.Atoi(bizCodeStr)
				}
			}
			reason = se.Reason
			msg = se.Message

			httpCode = bizCodeToHttpCode(code)

			// 检查是否为系统错误，如果是则使用统一的内部错误信息
			if isSystemError(se.Message) {
				code = 500
				reason = "INTERNAL_ERROR"
				msg = "internal error"
				httpCode = 500
			}

		} else {
			// 对于非 Kratos 错误，也检查是否包含系统错误关键词
			if isSystemError(err.Error()) {
				code = 500
				reason = "INTERNAL_ERROR"
				msg = "internal error"
			} else {
				// 如果不是系统错误，可能是业务错误，保留原始错误信息
				msg = err.Error()
				// 对于未知业务错误，默认使用 400
				code = 400
			}
		}

		// 设置 HTTP 状态码
		w.WriteHeader(httpCode)
		// 设置 JSON Content-Type
		w.Header().Set("Content-Type", "application/json")

		json.NewEncoder(w).Encode(StandardResponse{
			Code:   code,
			Reason: reason,
			Msg:    msg,
			Data:   nil,
		})

	}
}

// 编码标准JSON响应
func (c *CustomCodec) encodeJSONResponse(w http.ResponseWriter, r *http.Request, v interface{}) error {
	// 默认是成功响应
	code := 200
	bizCode := 0
	reason := ""
	msg := "success"
	var marshalData []byte

	// 如果是错误，提取错误信息
	if err, ok := v.(error); ok {
		// 首先尝试解析为 Kratos 错误
		if se := errors.FromError(err); se != nil {
			bizCode = int(se.Code)
			reason = se.Reason
			msg = se.Message

			// 从 metadata 中获取 bizCode
			if bizCodeStr, ok := se.Metadata["bizCode"]; ok {
				if parsedBizCode, err := strconv.Atoi(bizCodeStr); err == nil {
					bizCode = parsedBizCode
				}
			}

			// 根据 bizCode 确定 HTTP 状态码
			code = bizCodeToHttpCode(bizCode)
		} else {
			code = 500
			bizCode = 50001
			reason = "INTERNAL_ERROR"
			msg = err.Error()
		}

		// 设置 HTTP 状态码
		w.WriteHeader(code)
	} else {
		marshalData, err = func() ([]byte, error) {
			switch m := v.(type) {
			case json.Marshaler:
				return m.MarshalJSON()
			case proto.Message:
				return MarshalOptions.Marshal(m)
			default:
				return json.Marshal(m)
			}
		}()
		if err != nil {
			return err
		}
		// 成功响应设置 200 状态码
		w.WriteHeader(http.StatusOK)
	}

	stdResp := &StandardResponse{
		Code:   bizCode,
		Reason: reason,
		Msg:    msg,
		Data:   json.RawMessage(marshalData),
	}

	// 设置 JSON Content-Type
	w.Header().Set("Content-Type", "application/json")
	return json.NewEncoder(w).Encode(stdResp)
}
