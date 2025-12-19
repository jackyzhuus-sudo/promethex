# SSE 实时消息接口文档

## 概述

Server-Sent Events (SSE) 提供实时消息推送，支持用户消息、市场数据等实时更新。

## 接口列表

### 1. 建立 SSE 连接
- **URL**: `POST /bayes/sse/connect`
- **认证**: 可选（传入 Authorization 头部可接收用户专属消息）

### 2. 订阅市场频道
- **URL**: `POST /bayes/sse/subscribe` 
- **认证**: 可选（未登录用户需提供 connection_id）

### 3. 取消订阅市场频道
- **URL**: `POST /bayes/sse/unsubscribe`
- **认证**: 可选（未登录用户需提供 connection_id）

---

## 1. 建立 SSE 连接

### 请求
```http
POST /bayes/sse/connect
Accept: text/event-stream
Cache-Control: no-cache
Authorization: Bearer <token>  # 可选
```

### 响应
标准 SSE 格式，每个消息都遵循统一结构：
```
event: <事件名>
id: <消息ID>
retry: 3000
data: <JSON数据>

```

---

## 2. 订阅市场频道

### 请求
```http
POST /bayes/sse/subscribe
Content-Type: application/json
Authorization: Bearer <token>  # 可选
```

### 请求体
```json
{
  "connection_id": "连接ID",  // 可选，未登录用户必填，从 base:connected 事件获取
  "topics": ["0x1234567890abcdef...", "0xfedcba0987654321..."]  // 必填，市场合约地址列表
}
```

### 响应
```json
{}  // 空对象表示成功
```

---

## 3. 取消订阅市场频道

### 请求
```http
POST /bayes/sse/unsubscribe  
Content-Type: application/json
Authorization: Bearer <token>  # 可选
```

### 请求体
```json
{
  "connection_id": "连接ID",  // 可选，未登录用户必填，从 base:connected 事件获取
  "topics": ["0x1234567890abcdef...", "0xfedcba0987654321..."]  // 必填，市场合约地址列表
}
```

### 响应
```json
{}  // 空对象表示成功
```

---

## 消息格式

**所有 SSE 消息都采用统一的标准格式，只有 `event` 和 `data` 字段内容不同：**

```
event: <事件类型>
id: <唯一消息ID>
retry: 3000
data: <JSON格式的事件数据，某些事件可能为空>

```

### 基础事件

#### `base:connected` - 连接建立成功
```
event: base:connected
id: f09fd7b9-72e2-4322-8af8-15c56d877359
retry: 3000
data: {"connection_id": "f09fd7b9-72e2-4322-8af8-15c56d877359"}

```
**说明**: `connection_id` 用于未登录用户在订阅/取消订阅时标识连接身份

#### `base:ping` - 心跳消息（每10秒）
```
event: base:ping
id: a1b2c3d4-5678-90ef-abcd-1234567890ab
retry: 3000

```
**说明**: 心跳消息没有 data 字段，用于保持连接活跃

### 用户事件（必须登录才会推送）

#### `user:position_changed` - 用户持仓变化
```
event: user:position_changed
id: xxx
retry: 3000
data: {
  "uid": "user123",
  "market_address": "0x1234567890abcdef...",
  "option_address": "0xfedcba0987654321..."
}

```

#### `user:asset_changed` - 用户资产变化
```
event: user:asset_changed
id: xxx
retry: 3000
data: {
  "uid": "user123",
  "base_token_type": 1
}

```

#### `user:new_notification` - 新通知
```
event: user:new_notification
id: xxx
retry: 3000
data: {
  "uid": "user123"
}

```

### 市场事件（必须先订阅对应市场才会推送）

#### `market:new_trades` - 新交易数据
```
event: market:new_trades
id: xxx
retry: 3000
data: {
  "market_address": "0x1234567890abcdef...",
  "option_address": "0xfedcba0987654321..."
}

```

#### `market:price_change` - 市场价格变化
```
event: market:price_change
id: xxx
retry: 3000
data: {
  "market_address": "0x1234567890abcdef..."
}

```

#### `market:info_update` - 市场信息更新
```
event: market:info_update
id: xxx
retry: 3000
data: {
  "market_address": "0x1234567890abcdef..."
}

```

---

## 推送规则

1. **用户事件**: 必须使用有效 token 登录，只推送给对应用户
2. **市场事件**: 必须先通过订阅接口订阅对应的市场地址，才会收到该市场的事件推送
3. **基础事件**: 所有连接都会收到，无需额外条件

---

## 注意事项

1. **连接超时**: SSE 连接会在 10 分钟无活动后自动断开
2. **心跳机制**: 服务器每 10 秒发送一次 `base:ping` 事件保持连接
3. **认证可选**: 
   - 未登录用户：只能接收基础事件和已订阅的市场事件，订阅时需提供 `connection_id`
   - 登录用户：可接收所有类型事件，订阅时无需提供 `connection_id`
4. **重连处理**: 建议客户端实现自动重连机制
5. **Topics 格式**: 直接使用市场合约地址，如 `0x1234567890abcdef...` 