package base

import (
	"market-service/internal/pkg/common"
	"time"

	"github.com/go-redis/redis/v8"
)

// ============= Redis Stream 操作封装 =============

// StreamAddMessage 添加消息到stream
func (r Infra) StreamAddMessage(ctx common.Ctx, streamKey string, data interface{}) (string, error) {
	result, err := r.redis.XAdd(ctx.Ctx, &redis.XAddArgs{
		Stream: streamKey,
		Values: data,
	}).Result()
	if err != nil {
		ctx.Log.Errorf("StreamAddMessage error [stream: %s][error: %v]", streamKey, err)
		return "", err
	}
	return result, nil
}

// StreamAddMessageWithID 添加消息到stream，指定消息ID
func (r Infra) StreamAddMessageWithID(ctx common.Ctx, streamKey string, messageID string, data interface{}) (string, error) {
	result, err := r.redis.XAdd(ctx.Ctx, &redis.XAddArgs{
		Stream: streamKey,
		ID:     messageID,
		Values: data,
	}).Result()
	if err != nil {
		ctx.Log.Errorf("StreamAddMessageWithID error [stream: %s][id: %s][error: %v]", streamKey, messageID, err)
		return "", err
	}
	return result, nil
}

// StreamCreateGroup 创建消费者组
func (r Infra) StreamCreateGroup(ctx common.Ctx, streamKey string, groupName string, startID string) error {
	err := r.redis.XGroupCreateMkStream(ctx.Ctx, streamKey, groupName, startID).Err()
	if err != nil {
		// 如果消费者组已存在，不算错误
		if err.Error() == "BUSYGROUP Consumer Group name already exists" {
			return nil
		}
		ctx.Log.Errorf("StreamCreateGroup error [stream: %s][group: %s][startID: %s][error: %v]", streamKey, groupName, startID, err)
		return err
	}
	return nil
}

// StreamReadGroup 从消费者组读取消息
func (r Infra) StreamReadGroup(ctx common.Ctx, streamKey string, groupName string, consumerName string, count int64) ([]redis.XStream, error) {
	result, err := r.redis.XReadGroup(ctx.Ctx, &redis.XReadGroupArgs{
		Group:    groupName,
		Consumer: consumerName,
		Streams:  []string{streamKey, ">"},
		Count:    count,
		Block:    0,
	}).Result()
	if err != nil {
		ctx.Log.Errorf("StreamReadGroup error [stream: %s][group: %s][consumer: %s][count: %d][error: %v]", streamKey, groupName, consumerName, count, err)
		return nil, err
	}

	if len(result) == 0 || len(result[0].Messages) == 0 {
		return []redis.XStream{}, nil
	}

	return result, nil
}

// StreamReadGroupBlocking 阻塞从消费者组读取消息
func (r Infra) StreamReadGroupBlocking(ctx common.Ctx, streamKey string, groupName string, consumerName string, count int64, blockDuration time.Duration) ([]redis.XStream, error) {
	result, err := r.redis.XReadGroup(ctx.Ctx, &redis.XReadGroupArgs{
		Group:    groupName,
		Consumer: consumerName,
		Streams:  []string{streamKey, ">"},
		Count:    count,
		Block:    blockDuration,
	}).Result()
	if err != nil {
		if err == redis.Nil {
			return []redis.XStream{}, nil
		}
		ctx.Log.Errorf("StreamReadGroupBlocking error [stream: %s][group: %s][consumer: %s][count: %d][block: %v][error: %v]", streamKey, groupName, consumerName, count, blockDuration, err)
		return nil, err
	}

	if len(result) == 0 || len(result[0].Messages) == 0 {
		return []redis.XStream{}, nil
	}

	return result, nil
}

// StreamReadGroupBlockingMultiple 阻塞从消费者组读取多个stream的消息
func (r Infra) StreamReadGroupBlockingMultiple(ctx common.Ctx, streamKeys []string, groupName string, consumerName string, count int64, blockDuration time.Duration) ([]redis.XStream, error) {
	if len(streamKeys) == 0 {
		return []redis.XStream{}, nil
	}

	// 构建streams参数: [stream1, stream2, ..., ">", ">", ...]
	streams := make([]string, 0, len(streamKeys)*2)
	streams = append(streams, streamKeys...)
	for range streamKeys {
		streams = append(streams, ">")
	}

	result, err := r.redis.XReadGroup(ctx.Ctx, &redis.XReadGroupArgs{
		Group:    groupName,
		Consumer: consumerName,
		Streams:  streams,
		Count:    count,
		Block:    blockDuration,
	}).Result()
	if err != nil {
		if err == redis.Nil {
			return []redis.XStream{}, nil
		}
		ctx.Log.Errorf("StreamReadGroupBlockingMultiple error [streams: %v][group: %s][consumer: %s][count: %d][block: %v][error: %v]", streamKeys, groupName, consumerName, count, blockDuration, err)
		return nil, err
	}

	if len(result) == 0 {
		return []redis.XStream{}, nil
	}

	if len(result) == 0 {
		return []redis.XStream{}, nil
	}

	return result, nil
}

// StreamReadPendingMessages 读取指定消费者的pending消息
func (r Infra) StreamReadPendingMessages(ctx common.Ctx, streamKey string, groupName string, consumerName string, count int64, blockDuration time.Duration) ([]redis.XStream, error) {
	result, err := r.redis.XReadGroup(ctx.Ctx, &redis.XReadGroupArgs{
		Group:    groupName,
		Consumer: consumerName,
		Streams:  []string{streamKey, "0"}, // 使用"0"读取pending消息
		Count:    count,
		Block:    blockDuration,
	}).Result()
	if err != nil {
		if err == redis.Nil {
			return []redis.XStream{}, nil
		}
		ctx.Log.Errorf("StreamReadPendingMessages error [stream: %s][group: %s][consumer: %s][count: %d][error: %v]", streamKey, groupName, consumerName, count, err)
		return nil, err
	}

	if len(result) == 0 || len(result[0].Messages) == 0 {
		return []redis.XStream{}, nil
	}

	return result, nil
}

// StreamReadPendingMessagesMultiple 读取多个stream的pending消息
func (r Infra) StreamReadPendingMessagesMultiple(ctx common.Ctx, streamKeys []string, groupName string, consumerName string, count int64, blockDuration time.Duration) ([]redis.XStream, error) {
	if len(streamKeys) == 0 {
		return []redis.XStream{}, nil
	}

	// 构建streams参数: [stream1, stream2, ..., "0", "0", ...]
	streams := make([]string, 0, len(streamKeys)*2)
	streams = append(streams, streamKeys...)
	for range streamKeys {
		streams = append(streams, "0")
	}

	result, err := r.redis.XReadGroup(ctx.Ctx, &redis.XReadGroupArgs{
		Group:    groupName,
		Consumer: consumerName,
		Streams:  streams,
		Count:    count,
		Block:    blockDuration,
	}).Result()
	if err != nil {
		if err == redis.Nil {
			return []redis.XStream{}, nil
		}
		ctx.Log.Errorf("StreamReadPendingMessagesMultiple error [streams: %v][group: %s][consumer: %s][count: %d][error: %v]", streamKeys, groupName, consumerName, count, err)
		return nil, err
	}

	if len(result) == 0 {
		return []redis.XStream{}, nil
	}

	return result, nil
}

// StreamAckMessage 确认消息处理
func (r Infra) StreamAckMessage(ctx common.Ctx, streamKey string, groupName string, messageIDs ...string) (int64, error) {
	result, err := r.redis.XAck(ctx.Ctx, streamKey, groupName, messageIDs...).Result()
	if err != nil {
		ctx.Log.Errorf("StreamAckMessage error [stream: %s][group: %s][messageIDs: %v][error: %v]", streamKey, groupName, messageIDs, err)
		return 0, err
	}
	return result, nil
}

// StreamPendingMessages 查看未确认的消息
func (r Infra) StreamPendingMessages(ctx common.Ctx, streamKey string, groupName string) (*redis.XPending, error) {
	result, err := r.redis.XPending(ctx.Ctx, streamKey, groupName).Result()
	if err != nil {
		ctx.Log.Errorf("StreamPendingMessages error [stream: %s][group: %s][error: %v]", streamKey, groupName, err)
		return nil, err
	}
	return result, nil
}

// StreamPendingMessagesExt 查看未确认的消息详情
func (r Infra) StreamPendingMessagesExt(ctx common.Ctx, streamKey string, groupName string, start, end string, count int64) ([]redis.XPendingExt, error) {
	result, err := r.redis.XPendingExt(ctx.Ctx, &redis.XPendingExtArgs{
		Stream: streamKey,
		Group:  groupName,
		Start:  start,
		End:    end,
		Count:  count,
	}).Result()
	if err != nil {
		ctx.Log.Errorf("StreamPendingMessagesExt error [stream: %s][group: %s][start: %s][end: %s][count: %d][error: %v]", streamKey, groupName, start, end, count, err)
		return nil, err
	}
	return result, nil
}

// StreamDeleteMessage 删除消息
func (r Infra) StreamDeleteMessage(ctx common.Ctx, streamKey string, messageIDs ...string) (int64, error) {
	result, err := r.redis.XDel(ctx.Ctx, streamKey, messageIDs...).Result()
	if err != nil {
		ctx.Log.Errorf("StreamDeleteMessage error [stream: %s][messageIDs: %v][error: %v]", streamKey, messageIDs, err)
		return 0, err
	}
	return result, nil
}

// StreamLength 获取stream长度
func (r Infra) StreamLength(ctx common.Ctx, streamKey string) (int64, error) {
	result, err := r.redis.XLen(ctx.Ctx, streamKey).Result()
	if err != nil {
		ctx.Log.Errorf("StreamLength error [stream: %s][error: %v]", streamKey, err)
		return 0, err
	}
	return result, nil
}

// StreamRangeMessages 按范围读取消息
func (r Infra) StreamRangeMessages(ctx common.Ctx, streamKey string, start, end string, count int64) ([]redis.XMessage, error) {
	result, err := r.redis.XRange(ctx.Ctx, streamKey, start, end).Result()
	if err != nil {
		ctx.Log.Errorf("StreamRangeMessages error [stream: %s][start: %s][end: %s][error: %v]", streamKey, start, end, err)
		return nil, err
	}

	// 如果指定了count，限制返回数量
	if count > 0 && int64(len(result)) > count {
		result = result[:count]
	}

	return result, nil
}

// StreamTrim 修剪stream，保留指定数量的消息
func (r Infra) StreamTrim(ctx common.Ctx, streamKey string, maxLen int64) (int64, error) {
	result, err := r.redis.XTrimMaxLen(ctx.Ctx, streamKey, maxLen).Result()
	if err != nil {
		ctx.Log.Errorf("StreamTrim error [stream: %s][maxLen: %d][error: %v]", streamKey, maxLen, err)
		return 0, err
	}
	return result, nil
}

// StreamTrimApprox 修剪stream，大约保留指定数量的消息（性能更好）
func (r Infra) StreamTrimApprox(ctx common.Ctx, streamKey string, maxLen int64) (int64, error) {
	result, err := r.redis.XTrimMaxLenApprox(ctx.Ctx, streamKey, maxLen, 0).Result()
	if err != nil {
		ctx.Log.Errorf("StreamTrimApprox error [stream: %s][maxLen: %d][error: %v]", streamKey, maxLen, err)
		return 0, err
	}
	return result, nil
}

// StreamInfo 获取stream信息
func (r Infra) StreamInfo(ctx common.Ctx, streamKey string) (*redis.XInfoStream, error) {
	result, err := r.redis.XInfoStream(ctx.Ctx, streamKey).Result()
	if err != nil {
		ctx.Log.Errorf("StreamInfo error [stream: %s][error: %v]", streamKey, err)
		return nil, err
	}
	return result, nil
}

// StreamGroupInfo 获取消费者组信息
func (r Infra) StreamGroupInfo(ctx common.Ctx, streamKey string) ([]redis.XInfoGroup, error) {
	result, err := r.redis.XInfoGroups(ctx.Ctx, streamKey).Result()
	if err != nil {
		ctx.Log.Errorf("StreamGroupInfo error [stream: %s][error: %v]", streamKey, err)
		return nil, err
	}
	return result, nil
}

// StreamConsumerInfo 获取消费者信息
func (r Infra) StreamConsumerInfo(ctx common.Ctx, streamKey string, groupName string) ([]redis.XInfoConsumer, error) {
	result, err := r.redis.XInfoConsumers(ctx.Ctx, streamKey, groupName).Result()
	if err != nil {
		ctx.Log.Errorf("StreamConsumerInfo error [stream: %s][group: %s][error: %v]", streamKey, groupName, err)
		return nil, err
	}
	return result, nil
}
