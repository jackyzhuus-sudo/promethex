package util

import (
	"crypto/rand"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/google/uuid"
)

var (
	// 互斥锁保证并发安全
	mutex sync.Mutex
	// 序列号，从0开始递增
	sequence int64 = 0
	// 上次生成ID的时间戳
	lastTimestamp int64 = -1
	// 缓存的机器ID
	machineID int64
)

func init() {
	// 初始化时获取机器ID
	machineID = getMachineIDFromIP()
}

// GenerateUID 生成固定格式的用户ID
func GenerateUID() string {
	mutex.Lock()
	defer mutex.Unlock()

	// 定义雪花算法参数
	var (
		datacenterID int64 = 1                                                       // 固定数据中心ID为1
		twepoch      int64 = time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli() // 使用业务开始时间作为起始时间戳
	)

	// 获取当前时间戳
	timestamp := time.Now().UnixMilli()

	// 如果当前时间小于上次时间,说明系统时钟回退,抛出异常
	if timestamp < lastTimestamp {
		panic(fmt.Sprintf("Clock moved backwards. Refusing to generate id for %d milliseconds", lastTimestamp-timestamp))
	}

	// 如果是同一时间生成的,则进行序列号递增
	if lastTimestamp == timestamp {
		sequence = (sequence + 1) & 4095 // 序列号最大值为4095
		// 序列号超出最大值,阻塞到下一个毫秒
		if sequence == 0 {
			timestamp = tilNextMillis(lastTimestamp)
		}
	} else {
		sequence = 0
	}

	// 更新上次生成ID的时间戳
	lastTimestamp = timestamp

	// 生成ID
	// 时间戳部分 | 数据中心部分 | 机器标识部分 | 序列号部分
	snowflakeID := ((timestamp - twepoch) << 22) |
		(datacenterID << 17) |
		(machineID << 12) |
		sequence

	// 格式化为固定长度字符串
	return fmt.Sprintf("bayes%d", snowflakeID)
}

func getMachineIDFromIP() int64 {
	// 获取本机IP地址
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return 1 // 如果获取失败返回默认值1
	}

	var ipSum int64
	// 遍历所有网络接口
	for _, addr := range addrs {
		// 检查IP地址类型
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ip4 := ipnet.IP.To4(); ip4 != nil {
				// 使用IP地址所有段的和来生成机器ID
				for _, byte := range ip4 {
					ipSum += int64(byte)
				}
			}
		}
	}

	if ipSum == 0 {
		return 1 // 如果没有找到合适的IP地址,返回默认值1
	}

	// 确保机器ID在0-31范围内
	return ipSum % 32
}

// GenerateInviteCode 生成邀请码
func GenerateInviteCode() string {
	// 生成6位随机字符串,包含数字和大写字母
	const charset = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
	const length = 6

	// 使用crypto/rand生成随机字节
	b := make([]byte, length)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}

	// 将随机字节映射到字符集
	result := make([]byte, length)
	for i := range b {
		result[i] = charset[int(b[i])%len(charset)]
	}

	return string(result)
}

func GenerateUUID() string {
	return uuid.New().String()
}

// tilNextMillis 阻塞到下一个毫秒
func tilNextMillis(last int64) int64 {
	timestamp := time.Now().UnixMilli()
	for timestamp <= last {
		timestamp = time.Now().UnixMilli()
	}
	return timestamp
}
