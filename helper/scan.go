package helper

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

type ScanTask struct {
	RedisServer string
	Password    string
	Pattern     string
	NoCluster   bool
	Limit       int
	*RedisConnection
}

func (s *ScanTask) scan() error {
	fmt.Println("🔍 启动KEY扫描任务")
	fmt.Println("==========================================")

	// 连接Redis并识别模式
	fmt.Println("🔗 正在连接Redis服务器...")
	err := s.RedisConnection.ConnectRedis()
	if err != nil {
		return fmt.Errorf("❌ 连接失败: %v", err)
	}

	fmt.Printf("🎯 扫描模式: %s\n", s.Pattern)
	if s.Limit > 0 {
		fmt.Printf("📏 扫描数量上限: %d\n", s.Limit)
	} else {
		fmt.Println("📏 扫描数量上限: 无限")
	}
	fmt.Printf("🔧 连接模式: %s\n",
		map[bool]string{true: "集群模式", false: "单机模式"}[s.IsCluster && !s.NoCluster])

	ctx := context.Background()
	if !s.IsCluster || s.NoCluster {
		redisClient := s.CreateRedisClient()
		defer redisClient.Close()
		return s.scanStandalone(ctx, redisClient)
	} else {
		clusterClient := s.CreateRedisClusterClient()
		defer clusterClient.Close()
		return s.scanCluster(ctx, clusterClient)
	}
}

func (s *ScanTask) scanStandalone(ctx context.Context, client *redis.Client) error {
	fmt.Println("\n📋 开始扫描KEY...")
	var cursor uint64
	var totalCount int
	batchCount := 0

	for {
		keys, newCursor, err := client.Scan(ctx, cursor, s.Pattern, 100).Result()
		if err != nil {
			return fmt.Errorf("❌ 扫描失败: %v", err)
		}

		// 应用上限
		if s.Limit > 0 {
			remain := s.Limit - totalCount
			if remain <= 0 {
				break
			}
			if len(keys) > remain {
				keys = keys[:remain]
			}
		}

		if len(keys) > 0 {
			batchCount++
			fmt.Printf("  [批次 %d] 发现 %d 个KEY:\n", batchCount, len(keys))
			for _, key := range keys {
				fmt.Printf("    🔑 %s\n", key)
			}
		}
		totalCount += len(keys)
		cursor = newCursor
		if cursor == 0 || (s.Limit > 0 && totalCount >= s.Limit) {
			break
		}
	}

	fmt.Println("==========================================")
	if s.Limit > 0 && totalCount >= s.Limit {
		fmt.Printf("🎉 扫描完成，达到上限，共输出 %d 个KEY\n", totalCount)
	} else {
		fmt.Printf("🎉 扫描完成，总共发现 %d 个匹配的KEY\n", totalCount)
	}
	return nil
}

func (s *ScanTask) scanCluster(ctx context.Context, client *redis.ClusterClient) error {
	fmt.Println("\n📋 开始扫描集群KEY...")
	var cursor uint64
	var totalCount int
	batchCount := 0

	for {
		keys, newCursor, err := client.Scan(ctx, cursor, s.Pattern, 100).Result()
		if err != nil {
			return fmt.Errorf("❌ 扫描失败: %v", err)
		}

		// 应用上限
		if s.Limit > 0 {
			remain := s.Limit - totalCount
			if remain <= 0 {
				break
			}
			if len(keys) > remain {
				keys = keys[:remain]
			}
		}

		if len(keys) > 0 {
			batchCount++
			fmt.Printf("  [批次 %d] 发现 %d 个KEY:\n", batchCount, len(keys))
			for _, key := range keys {
				fmt.Printf("    🔑 %s\n", key)
			}
		}
		totalCount += len(keys)
		cursor = newCursor
		if cursor == 0 || (s.Limit > 0 && totalCount >= s.Limit) {
			break
		}
	}

	fmt.Println("==========================================")
	if s.Limit > 0 && totalCount >= s.Limit {
		fmt.Printf("🎉 集群扫描完成，达到上限，共输出 %d 个KEY\n", totalCount)
	} else {
		fmt.Printf("🎉 集群扫描完成，总共发现 %d 个匹配的KEY\n", totalCount)
	}
	return nil
}

func (s *ScanTask) Run() {
	// 初始化Redis连接配置
	s.RedisConnection = &RedisConnection{
		RedisServer: s.RedisServer,
		Password:    s.Password,
		NoCluster:   s.NoCluster,
	}

	err := s.scan()
	if err != nil {
		fmt.Printf("❌ 扫描失败: %s\n", err)
		return
	}
}
