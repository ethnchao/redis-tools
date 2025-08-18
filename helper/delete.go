package helper

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/redis/go-redis/v9"
)

type DeleteTask struct {
	RedisServer string
	Password    string
	Pattern     string
	BatchSize   int
	NoCluster   bool
	*RedisConnection
}

// confirmDeletion 交互式二次确认，只有输入 DELETE 才继续
func (d *DeleteTask) confirmDeletion() bool {
	fmt.Println("⚠️  高危操作提示：即将执行批量删除KEY")
	fmt.Println("------------------------------------------")
	fmt.Printf("目标Redis: %s\n", d.RedisServer)
	fmt.Printf("匹配规则: %s\n", d.Pattern)
	fmt.Printf("批次大小: %d\n", d.BatchSize)
	fmt.Printf("集群模式: %v (可通过 -no-cluster 强制单机)\n", !d.NoCluster)
	fmt.Println("------------------------------------------")
	fmt.Println("请确认您已备份数据，且已经校验pattern无误。")
	fmt.Println("如需继续，请输入大写的 'DELETE' 并回车；其他任何输入将取消操作。")
	fmt.Print("> ")

	reader := bufio.NewReader(os.Stdin)
	line, _ := reader.ReadString('\n')
	line = strings.TrimSpace(line)
	if line == "DELETE" {
		fmt.Println("✅ 已确认，开始执行删除...")
		return true
	}
	fmt.Println("🛑 已取消删除操作")
	return false
}

func (d *DeleteTask) delete() error {
	fmt.Println("🗑️  启动KEY删除任务")
	fmt.Println("==========================================")

	// 安全检查：pattern不能为空或者为*
	if d.Pattern == "" || d.Pattern == "*" {
		return fmt.Errorf("⚠️  安全检查失败: 删除操作必须指定pattern，且不能为 '*'")
	}

	fmt.Printf("🎯 删除模式: %s\n", d.Pattern)
	fmt.Printf("📦 批次大小: %d\n", d.BatchSize)

	// 二次确认
	if !d.confirmDeletion() {
		return nil
	}

	// 连接Redis并识别模式
	fmt.Println("🔗 正在连接Redis服务器...")
	err := d.RedisConnection.ConnectRedis()
	if err != nil {
		return fmt.Errorf("❌ 连接失败: %v", err)
	}

	fmt.Printf("🔧 连接模式: %s\n",
		map[bool]string{true: "集群模式", false: "单机模式"}[d.IsCluster && !d.NoCluster])

	ctx := context.Background()
	if !d.IsCluster || d.NoCluster {
		redisClient := d.CreateRedisClient()
		defer redisClient.Close()
		return d.deleteStandalone(ctx, redisClient)
	} else {
		clusterClient := d.CreateRedisClusterClient()
		defer clusterClient.Close()
		return d.deleteCluster(ctx, clusterClient)
	}
}

func (d *DeleteTask) deleteStandalone(ctx context.Context, client *redis.Client) error {
	fmt.Println("\n🔍 扫描并删除KEY...")
	var cursor uint64
	var totalDeleted int
	var batch []string
	batchNum := 0

	for {
		keys, newCursor, err := client.Scan(ctx, cursor, d.Pattern, 100).Result()
		if err != nil {
			return fmt.Errorf("❌ 扫描失败: %v", err)
		}

		batch = append(batch, keys...)

		// 当批次达到指定大小或扫描完成时，执行删除
		if len(batch) >= d.BatchSize || newCursor == 0 {
			if len(batch) > 0 {
				batchNum++
				fmt.Printf("  [批次 %d] 正在删除 %d 个KEY...", batchNum, len(batch))
				deleted, err := d.deleteBatch(ctx, client, batch)
				if err != nil {
					fmt.Printf(" ❌ 失败: %v\n", err)
					return err
				}
				totalDeleted += deleted
				fmt.Printf(" ✅ 完成 (删除: %d, 累计: %d)\n", deleted, totalDeleted)
				batch = batch[:0] // 清空批次
			}
		}

		cursor = newCursor
		if cursor == 0 {
			break
		}
	}

	fmt.Println("==========================================")
	fmt.Printf("🎉 删除任务完成，总共删除 %d 个KEY\n", totalDeleted)
	return nil
}

func (d *DeleteTask) deleteCluster(ctx context.Context, client *redis.ClusterClient) error {
	fmt.Println("\n🔍 扫描并删除集群KEY...")
	var cursor uint64
	var totalDeleted int
	var batch []string
	batchNum := 0

	for {
		keys, newCursor, err := client.Scan(ctx, cursor, d.Pattern, 100).Result()
		if err != nil {
			return fmt.Errorf("❌ 扫描失败: %v", err)
		}

		batch = append(batch, keys...)

		// 当批次达到指定大小或扫描完成时，执行删除
		if len(batch) >= d.BatchSize || newCursor == 0 {
			if len(batch) > 0 {
				batchNum++
				fmt.Printf("  [批次 %d] 正在删除 %d 个KEY...", batchNum, len(batch))
				deleted, err := d.deleteBatchCluster(ctx, client, batch)
				if err != nil {
					fmt.Printf(" ❌ 失败: %v\n", err)
					return err
				}
				totalDeleted += deleted
				fmt.Printf(" ✅ 完成 (删除: %d, 累计: %d)\n", deleted, totalDeleted)
				batch = batch[:0] // 清空批次
			}
		}

		cursor = newCursor
		if cursor == 0 {
			break
		}
	}

	fmt.Println("==========================================")
	fmt.Printf("🎉 集群删除任务完成，总共删除 %d 个KEY\n", totalDeleted)
	return nil
}

func (d *DeleteTask) deleteBatch(ctx context.Context, client *redis.Client, keys []string) (int, error) {
	if len(keys) == 0 {
		return 0, nil
	}

	// 使用pipeline批量删除
	pipe := client.Pipeline()
	for _, key := range keys {
		pipe.Del(ctx, key)
	}

	results, err := pipe.Exec(ctx)
	if err != nil {
		return 0, fmt.Errorf("批量删除失败: %v", err)
	}

	deleted := 0
	for _, result := range results {
		if delResult, ok := result.(*redis.IntCmd); ok {
			deleted += int(delResult.Val())
		}
	}

	return deleted, nil
}

func (d *DeleteTask) deleteBatchCluster(ctx context.Context, client *redis.ClusterClient, keys []string) (int, error) {
	if len(keys) == 0 {
		return 0, nil
	}

	// 使用pipeline批量删除
	pipe := client.Pipeline()
	for _, key := range keys {
		pipe.Del(ctx, key)
	}

	results, err := pipe.Exec(ctx)
	if err != nil {
		return 0, fmt.Errorf("批量删除失败: %v", err)
	}

	deleted := 0
	for _, result := range results {
		if delResult, ok := result.(*redis.IntCmd); ok {
			deleted += int(delResult.Val())
		}
	}

	return deleted, nil
}

func (d *DeleteTask) Run() {
	// 初始化Redis连接配置
	d.RedisConnection = &RedisConnection{
		RedisServer: d.RedisServer,
		Password:    d.Password,
		NoCluster:   d.NoCluster,
	}

	// 设置默认批次大小
	if d.BatchSize <= 0 {
		d.BatchSize = 1000
	}

	err := d.delete()
	if err != nil {
		fmt.Printf("❌ 删除操作失败: %s\n", err)
		return
	}
}
