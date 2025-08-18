package helper

import (
	"bufio"
	"context"
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9"
)

type DeleteTask struct {
	RedisServer string
	Password    string
	Pattern     string
	BatchSize   int
	hostPort    string
	info        map[string]string
	isCluster   bool
	db          int
}

func (d *DeleteTask) delete() error {
	// 安全检查：pattern不能为空或者为*
	if d.Pattern == "" || d.Pattern == "*" {
		return fmt.Errorf("「安全检查」- 删除操作必须指定pattern，且不能为 '*'")
	}

	fmt.Printf("「连接」- 以单机模式连接至Redis：%s...\n", d.hostPort)
	ctx := context.Background()
	redisClient := redis.NewClient(&redis.Options{
		Addr:     d.hostPort,
		Password: d.Password,
		DB:       d.db,
	})
	defer func(redisClient *redis.Client) {
		err := redisClient.Close()
		if err != nil {
			fmt.Printf("「连接」- 关闭连接异常，忽略: %v\n", err)
		}
	}(redisClient)

	infoStr := redisClient.Info(ctx).String()
	if strings.Contains(infoStr, "ERR invalid password") {
		return fmt.Errorf("「连接」- Redis登录失败：密码错误")
	} else if strings.Contains(infoStr, "NOAUTH Authentication required") {
		return fmt.Errorf("「连接」- Redis登录失败：需要密码但未提供，请使用 -p 参数指定密码")
	} else {
		fmt.Println("「连接」- Redis登录成功.")
	}

	scanner := bufio.NewScanner(strings.NewReader(infoStr))
	info := make(map[string]string)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, ":") {
			lineArr := strings.Split(line, ":")
			info[lineArr[0]] = lineArr[1]
		}
	}
	d.info = info

	if err := scanner.Err(); err != nil {
		fmt.Printf("error occurred: %v\n", err)
	}

	fmt.Printf("「扫描」- 扫描并删除Redis KEY，规则为：%s，批次大小：%d\n", d.Pattern, d.BatchSize)

	if info["redis_mode"] == "standalone" {
		fmt.Println("「连接」- 检测到Redis为 单机/哨兵模式...")
		return d.deleteStandalone(ctx, redisClient)
	} else if info["redis_mode"] == "cluster" {
		fmt.Println("「连接」- 检测到集群为 集群模式...")
		fmt.Println("「连接」- 以集群模式重连Redis...")
		clusterClient := redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:    []string{d.hostPort},
			Password: d.Password,
		})
		defer func(redisClient *redis.ClusterClient) {
			err := redisClient.Close()
			if err != nil {
				fmt.Printf("「连接」- 关闭连接异常，忽略: %v\n", err)
			}
		}(clusterClient)
		return d.deleteCluster(ctx, clusterClient)
	}
	return nil
}

func (d *DeleteTask) deleteStandalone(ctx context.Context, client *redis.Client) error {
	var cursor uint64
	var totalDeleted int
	var batch []string

	for {
		keys, newCursor, err := client.Scan(ctx, cursor, d.Pattern, 100).Result()
		if err != nil {
			return fmt.Errorf("扫描keys失败: %v", err)
		}

		batch = append(batch, keys...)

		// 当批次达到指定大小或扫描完成时，执行删除
		if len(batch) >= d.BatchSize || newCursor == 0 {
			if len(batch) > 0 {
				deleted, err := d.deleteBatch(ctx, client, batch)
				if err != nil {
					return err
				}
				totalDeleted += deleted
				fmt.Printf("「删除」- 已删除 %d 个keys，累计删除：%d\n", deleted, totalDeleted)
				batch = batch[:0] // 清空批次
			}
		}

		cursor = newCursor
		if cursor == 0 {
			break
		}
	}

	fmt.Printf("「完成」- 总共删除了 %d 个keys\n", totalDeleted)
	return nil
}

func (d *DeleteTask) deleteCluster(ctx context.Context, client *redis.ClusterClient) error {
	var cursor uint64
	var totalDeleted int
	var batch []string

	for {
		keys, newCursor, err := client.Scan(ctx, cursor, d.Pattern, 100).Result()
		if err != nil {
			return fmt.Errorf("扫描keys失败: %v", err)
		}

		batch = append(batch, keys...)

		// 当批次达到指定大小或扫描完成时，执行删除
		if len(batch) >= d.BatchSize || newCursor == 0 {
			if len(batch) > 0 {
				deleted, err := d.deleteBatchCluster(ctx, client, batch)
				if err != nil {
					return err
				}
				totalDeleted += deleted
				fmt.Printf("「删除」- 已删除 %d 个keys，累计删除：%d\n", deleted, totalDeleted)
				batch = batch[:0] // 清空批次
			}
		}

		cursor = newCursor
		if cursor == 0 {
			break
		}
	}

	fmt.Printf("「完成」- 总共删除了 %d 个keys\n", totalDeleted)
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
	d.hostPort, d.db = parseSrc(d.RedisServer)

	// 设置默认批次大小
	if d.BatchSize <= 0 {
		d.BatchSize = 1000
	}

	err := d.delete()
	if err != nil {
		fmt.Printf("删除操作失败：%s\n", err)
		return
	}
}
