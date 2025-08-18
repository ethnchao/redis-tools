package helper

import (
	"bufio"
	"context"
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9"
)

type Scan struct {
	RedisServer string
	Password    string
	Pattern     string
	hostPort    string
	info        map[string]string
	isCluster   bool
	db          int
}

func (s *Scan) scan() error {
	fmt.Printf("「连接」- 以单机模式连接至Redis：%s...\n", s.hostPort)
	ctx := context.Background()
	redisClient := redis.NewClient(&redis.Options{
		Addr:     s.hostPort,
		Password: s.Password,
		DB:       s.db,
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
		fmt.Println("「连接」- Redis登录成功: %s", infoStr)
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
	s.info = info

	if err := scanner.Err(); err != nil {
		fmt.Printf("error occurred: %v\n", err)
	}
	fmt.Printf("「扫描」- 扫描Redis KEY，规则为：%s\n", s.Pattern)
	if info["redis_mode"] == "standalone" {
		fmt.Println("「连接」- 检测到Redis为 单机/哨兵模式...")
		var cursor uint64
		var n int
		for {
			var keys []string
			var err error
			keys, cursor, err = redisClient.Scan(ctx, cursor, s.Pattern, 10).Result()
			if err != nil {
				panic(err)
			}
			if len(keys) > 0 {
				fmt.Println(strings.Join(keys, ", "))
			}
			n += len(keys)
			if cursor == 0 {
				break
			}
		}
	} else if info["redis_mode"] == "cluster" {
		fmt.Println("「连接」- 检测到集群为 集群模式...")
		fmt.Println("「连接」- 以集群模式重连Redis...")
		clusterClient := redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:    []string{s.hostPort},
			Password: s.Password, // 没有密码，默认值
		})
		defer func(redisClient *redis.ClusterClient) {
			err := redisClient.Close()
			if err != nil {
				fmt.Printf("「连接」- 关闭连接异常，忽略: %v\n", err)
			}
		}(clusterClient)
		var cursor uint64
		var n int
		for {
			var keys []string
			var err error
			keys, cursor, err = clusterClient.Scan(ctx, cursor, s.Pattern, 10).Result()
			if err != nil {
				panic(err)
			}
			if len(keys) > 0 {
				fmt.Println(strings.Join(keys, ", "))
			}
			n += len(keys)
			if cursor == 0 {
				break
			}
		}
	}
	return nil
}

func (s *Scan) Run() {
	s.hostPort, s.db = parseSrc(s.RedisServer)
	var err error
	err = s.scan()
	if err != nil {
		fmt.Printf("生成RDB文件失败：%s.\n", err)
		return
	}
}
