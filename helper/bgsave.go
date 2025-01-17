package helper

import (
	"bufio"
	"context"
	"fmt"
	"github.com/lithammer/shortuuid/v4"
	"github.com/redis/go-redis/v9"
	"github.com/scylladb/termtables"
	"os"
	"os/exec"
	"strings"
)

type BgSave struct {
	RedisServer string
	Password    string
	UseMaster   bool
	WorkDir     string
	NoDelete    bool
	NoCluster   bool
	DryRun      bool
	hostPort    string
	info        map[string]string
	isCluster   bool
	masters     []string
	slaves      []string
	Files       []string
	tmpDir      string
	db          int
}

func (s *BgSave) printNodes(masters []string, slaves []string) {
	t := termtables.CreateTable()
	t.AddHeaders("Master节点", "Slave节点")
	maxSize := max(len(masters), len(slaves))
	for i := 0; i < maxSize; i++ {
		col1 := ""
		col2 := ""
		if i+1 <= len(masters) {
			col1 = masters[i]
		}
		if i+1 <= len(slaves) {
			col2 = slaves[i]
		}
		t.AddRow(col1, col2)
	}
	fmt.Println(t.Render())
}

func (s *BgSave) connect() error {
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
		return fmt.Errorf("「连接」- Redis登录失败：密码错误 ")
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
	s.info = info

	if err := scanner.Err(); err != nil {
		fmt.Printf("error occurred: %v\n", err)
	}

	var masters []string
	var slaves []string

	if info["redis_mode"] == "standalone" {
		fmt.Println("「连接」- 检测到Redis为 单机/哨兵模式...")
		if info["role"] == "master" {
			masters = append(masters, s.hostPort)
		} else {
			slaves = append(slaves, s.hostPort)
		}
	} else if info["redis_mode"] == "cluster" {
		fmt.Println("「连接」- 检测到集群为 集群模式...")
		if s.NoCluster {
			fmt.Println("「连接」- 用户指定不使用集群模式...")
			if info["role"] == "master" {
				masters = append(masters, s.hostPort)
			} else {
				slaves = append(slaves, s.hostPort)
			}
		} else {
			fmt.Println("「连接」- 以集群模式重连Redis...")
			redisClient2 := redis.NewClusterClient(&redis.ClusterOptions{
				Addrs:    []string{s.hostPort},
				Password: s.Password, // 没有密码，默认值
			})
			defer func(redisClient *redis.ClusterClient) {
				err := redisClient.Close()
				if err != nil {
					fmt.Printf("「连接」- 关闭连接异常，忽略: %v\n", err)
				}
			}(redisClient2)
			clusterNodesStr, err := redisClient2.ClusterNodes(ctx).Result()
			if err != nil {
				fmt.Printf("「连接」- 获取集群节点异常: %v\n", err)
				return err
			}
			scanner := bufio.NewScanner(strings.NewReader(clusterNodesStr))
			for scanner.Scan() {
				line := scanner.Text()
				if strings.Contains(line, "@") {
					lineArr := strings.Split(line, " ")
					addrStr := lineArr[1]
					roleStr := lineArr[2]
					addrArr := strings.Split(addrStr, "@")
					addr := addrArr[0]
					if strings.Contains(roleStr, "master") {
						masters = append(masters, addr)
					} else if strings.Contains(roleStr, "slave") {
						slaves = append(slaves, addr)
					}
				}
			}
		}
	}
	s.masters = masters
	s.slaves = slaves
	return nil
}

func (s *BgSave) mkTmpDir() error {
	s.tmpDir = fmt.Sprintf("%s/redis-tools-%s", s.WorkDir, shortuuid.New())
	fmt.Printf("「准备」- 创建工作目录：%s...\n", s.tmpDir)
	err := os.Mkdir(s.tmpDir, 0755)
	if err != nil {
		return fmt.Errorf("「准备」- 创建临时目录失败：%s", err)
	}
	return nil
}

func (s *BgSave) dump() error {
	if s.UseMaster && len(s.masters) == 0 {
		return fmt.Errorf("「导出」- 用户选择使用Master节点进行分析，但没有可用的Master节点")
	}
	if !s.UseMaster && len(s.slaves) == 0 {
		return fmt.Errorf("「导出」- 用户选择使用Slave节点进行分析，但没有可用的Slave节点")
	}
	var nodes []string
	var files []string
	if s.UseMaster {
		fmt.Println("「导出」- 使用Master节点进行分析...")
		nodes = s.masters
	} else {
		fmt.Println("「导出」- 使用Slave节点进行分析...")
		nodes = s.slaves
	}
	for i := range nodes {
		node := nodes[i]
		fmt.Printf("「导出」- 连接至：%s 以生成RDB文件...\n", node)
		nodeArr := strings.Split(node, ":")
		host := nodeArr[0]
		port := nodeArr[1]
		rdbPath := fmt.Sprintf("%s/redis-dump-%s.rdb", s.tmpDir, strings.ReplaceAll(node, ":", "-"))
		cmd := exec.Command("redis-cli", "-h", host, "-p", port, "-a", s.Password, "--no-auth-warning", "--rdb", rdbPath)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		err := cmd.Run()
		if err != nil {
			fmt.Printf("「导出」- 生成失败: %v，跳过.\n", err)
			continue
		}
		files = append(files, rdbPath)
	}
	if len(files) == 0 {
		return fmt.Errorf("「导出」- 没有获取到任何rdb文件")
	}
	s.Files = files
	return nil
}

func (s *BgSave) Clean() {
	if s.NoDelete {
		fmt.Println("「清理」- 用户要求保留临时目录.")
		return
	}
	_, err := os.Stat(s.tmpDir)
	if err != nil {
		fmt.Printf("「清理」- 临时目录：%s 已不存在.\n", s.tmpDir)
		return
	}
	fmt.Printf("「清理」- 删除临时目录：%s...\n", s.tmpDir)
	err = os.RemoveAll(s.tmpDir)
	if err != nil {
		fmt.Printf("「清理」- 清理临时目录失败, %s", err)
		return
	}
}

func (s *BgSave) Run() {
	s.hostPort, s.db = parseSrc(s.RedisServer)
	var err error
	err = s.connect()
	fmt.Println("「连接」- 已扫描到的节点如下：")
	s.printNodes(s.masters, s.slaves)
	if err != nil {
		fmt.Printf("连接Redis失败：%s.\n", err)
		return
	}
	err = s.mkTmpDir()
	if err != nil {
		return
	}
	if s.DryRun {
		fmt.Println("Dry-Run模式，跳过导出RDB")
		return
	}
	err = s.dump()
	if err != nil {
		fmt.Printf("生成RDB文件失败：%s.\n", err)
		return
	}
}
