package helper

import (
	"archive/zip"
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"
)

// RedisConnection Redis连接配置和信息
type RedisConnection struct {
	RedisServer string
	Password    string
	NoCluster   bool
	HostPort    string
	DB          int
	Info        map[string]string
	IsCluster   bool
	Masters     []string
	Slaves      []string
}

// ConnectRedis 连接Redis并识别模式
func (rc *RedisConnection) ConnectRedis() error {
	rc.HostPort, rc.DB = parseSrc(rc.RedisServer)

	fmt.Printf("「连接」- 以单机模式连接至Redis：%s...\n", rc.HostPort)
	ctx := context.Background()
	redisClient := redis.NewClient(&redis.Options{
		Addr:     rc.HostPort,
		Password: rc.Password,
		DB:       rc.DB,
	})
	defer func(redisClient *redis.Client) {
		err := redisClient.Close()
		if err != nil {
			fmt.Printf("「连接」- 关闭连接异常，忽略: %v\n", err)
		}
	}(redisClient)

	// 验证连接和密码
	infoStr := redisClient.Info(ctx).String()
	if strings.Contains(infoStr, "ERR invalid password") {
		return fmt.Errorf("「连接」- Redis登录失败：密码错误")
	} else if strings.Contains(infoStr, "NOAUTH Authentication required") {
		return fmt.Errorf("「连接」- Redis登录失败：需要密码但未提供，请使用 -p 参数指定密码")
	} else {
		fmt.Println("「连接」- Redis登录成功.")
	}

	// 解析Redis信息
	scanner := bufio.NewScanner(strings.NewReader(infoStr))
	info := make(map[string]string)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, ":") {
			lineArr := strings.Split(line, ":")
			info[lineArr[0]] = lineArr[1]
		}
	}
	rc.Info = info

	if err := scanner.Err(); err != nil {
		fmt.Printf("error occurred: %v\n", err)
	}

	// 识别Redis模式和节点
	var masters []string
	var slaves []string

	if info["redis_mode"] == "standalone" {
		fmt.Println("「连接」- 检测到Redis为 单机/哨兵模式...")
		if info["role"] == "master" {
			masters = append(masters, rc.HostPort)
		} else {
			slaves = append(slaves, rc.HostPort)
		}
		rc.IsCluster = false
	} else if info["redis_mode"] == "cluster" {
		fmt.Println("「连接」- 检测到集群为 集群模式...")
		rc.IsCluster = true

		if rc.NoCluster {
			fmt.Println("「连接」- 用户指定不使用集群模式...")
			if info["role"] == "master" {
				masters = append(masters, rc.HostPort)
			} else {
				slaves = append(slaves, rc.HostPort)
			}
		} else {
			fmt.Println("「连接」- 以集群模式重连Redis...")
			clusterClient := redis.NewClusterClient(&redis.ClusterOptions{
				Addrs:    []string{rc.HostPort},
				Password: rc.Password,
			})
			defer func(redisClient *redis.ClusterClient) {
				err := redisClient.Close()
				if err != nil {
					fmt.Printf("「连接」- 关闭连接异常，忽略: %v\n", err)
				}
			}(clusterClient)

			clusterNodesStr, err := clusterClient.ClusterNodes(ctx).Result()
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

	rc.Masters = masters
	rc.Slaves = slaves
	return nil
}

// CreateRedisClient 创建Redis客户端（单机模式）
func (rc *RedisConnection) CreateRedisClient() *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:     rc.HostPort,
		Password: rc.Password,
		DB:       rc.DB,
	})
}

// CreateRedisClusterClient 创建Redis集群客户端
func (rc *RedisConnection) CreateRedisClusterClient() *redis.ClusterClient {
	return redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    []string{rc.HostPort},
		Password: rc.Password,
	})
}

func getOutPath(rdbFilename string, workDir string, suffix string) (string, error) {
	outputPath, _, err := createOutPath(rdbFilename, workDir, suffix, true)
	return outputPath, err
}

func createOutPath(rdbFilename string, workDir string, suffix string, dryRun bool) (string, *os.File, error) {
	// 生成基于RDB文件名的输出文件路径
	baseName := filepath.Base(rdbFilename)
	// 移除.rdb扩展名
	baseName = strings.TrimSuffix(baseName, ".rdb")
	outputPath := fmt.Sprintf("%s/%s%s", workDir, baseName, suffix)

	if !dryRun {
		outputFile, err := os.Create(outputPath)
		if err != nil {
			return "", nil, fmt.Errorf("创建输出文件 %s 失败: %v", outputPath, err)
		}
		return outputPath, outputFile, nil
	}
	return outputPath, nil, nil
}

func parseSrc(redisServer string) (hostPort string, dbInt int) {
	hostPort = strings.Split(redisServer, "redis://")[1]
	dbInt = 0
	if strings.Contains(hostPort, "/") {
		dbStr := strings.Split(hostPort, "/")[1]
		if dbStr != "" {
			dbInt, _ = strconv.Atoi(dbStr)
		}
	}
	return hostPort, dbInt
}

// compressFiles 将文件列表压缩为ZIP文件
func compressFiles(files []string, zipPath string) error {
	// 创建ZIP文件
	zipFile, err := os.Create(zipPath)
	if err != nil {
		return fmt.Errorf("创建ZIP文件失败: %v", err)
	}
	defer zipFile.Close()

	// 创建ZIP写入器
	zipWriter := zip.NewWriter(zipFile)
	defer zipWriter.Close()

	for _, filePath := range files {
		// 跳过不存在的文件
		if _, err := os.Stat(filePath); os.IsNotExist(err) {
			continue
		}

		// 打开要压缩的文件
		file, err := os.Open(filePath)
		if err != nil {
			return fmt.Errorf("打开文件 %s 失败: %v", filePath, err)
		}

		// 获取文件信息
		fileInfo, err := file.Stat()
		if err != nil {
			file.Close()
			return fmt.Errorf("获取文件信息失败: %v", err)
		}

		// 在ZIP中创建文件条目，只使用文件名，不包含完整路径
		fileName := filepath.Base(filePath)

		// 创建ZIP文件头，包含时间信息
		header := &zip.FileHeader{
			Name:     fileName,
			Method:   zip.Deflate,
			Modified: fileInfo.ModTime(),
		}

		zipEntry, err := zipWriter.CreateHeader(header)
		if err != nil {
			file.Close()
			return fmt.Errorf("创建ZIP条目失败: %v", err)
		}

		// 将文件内容复制到ZIP条目
		_, err = io.Copy(zipEntry, file)
		file.Close()
		if err != nil {
			return fmt.Errorf("复制文件内容失败: %v", err)
		}

		fmt.Printf("    📄 %s (%.2fKB)\n", fileName, float64(fileInfo.Size())/1024)
	}

	return nil
}

// cleanupFiles 清理原始文件
func cleanupFiles(files []string) {
	for _, filePath := range files {
		if err := os.Remove(filePath); err != nil {
			fmt.Printf("⚠️  清理文件失败 %s: %v\n", filepath.Base(filePath), err)
		}
	}
}

// generateZipName 生成ZIP文件名
func generateZipName(workDir string, workDirName string) string {
	return fmt.Sprintf("%s/%s-report.zip", filepath.Dir(workDir), workDirName)
}
