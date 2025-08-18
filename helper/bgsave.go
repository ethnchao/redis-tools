package helper

import (
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/scylladb/termtables"
)

type BgSave struct {
	RedisServer string
	Password    string
	UseMaster   bool
	WorkDir     string
	NoDelete    bool
	NoCluster   bool
	DryRun      bool
	Files       []string
	tmpDir      string
	*RedisConnection
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
	fmt.Println("🔗 正在连接Redis服务器...")
	err := s.RedisConnection.ConnectRedis()
	if err != nil {
		return err
	}
	fmt.Printf("✅ Redis连接成功 | 模式: %s\n",
		map[bool]string{true: "集群模式", false: "单机模式"}[s.IsCluster])
	return nil
}

func (s *BgSave) mkTmpDir() error {
	// 直接使用传入的工作目录
	s.tmpDir = s.WorkDir
	fmt.Printf("📁 工作目录: %s\n", s.tmpDir)
	return nil
}

func (s *BgSave) dump() error {
	if s.UseMaster && len(s.Masters) == 0 {
		return fmt.Errorf("❌ 错误: 用户选择使用Master节点进行分析，但没有可用的Master节点")
	}
	if !s.UseMaster && len(s.Slaves) == 0 {
		return fmt.Errorf("❌ 错误: 用户选择使用Slave节点进行分析，但没有可用的Slave节点")
	}
	var nodes []string
	var files []string
	if s.UseMaster {
		fmt.Printf("🎯 使用Master节点进行RDB导出 (%d个节点)\n", len(s.Masters))
		nodes = s.Masters
	} else {
		fmt.Printf("🎯 使用Slave节点进行RDB导出 (%d个节点)\n", len(s.Slaves))
		nodes = s.Slaves
	}

	fmt.Println("📦 开始生成RDB文件...")
	for i, node := range nodes {
		fmt.Printf("  [%d/%d] 正在从 %s 导出RDB...", i+1, len(nodes), node)
		nodeArr := strings.Split(node, ":")
		host := nodeArr[0]
		port := nodeArr[1]
		rdbPath := fmt.Sprintf("%s/redis-dump-%s.rdb", s.tmpDir, strings.ReplaceAll(node, ":", "-"))
		cmd := exec.Command("redis-cli", "-h", host, "-p", port, "-a", s.Password, "--no-auth-warning", "--rdb", rdbPath)
		cmd.Stdout = nil // 不显示redis-cli的输出
		cmd.Stderr = nil
		err := cmd.Run()
		if err != nil {
			fmt.Printf(" ❌ 失败: %v\n", err)
			continue
		}
		// 获取文件大小
		if fileInfo, err := os.Stat(rdbPath); err == nil {
			fmt.Printf(" ✅ 完成 (%.2fMB)\n", float64(fileInfo.Size())/1024/1024)
		} else {
			fmt.Println(" ✅ 完成")
		}
		files = append(files, rdbPath)
	}
	if len(files) == 0 {
		return fmt.Errorf("❌ 错误: 没有成功生成任何RDB文件")
	}
	fmt.Printf("🎉 RDB文件生成完成，共生成 %d 个文件\n", len(files))
	s.Files = files
	return nil
}

func (s *BgSave) Clean() {
	if s.NoDelete {
		fmt.Println("🔒 保留工作目录 (用户指定)")
		return
	}
	_, err := os.Stat(s.tmpDir)
	if err != nil {
		fmt.Printf("⚠️  工作目录已不存在: %s\n", s.tmpDir)
		return
	}
	fmt.Printf("🧹 清理工作目录: %s\n", s.tmpDir)
	err = os.RemoveAll(s.tmpDir)
	if err != nil {
		fmt.Printf("❌ 清理失败: %v\n", err)
		return
	}
	fmt.Println("✅ 清理完成")
}

func (s *BgSave) Run() {
	fmt.Println("🚀 启动RDB导出任务")
	fmt.Println("==========================================")

	// 初始化Redis连接配置
	s.RedisConnection = &RedisConnection{
		RedisServer: s.RedisServer,
		Password:    s.Password,
		NoCluster:   s.NoCluster,
	}

	var err error
	err = s.connect()
	if err != nil {
		fmt.Printf("❌ 连接失败: %s\n", err)
		return
	}

	fmt.Println("\n📊 节点信息:")
	s.printNodes(s.Masters, s.Slaves)

	err = s.mkTmpDir()
	if err != nil {
		fmt.Printf("❌ 工作目录创建失败: %v\n", err)
		return
	}

	if s.DryRun {
		fmt.Println("🧪 试运行模式，跳过RDB导出")
		return
	}

	err = s.dump()
	if err != nil {
		fmt.Printf("❌ RDB导出失败: %s\n", err)
		return
	}

	fmt.Println("==========================================")
	fmt.Println("✅ RDB导出任务完成")
}
