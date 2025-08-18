package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"redis-tools/helper"
)

const help = `
Redis工具集 - 用于解析RDB文件和操作Redis数据库

用法: redis-tools [选项] <数据源>

数据源:
  本地RDB文件      如: dump.rdb 或 dump1.rdb,dump2.rdb,dump3.rdb
  Redis连接地址    如: redis://127.0.0.1:6379 或 redis://127.0.0.1:6379/1

基础选项:
  -c <命令>        [必需] 指定执行的命令
                   可选值: json, memory, bigkey, prefix, flamegraph, scan, delete
  -data-dir <目录> 数据目录，用于存储RDB文件和报告 (默认: /tmp)
  -p <密码>        Redis密码，连接Redis服务器时使用
  -dry-run         试运行模式，不执行实际操作 (默认: false)

命令相关选项:
  -n <数量>        返回结果数量限制
                   · bigkey: 显示最大KEY的数量 (默认: 无限制)
                   · prefix: 显示前缀分析结果数量 (默认: 100)
                   · scan:   最多展示的KEY数量 (默认: 无限制)
  
  -pattern <模式>  glob风格的匹配模式，支持通配符
                   · scan: 扫描匹配的KEY (默认: *)
                   · delete: 删除匹配的KEY (必需，不可为*)
                   例如: user:*, cache:*:session, temp_*
  
  -batch-size <数量> 批量操作的大小
                   · delete: 每批删除的KEY数量 (默认: 1000)
  
  -max-depth <深度> 前缀分析的最大深度
                   · prefix: 分析层级深度 (默认: 无限制)
  
  -port <端口>     Web服务监听端口
                   · flamegraph: 火焰图Web服务端口 (默认: 16379)
  
  -sep <分隔符>    KEY分隔符，可多次指定
                   · flamegraph: 火焰图KEY分割符 (默认: ":")
                   例如: -sep : -sep _

过滤选项:
  -regex <正则>    正则表达式过滤器，过滤KEY名称
                   适用命令: json, memory, bigkey, prefix
                   例如: '^user:.*$', '.*session.*'
  
  -expire <类型>   按过期类型过滤KEY
                   可选值: persistent(持久), volatile(易失), not-expired(未过期), expired(已过期)
                   适用命令: json, memory, bigkey, prefix

连接选项:
  -use-master      使用Master节点生成RDB (默认: 使用Slave节点)
                   适用命令: 所有RDB文件分析命令
  
  -no-cluster      强制使用单机模式，不使用集群模式
                   适用命令: scan, delete 及所有Redis连接操作

使用示例:

1. RDB文件转JSON
   redis-tools -c json dump.rdb
   redis-tools -c json dump1.rdb,dump2.rdb    # 多文件处理
   redis-tools -c json redis://127.0.0.1:6379 # 连接Redis服务器

2. 内存分析报告
   redis-tools -c memory dump.rdb
   redis-tools -c memory -regex '^user:.*' dump.rdb  # 只分析user:开头的KEY

3. 大KEY分析
   redis-tools -c bigkey -n 20 dump.rdb       # 显示最大的20个KEY
   redis-tools -c bigkey redis://127.0.0.1:6379

4. 前缀分析
   redis-tools -c prefix -n 50 -max-depth 3 dump.rdb
   redis-tools -c prefix -data-dir /data redis://127.0.0.1:6379

5. KEY扫描
   redis-tools -c scan -pattern "user:*" -n 500 redis://127.0.0.1:6379
   redis-tools -c scan -pattern "session:*" -no-cluster -n 100 redis://127.0.0.1:6379

6. 批量删除KEY
   redis-tools -c delete -pattern "temp:*" redis://127.0.0.1:6379
   redis-tools -c delete -pattern "cache:expired:*" -batch-size 500 redis://127.0.0.1:6379
   redis-tools -c delete -pattern "session:*" -p mypassword redis://127.0.0.1:6379

7. 火焰图分析
   redis-tools -c flamegraph -port 8080 -sep : dump.rdb
   redis-tools -c flamegraph -sep : -sep _ dump.rdb

8. 高级过滤示例
   redis-tools -c memory -regex '^(user|order):.*' -expire persistent dump.rdb
   redis-tools -c bigkey -expire not-expired -n 10 redis://127.0.0.1:6379

注意事项:
- 删除操作必须指定-pattern参数，且不能为'*'以防误删
- 所有生成的报告文件会自动打包为ZIP格式
- 使用Redis连接时，工具会自动检测单机/集群模式
- 方括号[]内的参数为可选参数
`

type separators []string

func (s *separators) String() string {
	return strings.Join(*s, " ")
}

func (s *separators) Set(value string) error {
	*s = append(*s, value)
	return nil
}

func main() {
	flagSet := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	fmt.Println("==========================================")
	fmt.Println("🚀 Redis工具集 启动")
	fmt.Printf("🕒 %s\n", time.Now().Format("2006-01-02 15:04:05"))
	fmt.Println("==========================================")
	var cmd string
	var topN int
	var port int
	var seps separators
	var regexExpr string
	var expireOpt string
	var maxDepth int
	var err error
	var password string
	var useMaster bool
	var dataDir string
	var pattern string
	var noCluster bool
	var dryRun bool
	var batchSize int
	flagSet.StringVar(&cmd, "c", "", "command for rdb: json")
	flagSet.IntVar(&topN, "n", 0, "")
	flagSet.IntVar(&maxDepth, "max-depth", 0, "max depth of prefix tree")
	flagSet.IntVar(&port, "port", 0, "listen port for web")
	flagSet.Var(&seps, "sep", "separator for flame graph")
	flagSet.StringVar(&regexExpr, "regex", "", "regex expression")
	flagSet.StringVar(&expireOpt, "expire", "", "persistent/volatile/not-expired")
	flagSet.StringVar(&password, "p", "", "redis password")
	flagSet.BoolVar(&useMaster, "use-master", false, "use master nodes")
	flagSet.StringVar(&dataDir, "data-dir", "/tmp", "data directory for storing rdb files and reports")
	flagSet.StringVar(&pattern, "pattern", "*", "glob-style pattern")
	flagSet.BoolVar(&noCluster, "no-cluster", false, "do not use cluster mode")
	flagSet.BoolVar(&dryRun, "dry-run", false, "dry run mode")
	flagSet.IntVar(&batchSize, "batch-size", 1000, "batch size for delete operation")
	_ = flagSet.Parse(os.Args[1:]) // ExitOnError
	src := flagSet.Arg(0)

	if cmd == "" {
		println(help)
		return
	}
	if src == "" {
		fmt.Println("❌ 错误: 必须指定数据源 (RDB文件路径或Redis连接地址)")
		fmt.Println("   示例: redis-tools -c memory dump.rdb")
		fmt.Println("   示例: redis-tools -c scan redis://127.0.0.1:6379")
		return
	}

	var rdbFiles []string

	// 生成唯一工作目录
	now := time.Now()
	workDirName := fmt.Sprintf("redis-tools-%s", now.Format("20060102-150405"))
	workDir := fmt.Sprintf("%s/%s", dataDir, workDirName)

	// 创建工作目录
	err = os.MkdirAll(workDir, 0755)
	if err != nil {
		fmt.Printf("❌ 创建工作目录失败: %v\n", err)
		return
	}

	// 需要生成RDB文件的命令
	needsRdbFile := func(command string) bool {
		rdbCommands := []string{"json", "memory", "bigkey", "prefix", "flamegraph"}
		for _, c := range rdbCommands {
			if c == command {
				return true
			}
		}
		return false
	}

	if strings.HasPrefix(src, "redis://") && needsRdbFile(cmd) {
		save := helper.BgSave{
			RedisServer: src,
			Password:    password,
			UseMaster:   useMaster,
			WorkDir:     workDir,
			NoCluster:   noCluster,
			DryRun:      dryRun,
		}
		save.Run()
		//defer func() {
		//	save.Clean()
		//}()
		rdbFiles = save.Files
	} else {
		rdbFiles = strings.Split(src, ",")
	}

	var options []interface{}
	if regexExpr != "" {
		options = append(options, helper.WithRegexOption(regexExpr))
	}
	if expireOpt != "" {
		options = append(options, helper.WithExpireOption(expireOpt))
	}

	if dryRun {
		fmt.Println("🧪 试运行模式，跳过实际执行步骤")
		fmt.Println("==========================================")
		return
	}

	switch cmd {
	case "json":
		err = helper.ToJsons(rdbFiles, workDir, workDirName, options...)
	case "memory":
		err = helper.MemoryProfile(rdbFiles, workDir, workDirName, options...)
	//case "aof":
	//	err = helper.ToAOF(src, output, options)
	case "bigkey":
		err = helper.FindBiggestKeys(rdbFiles, topN, workDir, workDirName, options...)
	case "scan":
		scanTask := helper.ScanTask{
			RedisServer: src,
			Password:    password,
			Pattern:     pattern,
			NoCluster:   noCluster,
			Limit:       topN,
		}
		scanTask.Run()
	case "delete":
		deleteTask := helper.DeleteTask{
			RedisServer: src,
			Password:    password,
			Pattern:     pattern,
			BatchSize:   batchSize,
			NoCluster:   noCluster,
		}
		deleteTask.Run()
	case "prefix":
		err = helper.PrefixAnalyse(rdbFiles, topN, maxDepth, workDir, workDirName, options...)
	case "flamegraph":
		err = helper.FlameGraph(rdbFiles, port, seps, workDir, workDirName, options...)
	default:
		fmt.Printf("❌ 错误: 未知命令 '%s'\n", cmd)
		fmt.Println("   支持的命令: json, memory, bigkey, prefix, flamegraph, scan, delete")
		fmt.Println("   使用 'redis-tools' 查看完整帮助信息")
		return
	}
	if err != nil {
		fmt.Printf("❌ 执行失败: %v\n", err)
		return
	}
}
