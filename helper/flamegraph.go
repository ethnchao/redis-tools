package helper

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/hdt3213/rdb/core"
	"github.com/hdt3213/rdb/d3flame"
	"github.com/hdt3213/rdb/model"
)

// TrimThreshold is the min count of keys to enable trim
var TrimThreshold = 1000

// FlameGraph draws flamegraph in web page to analysis memory usage pattern
func FlameGraph(rdbFiles []string, port int, separators []string, workDir string, workDirName string, options ...interface{}) error {
	if len(rdbFiles) == 0 {
		return errors.New("rdb files are required")
	}
	if port == 0 {
		port = 16379 // default port
	}

	// 创建根节点
	root := &d3flame.FlameItem{
		Name:     "root",
		Children: make(map[string]*d3flame.FlameItem),
	}
	var count int

	fmt.Printf("📁 工作目录: %s\n", workDir)
	fmt.Printf("📊 分析文件数量: %d\n", len(rdbFiles))
	fmt.Printf("🌐 Web服务端口: %d\n\n", port)

	// 处理所有RDB文件
	for i, rdbFilename := range rdbFiles {
		fmt.Printf("[%d/%d] 正在处理: %s\n", i+1, len(rdbFiles), rdbFilename)

		rdbFile, err := os.Open(rdbFilename)
		if err != nil {
			return fmt.Errorf("打开RDB文件 %s 失败: %v", rdbFilename, err)
		}

		var dec decoder = core.NewDecoder(rdbFile)
		if dec, err = wrapDecoder(dec, options...); err != nil {
			rdbFile.Close()
			return err
		}

		err = dec.Parse(func(object model.RedisObject) bool {
			count++
			addObject(root, separators, object)
			return true
		})
		rdbFile.Close()

		if err != nil {
			return fmt.Errorf("❌ 解析RDB文件 %s 失败: %v", rdbFilename, err)
		}

		fmt.Printf("  ✅ 完成\n")
	}

	// 计算总大小
	totalSize := 0
	for _, v := range root.Children {
		totalSize += v.Value
	}
	root.Value = totalSize

	// 如果数据量大，进行裁剪
	if count >= TrimThreshold {
		trimData(root)
	}

	// 序列化数据
	data, err := json.Marshal(root)
	if err != nil {
		return fmt.Errorf("序列化火焰图数据失败: %v", err)
	}

	fmt.Println("==========================================")
	fmt.Printf("🎉 火焰图分析完成，共处理 %d 个KEY\n", count)
	fmt.Printf("🌐 Web服务已启动: http://localhost:%d\n", port)
	fmt.Printf("⚠️  按 Ctrl+C 退出程序\n")

	// 启动Web服务并等待用户停止
	d3flame.Web(data, port)
	// 阻塞等待用户停止（通过Ctrl+C）
	select {}
}

func split(s string, separators []string) []string {
	sep := ":"
	if len(separators) > 0 {
		sep = separators[0]
	}
	for i := 1; i < len(separators); i++ {
		s = strings.ReplaceAll(s, separators[i], sep)
	}
	return strings.Split(s, sep)
}

func addObject(root *d3flame.FlameItem, separators []string, object model.RedisObject) {
	node := root
	parts := split(object.GetKey(), separators)
	parts = append([]string{"db:" + strconv.Itoa(object.GetDBIndex())}, parts...)
	for _, part := range parts {
		if node.Children[part] == nil {
			n := &d3flame.FlameItem{
				Name:     part,
				Children: make(map[string]*d3flame.FlameItem),
			}
			node.AddChild(n)
		}
		node = node.Children[part]
		node.Value += object.GetSize()
	}
}

// bigNodeThreshold is the min size
var bigNodeThreshold = 1024 * 1024 // 1MB

func trimData(root *d3flame.FlameItem) {
	// trim long tail
	queue := []*d3flame.FlameItem{
		root,
	}
	for len(queue) > 0 {
		// Aggregate leaf nodes
		node := queue[0]
		queue = queue[1:]
		leafSum := 0
		for key, child := range node.Children {
			if len(child.Children) == 0 && child.Value < bigNodeThreshold { // child is a leaf node
				delete(node.Children, key) // remove small leaf keys
				leafSum += child.Value
			}
			queue = append(queue, child) // reserve big key
		}
		if leafSum > 0 {
			n := &d3flame.FlameItem{
				Name:  "others",
				Value: leafSum,
			}
			node.AddChild(n)
		}
	}
}
