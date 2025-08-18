package helper

import (
	"encoding/csv"
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"

	"github.com/hdt3213/rdb/bytefmt"
	"github.com/hdt3213/rdb/core"
	"github.com/hdt3213/rdb/model"
)

func prefixIt(rdbFilename string, outputFile *os.File, csvWriter *csv.Writer, topN int, maxDepth int, closeOutput bool, options ...interface{}) error {
	// decode rdb file
	rdbFile, err := os.Open(rdbFilename)
	if err != nil {
		return fmt.Errorf("open rdb %s failed, %v", rdbFilename, err)
	}
	defer func() {
		_ = rdbFile.Close()
	}()
	if closeOutput {
		defer func() {
			_ = outputFile.Close()
		}()
	}
	defer csvWriter.Flush()
	var dec decoder = core.NewDecoder(rdbFile)
	if dec, err = wrapDecoder(dec, options...); err != nil {
		return err
	}

	// prefix tree
	tree := newRadixTree()
	err = dec.Parse(func(object model.RedisObject) bool {
		key := genKey(object.GetDBIndex(), object.GetKey())
		tree.insert(key, object.GetSize())
		return true
	})
	if err != nil {
		return err
	}

	// get top list
	topListO := newToplist(topN)
	tree.traverse(func(node *radixNode, depth int) bool {
		if depth > maxDepth {
			return false
		}
		if depth <= 2 {
			// skip root and database root
			return true
		}
		topListO.add(node)
		return true
	})
	printNode := func(node *radixNode) error {
		db, key := parseNodeKey(node.fullpath)
		dbStr := strconv.Itoa(db)
		return csvWriter.Write([]string{
			dbStr,
			key,
			strconv.Itoa(node.totalSize),
			bytefmt.FormatSize(uint64(node.totalSize)),
			strconv.Itoa(node.keyCount),
		})
	}
	for _, n := range topListO.list {
		node := n.(*radixNode)
		err := printNode(node)
		if err != nil {
			return err
		}
	}
	return nil
}

// PrefixAnalyse read rdb file and find the largest N keys.
// The invoker owns output, FindBiggestKeys won't close it
func PrefixAnalyse(rdbFiles []string, topN int, maxDepth int, workDir string, workDirName string, options ...interface{}) error {
	fmt.Println("🔍 启动前缀分析任务")
	fmt.Println("==========================================")

	if topN < 0 {
		return errors.New("❌ 错误: 结果数量必须大于0")
	} else if topN == 0 {
		topN = math.MaxInt
	}
	if maxDepth == 0 {
		maxDepth = math.MaxInt
	} else {
		maxDepth += 2 // for root(depth==1) and database root(depth==2)
	}

	fmt.Printf("📁 工作目录: %s\n", workDir)
	fmt.Printf("📊 分析文件数量: %d\n", len(rdbFiles))
	fmt.Printf("🎯 显示TOP %d 前缀 (最大深度: %d)\n\n",
		func() int {
			if topN == math.MaxInt {
				return -1
			} else {
				return topN
			}
		}(),
		func() int {
			if maxDepth-2 == math.MaxInt {
				return -1
			} else {
				return maxDepth - 2
			}
		}())

	var outputFiles []string // 用于收集生成的文件路径，后续压缩

	for i, rdbFilename := range rdbFiles {
		fmt.Printf("[%d/%d] 正在分析: %s\n", i+1, len(rdbFiles), rdbFilename)

		outputPath, outputFile, err := createOutPath(rdbFilename, workDir, "-prefix.csv", false)
		if err != nil {
			return fmt.Errorf("❌ 创建输出文件失败: %v", err)
		}

		// 收集输出文件路径
		outputFiles = append(outputFiles, outputPath)

		// 写入CSV头部
		_, err = outputFile.WriteString("数据库,前缀,KEY大小,KEY大小[K/M/G],个数\n")
		if err != nil {
			return fmt.Errorf("❌ 写入CSV头部失败: %v", err)
		}

		csvWriter := csv.NewWriter(outputFile)
		err = prefixIt(rdbFilename, outputFile, csvWriter, topN, maxDepth, true, options...)
		if err != nil {
			return fmt.Errorf("❌ 分析RDB文件失败: %v", err)
		}

		fmt.Printf("  ✅ 完成 -> %s\n", outputPath)
	}

	fmt.Println("\n📦 正在打包报告文件...")
	// 压缩输出文件
	if len(outputFiles) > 0 {
		zipPath := generateZipName(workDir, workDirName)
		err := compressFiles(outputFiles, zipPath)
		if err != nil {
			fmt.Printf("❌ 压缩失败: %v\n", err)
		} else {
			fmt.Printf("✅ 压缩完成: %s\n", zipPath)
			// 清理原始文件
			cleanupFiles(outputFiles)
		}
	}

	fmt.Println("==========================================")
	fmt.Printf("🎉 前缀分析任务完成，共分析 %d 个RDB文件\n", len(rdbFiles))
	return nil
}
