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
func PrefixAnalyse(rdbFiles []string, topN int, maxDepth int, output string, indOutput bool, options ...interface{}) error {
	var outputPath string
	var outputFile *os.File
	var createFile bool
	var closeOutput bool
	var err error
	var csvWriter *csv.Writer
	if topN < 0 {
		return errors.New("n must greater than 0")
	} else if topN == 0 {
		topN = math.MaxInt
	}
	if maxDepth == 0 {
		maxDepth = math.MaxInt
	} else {
		maxDepth += 2 // for root(depth==1) and database root(depth==2)
	}
	for index, rdbFilename := range rdbFiles {
		createFile = false
		closeOutput = false
		outputPath, err = ckOutput(rdbFilename, output, indOutput, ".csv")
		fmt.Printf("「前缀分析」- RDB文件: %s -> 分析报告: %s\n", rdbFilename, outputPath)
		// 如果拆分输出报告，或者只有一个文件需要分析，则需要创建新的CSV文件，并且要关闭文件流
		if indOutput || len(rdbFiles) == 1 {
			createFile = true
			closeOutput = true
		} else {
			// 如果不需要拆分输出，在第一个rdb文件时，需要创建新的CSV文件，且无需关闭文件流
			if index == 0 {
				createFile = true
				closeOutput = false
			}
			// 如果是最后一个rdb文件，则不需要创建新的CSV文件，且需要关闭文件流
			if index == len(rdbFiles)-1 {
				createFile = false
				closeOutput = true
			}
		}
		if createFile {
			outputPath, outputFile, err = mkOutput(rdbFilename, output, indOutput, ".csv", false)
			if err != nil {
				return err
			}
			_, err = outputFile.WriteString("数据库,前缀,KEY大小,KEY大小[K/M/G],个数\n")
			if err != nil {
				return fmt.Errorf("write header failed: %v", err)
			}
			csvWriter = csv.NewWriter(outputFile)
		}
		if outputFile == nil {
			return fmt.Errorf("outputFile not created: %v", err)
		}
		if csvWriter == nil {
			return fmt.Errorf("csvWriter not created: %v", err)
		}
		err := prefixIt(rdbFilename, outputFile, csvWriter, topN, maxDepth, closeOutput, options...)
		if err != nil {
			return err
		}
	}
	fmt.Printf("「前缀分析」- 生成完成\n")
	//outputPath, outputFile, err = mkOutput(rdbFilename, output, false, ".csv", false)
	//if err != nil {
	//	return err
	//}
	return nil
}
