package helper

import (
	"encoding/csv"
	"errors"
	"fmt"
	"os"
	"strconv"

	"github.com/hdt3213/rdb/bytefmt"
	"github.com/hdt3213/rdb/core"
	"github.com/hdt3213/rdb/model"
)

func findIt(rdbFilename string, top *topList, outputFile *os.File, csvWriter *csv.Writer, closeOutput bool, options ...interface{}) error {
	if rdbFilename == "" {
		return errors.New("src file path is required")
	}
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
	err = dec.Parse(func(object model.RedisObject) bool {
		top.add(object)
		return true
	})
	if err != nil {
		return err
	}
	// 与memory模式不同的是，大key扫描的结果，需要在最终关闭输出时，才输出内容到csv，而memory模式则总是输出到csv
	if closeOutput {
		for _, o := range top.list {
			object := o.(model.RedisObject)
			err = csvWriter.Write([]string{
				strconv.Itoa(object.GetDBIndex()),
				object.GetKey(),
				object.GetType(),
				strconv.Itoa(object.GetSize()),
				bytefmt.FormatSize(uint64(object.GetSize())),
				strconv.Itoa(object.GetElemCount()),
			})
			if err != nil {
				return fmt.Errorf("csv write failed: %v", err)
			}
		}
	}
	return nil
}

// FindBiggestKeys read rdb file and find the largest N keys.
// The invoker owns output, FindBiggestKeys won't close it
func FindBiggestKeys(rdbFiles []string, topN int, workDir string, workDirName string, options ...interface{}) error {
	fmt.Println("🔍 启动大KEY分析任务")
	fmt.Println("==========================================")

	if topN < 0 {
		return errors.New("❌ 错误: 结果数量必须大于0")
	} else if topN == 0 {
		topN = 100
	}

	fmt.Printf("📁 工作目录: %s\n", workDir)
	fmt.Printf("📊 分析文件数量: %d\n", len(rdbFiles))
	fmt.Printf("🎯 显示TOP %d 大KEY\n\n", topN)

	var outputFiles []string // 用于收集生成的文件路径，后续压缩

	for i, rdbFilename := range rdbFiles {
		fmt.Printf("[%d/%d] 正在分析: %s\n", i+1, len(rdbFiles), rdbFilename)

		outputPath, outputFile, err := createOutPath(rdbFilename, workDir, "-bigkey.csv", false)
		if err != nil {
			return fmt.Errorf("❌ 创建输出文件失败: %v", err)
		}

		// 收集输出文件路径
		outputFiles = append(outputFiles, outputPath)

		// 写入CSV头部
		_, err = outputFile.WriteString("database,key,type,size,size_readable,element_count\n")
		if err != nil {
			return fmt.Errorf("❌ 写入CSV头部失败: %v", err)
		}

		csvWriter := csv.NewWriter(outputFile)
		top := newToplist(topN)
		err = findIt(rdbFilename, top, outputFile, csvWriter, true, options...)
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
	fmt.Printf("🎉 大KEY分析任务完成，共分析 %d 个RDB文件\n", len(rdbFiles))
	return nil
}
