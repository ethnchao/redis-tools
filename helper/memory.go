package helper

import (
	"encoding/csv"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/hdt3213/rdb/bytefmt"
	"github.com/hdt3213/rdb/core"
	"github.com/hdt3213/rdb/model"
)

func profileIt(rdbFilename string, outputFile *os.File, csvWriter *csv.Writer, closeOutput bool, options ...interface{}) error {
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
	formatExpiration := func(o model.RedisObject) string {
		expiration := o.GetExpiration()
		if expiration == nil {
			return "PERSISTENT"
		}
		return expiration.Format(time.RFC3339)
	}
	return dec.Parse(func(object model.RedisObject) bool {
		err = csvWriter.Write([]string{
			strconv.Itoa(object.GetDBIndex()),
			object.GetKey(),
			object.GetType(),
			strconv.Itoa(object.GetSize()),
			bytefmt.FormatSize(uint64(object.GetSize())),
			strconv.Itoa(object.GetElemCount()),
			object.GetEncoding(),
			formatExpiration(object),
		})
		if err != nil {
			fmt.Printf("csv write failed: %v", err)
			return false
		}
		return true
	})
}

// MemoryProfile read rdb file and analysis memory usage then write result to csv file
func MemoryProfile(rdbFiles []string, workDir string, workDirName string, options ...interface{}) error {
	fmt.Println("🔍 启动内存分析任务")
	fmt.Println("==========================================")

	var outputFiles []string // 用于收集生成的文件路径，后续压缩

	fmt.Printf("📁 工作目录: %s\n", workDir)
	fmt.Printf("📊 分析文件数量: %d\n\n", len(rdbFiles))

	for i, rdbFilename := range rdbFiles {
		fmt.Printf("[%d/%d] 正在分析: %s\n", i+1, len(rdbFiles), rdbFilename)

		outputPath, outputFile, err := createOutPath(rdbFilename, workDir, "-memory.csv", false)
		if err != nil {
			return fmt.Errorf("❌ 创建输出文件失败: %v", err)
		}

		// 收集输出文件路径
		outputFiles = append(outputFiles, outputPath)

		// 写入CSV头部
		_, err = outputFile.WriteString("数据库,KEY名,KEY类型,KEY大小,KEY大小[K/M/G],元素个数,编码,过期时间/配置\n")
		if err != nil {
			return fmt.Errorf("❌ 写入CSV头部失败: %v", err)
		}

		csvWriter := csv.NewWriter(outputFile)
		err = profileIt(rdbFilename, outputFile, csvWriter, true, options...)
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
	fmt.Printf("🎉 内存分析任务完成，共分析 %d 个RDB文件\n", len(rdbFiles))
	return nil
}
