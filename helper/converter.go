package helper

import (
	"errors"
	"fmt"
	"os"

	"github.com/bytedance/sonic"
	"github.com/hdt3213/rdb/core"
	"github.com/hdt3213/rdb/model"
)

var jsonEncoder = sonic.ConfigDefault

func jsonIt(rdbFilename string, outputFile *os.File, closeOutput bool, options ...interface{}) (bool, error) {
	if rdbFilename == "" {
		return false, errors.New("src file path is required")
	}
	// open file
	rdbFile, err := os.Open(rdbFilename)
	if err != nil {
		return false, fmt.Errorf("open rdb %s failed, %v", rdbFilename, err)
	}
	defer func() {
		_ = rdbFile.Close()
	}()
	if closeOutput {
		defer func() {
			_ = outputFile.Close()
		}()
	}
	// create decoder
	var dec decoder = core.NewDecoder(rdbFile)
	if dec, err = wrapDecoder(dec, options...); err != nil {
		return false, err
	}
	altered := false
	err = dec.Parse(func(object model.RedisObject) bool {
		data, err := jsonEncoder.Marshal(object) // enable SortMapKeys to ensure same result
		if err != nil {
			fmt.Printf("json marshal failed: %v", err)
			return true
		}
		data = append(data, ',', '\n')
		_, err = outputFile.Write(data)
		if err != nil {
			fmt.Printf("write failed: %v", err)
			return true
		}
		altered = true
		return true
	})
	if err != nil {
		return altered, err
	}
	return altered, nil
}

// 如果是独立输出，每次都要创建文件加表头、关闭文件
// 如果是公共输出，只有第一次要创建文件加表头，最后一次关闭文件
// 让for循环控制信号的给出、创建文件（因为文件可能要复用），剩余步骤由子方法负责实现

// ToJsons read rdb file and convert to json file
func ToJsons(rdbFiles []string, workDir string, workDirName string, options ...interface{}) error {
	fmt.Println("🔄 启动JSON转换任务")
	fmt.Println("==========================================")

	var outputFiles []string // 用于收集生成的文件路径，后续压缩

	fmt.Printf("📁 工作目录: %s\n", workDir)
	fmt.Printf("📊 转换文件数量: %d\n\n", len(rdbFiles))

	for i, rdbFilename := range rdbFiles {
		fmt.Printf("[%d/%d] 正在转换: %s\n", i+1, len(rdbFiles), rdbFilename)

		outputPath, outputFile, err := createOutPath(rdbFilename, workDir, "-json.json", false)
		if err != nil {
			return fmt.Errorf("❌ 创建输出文件失败: %v", err)
		}

		// 收集输出文件路径
		outputFiles = append(outputFiles, outputPath)

		// 写入JSON开始标记
		_, err = outputFile.WriteString("[\n")
		if err != nil {
			return fmt.Errorf("❌ 写入JSON开始标记失败: %v", err)
		}

		altered, err := jsonIt(rdbFilename, outputFile, true, options...)
		if err != nil {
			return fmt.Errorf("❌ JSON转换失败: %v", err)
		}

		// 写入JSON结束标记
		if altered {
			_, err = outputFile.Seek(-2, 2)
			if err != nil {
				return fmt.Errorf("❌ 文件定位失败: %v", err)
			}
		}
		_, err = outputFile.WriteString("\n]")
		if err != nil {
			return fmt.Errorf("❌ 写入JSON结束标记失败: %v", err)
		}

		fmt.Printf("  ✅ 完成 -> %s\n", outputPath)
	}

	fmt.Println("\n📦 正在打包JSON文件...")
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
	fmt.Printf("🎉 JSON转换任务完成，共转换 %d 个RDB文件\n", len(rdbFiles))
	return nil
}

// ToAOF read rdb file and convert to aof file (Redis Serialization )
func ToAOF(rdbFilename string, aofFilename string, options ...interface{}) error {
	if rdbFilename == "" {
		return errors.New("src file path is required")
	}
	if aofFilename == "" {
		return errors.New("output file path is required")
	}
	rdbFile, err := os.Open(rdbFilename)
	if err != nil {
		return fmt.Errorf("open rdb %s failed, %v", rdbFilename, err)
	}
	defer func() {
		_ = rdbFile.Close()
	}()
	aofFile, err := os.Create(aofFilename)
	if err != nil {
		return fmt.Errorf("create json %s failed, %v", aofFilename, err)
	}
	defer func() {
		_ = aofFile.Close()
	}()

	var dec decoder = core.NewDecoder(rdbFile)
	if dec, err = wrapDecoder(dec, options...); err != nil {
		return err
	}
	return dec.Parse(func(object model.RedisObject) bool {
		cmdLines := ObjectToCmd(object, options...)
		data := CmdLinesToResp(cmdLines)
		_, err = aofFile.Write(data)
		if err != nil {
			fmt.Printf("write failed: %v", err)
			return true
		}
		return true
	})
}
