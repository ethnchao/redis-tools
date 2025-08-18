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
func ToJsons(rdbFiles []string, output string, indOutput bool, options ...interface{}) error {
	var outputFile *os.File
	var outputPath string
	var createFile bool
	var addSuffix bool
	var closeOutput bool
	var err error
	for index, rdbFilename := range rdbFiles {
		createFile = false
		addSuffix = false
		closeOutput = false
		outputPath, err = getOutPath(rdbFilename, output, indOutput, "-json.json")
		fmt.Printf("「JSON数据」- RDB文件: %s -> JSON文件: %s\n", rdbFilename, outputPath)
		if indOutput || len(rdbFiles) == 1 {
			createFile = true
			addSuffix = true
			closeOutput = true
		} else {
			if index == 0 {
				createFile = true
				addSuffix = false
				closeOutput = false
			}
			if index == len(rdbFiles)-1 {
				createFile = false
				addSuffix = true
				closeOutput = true
			}
		}
		if createFile {
			_, outputFile, err = createOutPath(rdbFilename, output, indOutput, "-json.json", false)
			if err != nil {
				return err
			}
			_, err := outputFile.WriteString("[\n")
			if err != nil {
				return fmt.Errorf("write json failed, %v", err)
			}
		}
		if outputFile == nil {
			return fmt.Errorf("outputFile not createed: %v", err)
		}
		altered, err := jsonIt(rdbFilename, outputFile, closeOutput, options...)
		if err != nil {
			return err
		}
		if addSuffix {
			if altered {
				_, err = outputFile.Seek(-2, 2)
				if err != nil {
					return fmt.Errorf("error during seek in file: %v", err)
				}
			}
			_, err := outputFile.WriteString("\n]")
			if err != nil {
				return fmt.Errorf("error during write in file: %v", err)
			}
		}
	}
	fmt.Printf("「JSON数据」- 生成完成\n")
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
