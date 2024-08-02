package helper

import (
	"encoding/csv"
	"errors"
	"fmt"
	"github.com/hdt3213/rdb/bytefmt"
	"github.com/hdt3213/rdb/core"
	"github.com/hdt3213/rdb/model"
	"os"
	"strconv"
	"time"
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
			return ""
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
func MemoryProfile(rdbFiles []string, output string, indOutput bool, options ...interface{}) error {
	var outputPath string
	var outputFile *os.File
	var createFile bool
	var closeOutput bool
	var err error
	var csvWriter *csv.Writer
	for index, rdbFilename := range rdbFiles {
		createFile = false
		closeOutput = false
		if indOutput || len(rdbFiles) == 1 {
			createFile = true
			closeOutput = true
		} else {
			if index == 0 {
				createFile = true
				closeOutput = false
			}
			if index == len(rdbFiles)-1 {
				createFile = false
				closeOutput = true
			}
		}
		if createFile {
			outputPath, outputFile, err = mkOutput(rdbFilename, output, indOutput, ".csv")
			if err != nil {
				return err
			}
			fmt.Printf("「内存报告」- RDB文件: %s -> 报告文件: %s\n", rdbFilename, outputPath)
			_, err = outputFile.WriteString("database,key,type,size,size_readable,element_count,encoding,expiration\n")
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
		err := profileIt(rdbFilename, outputFile, csvWriter, closeOutput, options...)
		if err != nil {
			return err
		}
	}
	fmt.Printf("「内存报告」- 生成完成\n")
	return nil
}
