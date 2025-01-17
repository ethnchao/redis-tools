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
func FindBiggestKeys(rdbFiles []string, topN int, output string, indOutput bool, options ...interface{}) error {
	if topN < 0 {
		return errors.New("n must greater than 0")
	} else if topN == 0 {
		topN = 100
	}
	var outputPath string
	var outputFile *os.File
	var createFile bool
	var closeOutput bool
	var err error
	var top *topList
	var csvWriter *csv.Writer
	for index, rdbFilename := range rdbFiles {
		createFile = false
		closeOutput = false
		outputPath, err = ckOutput(rdbFilename, output, indOutput, ".csv")
		fmt.Printf("「大KEY分析」- RDB文件: %s -> 分析报告: %s\n", rdbFilename, outputPath)
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
			top = newToplist(topN)
			outputPath, outputFile, err = mkOutput(rdbFilename, output, indOutput, ".csv", false)
			if err != nil {
				return err
			}
			_, err = outputFile.WriteString("database,key,type,size,size_readable,element_count\n")
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
		err := findIt(rdbFilename, top, outputFile, csvWriter, closeOutput, options...)
		if err != nil {
			return err
		}
	}
	fmt.Printf("「大KEY分析」- 分析完成\n")
	return nil
}
