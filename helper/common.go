package helper

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

func getOutPath(rdbFilename string, output string, indOutput bool, suffix string) (string, error) {
	outputPath, _, err := createOutPath(rdbFilename, output, indOutput, suffix, true)
	return outputPath, err
}

func createOutPath(rdbFilename string, output string, indOutput bool, suffix string, dryRun bool) (string, *os.File, error) {
	// 如果没有配置输出，并且不是独立文件输出模式
	if output == "" && !indOutput {
		return "STDOUT", os.Stdout, nil
	} else {
		var outputPath string
		if indOutput {
			outputPath = rdbFilename + suffix
		} else {
			outputPath = output
		}
		outputPath = strings.Replace(outputPath, ":", "-", -1)
		if !dryRun {
			outputFile, err := os.Create(outputPath)
			if err != nil {
				return "", nil, fmt.Errorf("open output %s faild: %v", outputPath, err)
			}
			return outputPath, outputFile, nil
		}
		return outputPath, nil, nil
	}
}

func parseSrc(redisServer string) (hostPort string, dbInt int) {
	hostPort = strings.Split(redisServer, "redis://")[1]
	dbInt = 0
	if strings.Contains(hostPort, "/") {
		dbStr := strings.Split(hostPort, "/")[1]
		if dbStr != "" {
			dbInt, _ = strconv.Atoi(dbStr)
		}
	}
	return hostPort, dbInt
}
