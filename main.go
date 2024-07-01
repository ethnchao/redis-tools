package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"github.com/hdt3213/rdb/helper"
	"log"
	"os"
	"redis-tools/tools"
	"strconv"
	"strings"
)

const help = `
Redis 分析工具
Options:
  -m          Module, including: bigkey[default]
  -a          Redis host:port address
  -p          Redis password, default empty
  -n          Number of result, default 100
  -regex      Using regex expression filter keys, default empty
  -no-expired Filter expired keys, default empty
  -use-master Use master node to analyze, default false
  -work-dir   Work directory, default to /tmp

Examples:
Parameters between '[' and ']' is optional
1. find redis biggest keys
  redis-tools [-m bigkey] -a 127.0.0.1:6379 [-p password] [-n 100] \
	[-regex '^PREFIX\-.*'] [-no-expired] [-use-master] [-work-dir /opt]
`

type separators []string

func (s *separators) String() string {
	return strings.Join(*s, " ")
}

func (s *separators) Set(value string) error {
	*s = append(*s, value)
	return nil
}

func test() {
	flagSet := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	var module string
	var addr string
	var password string
	var numOfResult int
	var regexExpr string
	var noExpired bool
	var useMaster bool
	var workDir string
	var rdbFile string
	flagSet.StringVar(&module, "m", "bigkey", "module name")
	flagSet.StringVar(&addr, "a", "", "redis host:port address")
	flagSet.StringVar(&password, "p", "", "redis password")
	flagSet.IntVar(&numOfResult, "n", 100, "number of results")
	flagSet.StringVar(&regexExpr, "regex", "", "regex expression")
	flagSet.BoolVar(&noExpired, "no-expired", false, "filter expired keys")
	flagSet.BoolVar(&useMaster, "use-master", false, "use master nodes")
	flagSet.StringVar(&workDir, "work-dir", "/tmp", "working directory")
	flagSet.StringVar(&rdbFile, "f", "", "Use RDB file instead of Redis connection")
	_ = flagSet.Parse(os.Args[1:]) // ExitOnError

	if addr == "" {
		println(help)
		return
	}

	var options []interface{}
	if regexExpr != "" {
		fmt.Printf("「条件」- 使用正则匹配规则：\"%s\"\n", regexExpr)
		options = append(options, helper.WithRegexOption(regexExpr))
	}
	if noExpired {
		fmt.Println("「条件」- 仅匹配不过期的KEY")
		options = append(options, helper.WithNoExpiredOption())
	}

	var err error
	ctx := context.Background()
	switch module {
	case "bigkey":
		tool := tools.RedisBigKeys{
			Ctx:       ctx,
			HostPort:  addr,
			Password:  password,
			UseMaster: useMaster,
			WorkDir:   workDir,
			RdbFile:   rdbFile,
		}
		tool.Run(options...)
	default:
		println("unknown command")
		return
	}
	if err != nil {
		fmt.Printf("error: %v\n", err)
		return
	}
}

func Test_hexToUint64() {
	zxidString := "0x600005f0c"
	log.Println(zxidString)
	if len(zxidString) < 3 {
		log.Fatalf("less than 3 characters on '%s'", zxidString)
	}
	zxid, err := strconv.ParseInt(zxidString[2:], 16, 64)
	if err != nil {
		log.Fatalf("error trying to parse value '%s' to int", zxidString[2:])
	}

	bs := make([]byte, 8)
	binary.BigEndian.PutUint64(bs, uint64(zxid))

	epoch := bs[:4]
	count := bs[4:]
	log.Println("epoch", binary.BigEndian.Uint32(epoch))
	log.Println("count", binary.BigEndian.Uint32(count))
}

//func main() {
//	var options []interface{}
//	options = append(options, helper.WithRegexOption("^MDM.*"))
//	tool := tools.RedisBigKeys{RegexExpr: "^MDM.*"}
//	tool.Run(options...)
//}

func main() {
	Test_hexToUint64()
}
