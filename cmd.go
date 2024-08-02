package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"redis-tools/helper"
)

const help = `
This is a tool to parse Redis' RDB files
Options:
  -c <command>     including: json/memory/aof/bigkey/prefix/flamegraph
  -o <path>        output file path
  -n <number>      number of result, using in command: bigkey/prefix
  -p <password>    redis password, using when src is start with: redis://
  -port <number>   listen port for flame graph web service
  -sep <separator> for flamegraph, rdb will separate key by it, default value is ":". 
		supporting multi separators: -sep sep1 -sep sep2 
  -regex <regex>   using regex expression filter keys
  -no-expired      filter expired keys
  -use-master      Use master node to make rdb dump, default false
  -work-dir        Work directory, default to /tmp
  -ind-output      Individual output result: dump1.rdb result will be dump1.rdb.[json|aof|csv]

Examples:
parameters between '[' and ']' is optional
1. convert rdb to json
  rdb -c json -o dump.json dump.rdb
  rdb -c json -o dump.json dump1.rdb,dump2.rdb,dump3.rdb // This will parse multiple rdb, output result to dump.json
  rdb -c json -ind-output dump1.rdb,dump2.rdb,dump3.rdb // This will parse multiple rdb, individually output to dump1.rdb.json, dump2.rdb.json, dump3.rdb.json
  rdb -c json -o dump.json redis://127.0.0.1:6379/1 // Connect to redis server(support standalone, cluster) fetch rdb dump, then convert rdb to json
2. generate memory report(also support multiple rdb, individual output, connect to redis server fetch rdb)
  rdb -c memory -o memory.csv dump.rdb
3. convert to aof file
  rdb -c aof -o dump.aof dump.rdb
4. get largest keys(also support multiple rdb, individual output, connect to redis server fetch rdb)
  rdb -c bigkey [-o dump.aof] [-n 10] dump.rdb
5. get number and memory size by prefix
  rdb -c prefix [-n 10] [-max-depth 3] [-o prefix-report.csv] dump.rdb
6. draw flamegraph
  rdb -c flamegraph [-port 16379] [-sep :] dump.rdb
`

type separators []string

func (s *separators) String() string {
	return strings.Join(*s, " ")
}

func (s *separators) Set(value string) error {
	*s = append(*s, value)
	return nil
}

func main() {
	flagSet := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	var cmd string
	var output string
	var n int
	var port int
	var seps separators
	var regexExpr string
	var noExpired bool
	var maxDepth int
	var err error
	var password string
	var useMaster bool
	var workDir string
	var indOutput bool
	flagSet.StringVar(&cmd, "c", "", "command for rdb: json")
	flagSet.StringVar(&output, "o", "", "output file path")
	flagSet.IntVar(&n, "n", 0, "")
	flagSet.IntVar(&maxDepth, "max-depth", 0, "max depth of prefix tree")
	flagSet.IntVar(&port, "port", 0, "listen port for web")
	flagSet.Var(&seps, "sep", "separator for flame graph")
	flagSet.StringVar(&regexExpr, "regex", "", "regex expression")
	flagSet.BoolVar(&noExpired, "no-expired", false, "filter expired keys")
	flagSet.StringVar(&password, "p", "", "redis password")
	flagSet.BoolVar(&useMaster, "use-master", false, "use master nodes")
	flagSet.StringVar(&workDir, "work-dir", "/tmp", "working directory")
	flagSet.BoolVar(&indOutput, "ind-output", false, "Individual output file")
	_ = flagSet.Parse(os.Args[1:]) // ExitOnError
	src := flagSet.Arg(0)

	if cmd == "" {
		println(help)
		return
	}
	if src == "" {
		println("src file or redis server address is required")
		return
	}

	var rdbFiles []string

	if strings.HasPrefix(src, "redis://") {
		save := helper.BgSave{
			RedisServer: src,
			Password:    password,
			UseMaster:   useMaster,
			WorkDir:     workDir,
		}
		save.Run()
		//defer func() {
		//	save.Clean()
		//}()
		rdbFiles = save.Files
	} else {
		rdbFiles = strings.Split(src, ",")
	}

	var options []interface{}
	if regexExpr != "" {
		options = append(options, helper.WithRegexOption(regexExpr))
	}
	if noExpired {
		options = append(options, helper.WithNoExpiredOption())
	}

	switch cmd {
	case "json":
		err = helper.ToJsons(rdbFiles, output, indOutput, options...)
	case "memory":
		err = helper.MemoryProfile(rdbFiles, output, indOutput, options...)
	//case "aof":
	//	err = helper.ToAOF(src, output, options)
	case "bigkey":
		err = helper.FindBiggestKeys(rdbFiles, n, output, indOutput, options...)
	//case "prefix":
	//	err = helper.PrefixAnalyse(src, n, maxDepth, outputFile, options...)
	//case "flamegraph":
	//	_, err = helper.FlameGraph(src, port, seps, options...)
	//	if err != nil {
	//		fmt.Printf("error: %v\n", err)
	//		return
	//	}
	//	<-make(chan struct{})
	default:
		println("unknown command")
		return
	}
	if err != nil {
		fmt.Printf("error: %v\n", err)
		return
	}
}
