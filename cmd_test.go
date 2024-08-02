package main

import (
	"os"
	"testing"
)

// just make sure it can parse command line args correctly
func TestCmd(t *testing.T) {
	//os.Args = []string{"", "-c", "json", "-o", "/Users/ethnchao/memory-profile-1.json", "/Users/ethnchao/dump.rdb,/Users/ethnchao/dump.rdb"}
	//os.Args = []string{"", "-c", "json", "-ind-output", "/Users/ethnchao/dump-1.rdb,/Users/ethnchao/dump-2.rdb"}
	//os.Args = []string{"", "-c", "memory", "-o", "/Users/ethnchao/memory-profile-1.csv", "-regex", "^backer:activity:user:like:count:20240601Activity:MEDDY130045791440497195$", "/Users/ethnchao/dump.rdb"}
	//os.Args = []string{"", "-c", "memory", "-o", "/Users/ethnchao/memory-profile-1.csv", "-regex", "^backer:activity:user:like:count:20240601Activity:MEDDY130045791440497195$", "/Users/ethnchao/dump-1.rdb,/Users/ethnchao/dump-2.rdb"}
	//os.Args = []string{"", "-c", "memory", "-ind-output", "-regex", "^backer:activity:user:like:count:20240601Activity:MEDDY130045791440497195$", "/Users/ethnchao/dump-1.rdb,/Users/ethnchao/dump-2.rdb"}
	//os.Args = []string{"", "-c", "bigkey", "-o", "/Users/ethnchao/memory-bigkey-1.csv", "-n", "100", "/Users/ethnchao/dump.rdb"}
	os.Args = []string{"", "-c", "bigkey", "-n", "10", "/Users/ethnchao/dump-1.rdb,/Users/ethnchao/dump-2.rdb,/Users/ethnchao/dump-2.rdb,/Users/ethnchao/dump-2.rdb"}
	//os.Args = []string{"", "-c", "bigkey", "-ind-output", "-regex", "^backer:activity:user:like:count:20240601Activity:MEDDY130045791440497195$", "/Users/ethnchao/dump-1.rdb,/Users/ethnchao/dump-2.rdb"}
	main()
	//err := os.MkdirAll("tmp", os.ModePerm)
	//if err != nil {
	//	return
	//}
	//defer func() {
	//	err := os.RemoveAll("tmp")
	//	if err != nil {
	//		t.Logf("remove tmp directory failed: %v", err)
	//	}
	//}()
	//// test command line parser only
	//os.Args = []string{"", "-c", "json", "-o", "tmp/cmd.json", "cases/memory.rdb"}
	//main()
	//if f, _ := os.Stat("tmp/cmd.json"); f == nil {
	//	t.Error("command json failed")
	//}
	//os.Args = []string{"", "-c", "memory", "-o", "tmp/memory.csv", "cases/memory.rdb"}
	//main()
	//if f, _ := os.Stat("tmp/memory.csv"); f == nil {
	//	t.Error("command memory failed")
	//}
	//os.Args = []string{"", "-c", "aof", "-o", "tmp/memory.aof", "cases/memory.rdb"}
	//main()
	//if f, _ := os.Stat("tmp/memory.aof"); f == nil {
	//	t.Error("command memory failed")
	//}
	//os.Args = []string{"", "-c", "bigkey", "-o", "tmp/bigkey.csv", "-n", "10", "cases/memory.rdb"}
	//main()
	//if f, _ := os.Stat("tmp/bigkey.csv"); f == nil {
	//	t.Error("command bigkey failed")
	//}
	//os.Args = []string{"", "-c", "bigkey", "-n", "10", "cases/memory.rdb"}
	//main()
	//
	//os.Args = []string{"", "-c", "memory", "-o", "tmp/memory_regex.csv", "-regex", "^l.*", "cases/memory.rdb"}
	//main()
	//if f, _ := os.Stat("tmp/memory_regex.csv"); f == nil {
	//	t.Error("command memory failed")
	//}
	//
	//os.Args = []string{"", "-c", "memory", "-o", "tmp/memory_regex.csv", "-regex", "^l.*", "-no-expired", "cases/memory.rdb"}
	//main()
	//if f, _ := os.Stat("tmp/memory_regex.csv"); f == nil {
	//	t.Error("command memory failed")
	//}
	//os.Args = []string{"", "-c", "prefix", "-o", "tmp/tree.csv", "cases/tree.rdb"}
	//main()
	//if f, _ := os.Stat("tmp/tree.csv"); f == nil {
	//	t.Error("command prefix failed")
	//}
	//
	//// test error command line
	//os.Args = []string{"", "-c", "json", "-o", "tmp/output", "/none/a"}
	//main()
	//os.Args = []string{"", "-c", "aof", "-o", "tmp/output", "/none/a"}
	//main()
	//os.Args = []string{"", "-c", "memory", "-o", "tmp/output", "/none/a"}
	//main()
	//os.Args = []string{"", "-c", "bigkey", "-o", "tmp/output", "/none/a"}
	//main()
	//
	//os.Args = []string{"", "-c", "bigkey", "-o", "/none/a", "-n", "10", "cases/memory.rdb"}
	//main()
	//os.Args = []string{"", "-c", "aof", "-o", "/none/a", "cases/memory.rdb"}
	//main()
	//os.Args = []string{"", "-c", "memory", "-o", "/none/a", "cases/memory.rdb"}
	//main()
	//os.Args = []string{"", "-c", "json", "-o", "/none/a", "cases/memory.rdb"}
	//main()
	//
	//os.Args = []string{"", "-c", "bigkey", "-o", "tmp/bigkey.csv", "cases/memory.rdb"}
	//main()
	//os.Args = []string{"", "-c", "none", "-o", "tmp/memory.aof", "cases/memory.rdb"}
	//main()
	//os.Args = []string{""}
	//main()
	//os.Args = []string{"", "-c", "aof"}
	//main()
}
