package tools

import (
	"bufio"
	"context"
	"github.com/google/uuid"
	"github.com/hdt3213/rdb/core"
	"github.com/hdt3213/rdb/helper"
	"github.com/redis/go-redis/v9"
	"github.com/scylladb/termtables"
	"os"
	"os/exec"
	"path"
	"regexp"
	"strings"
	"time"
)

import (
	"errors"
	"fmt"
	"github.com/emirpasic/gods/sets/treeset"
	"github.com/hdt3213/rdb/bytefmt"
	"github.com/hdt3213/rdb/model"
	"strconv"
)

type RedisBigKeys struct {
	Ctx         context.Context
	HostPort    string
	Password    string
	NumOfResult int
	UseMaster   bool
	WorkDir     string
	RdbFile     string
	NoDelete    bool
	info        map[string]string
	isCluster   bool
	masters     []string
	slaves      []string
	rdbs        []string
	topList     *redisTreeSet
	tmpDir      string
}

type redisTreeSet struct {
	set      *treeset.Set
	capacity int
}

func (h *redisTreeSet) getMinimalItem() model.RedisObject {
	iter := h.set.Iterator()
	iter.End()
	if iter.Prev() {
		raw := iter.Value()
		return raw.(model.RedisObject)
	}
	return nil
}

// Append new object into tree set
// time complexity: O(n*log(m)), n is number of redis object, m is heap capacity. m if far less than n
func (h *redisTreeSet) appendObjectToHeap(x model.RedisObject) {
	if h.set.Size() < h.capacity {
		h.set.Add(x)
		return
	}
	// if heap is full && x.Size > minSize, then pop min
	minimal := h.getMinimalItem()
	if minimal.GetSize() < x.GetSize() {
		h.set.Remove(minimal)
		h.set.Add(x)
	}
}

func (h *redisTreeSet) IterRDBObjects() []model.RedisObject {
	result := make([]model.RedisObject, 0, h.set.Size())
	iter := h.set.Iterator()
	for iter.Next() {
		result = append(result, iter.Value().(model.RedisObject))
	}
	return result
}

func newRedisHeap(cap int) *redisTreeSet {
	s := treeset.NewWith(func(a, b interface{}) int {
		o1 := a.(model.RedisObject)
		o2 := b.(model.RedisObject)
		return o2.GetSize() - o1.GetSize() // desc order
	})
	return &redisTreeSet{
		set:      s,
		capacity: cap,
	}
}

type regexDecoder struct {
	reg *regexp.Regexp
	dec decoder
}

func (d *regexDecoder) Parse(cb func(object model.RedisObject) bool) error {
	return d.dec.Parse(func(object model.RedisObject) bool {
		if d.reg.MatchString(object.GetKey()) {
			return cb(object)
		}
		return true
	})
}

func (d *noExpiredDecoder) Parse(cb func(object model.RedisObject) bool) error {
	now := time.Now()
	return d.dec.Parse(func(object model.RedisObject) bool {
		expiration := object.GetExpiration()
		if expiration == nil || expiration.After(now) {
			return cb(object)
		}
		return true
	})
}

type decoder interface {
	Parse(cb func(object model.RedisObject) bool) error
}

// noExpiredDecoder filter all expired keys
type noExpiredDecoder struct {
	dec decoder
}

// regexWrapper returns
func regexWrapper(d decoder, expr string) (*regexDecoder, error) {
	reg, err := regexp.Compile(expr)
	if err != nil {
		return nil, fmt.Errorf("illegal regex expression: %v", expr)
	}
	return &regexDecoder{
		dec: d,
		reg: reg,
	}, nil
}

func wrapDecoder(dec decoder, options ...interface{}) (decoder, error) {
	var regexOpt helper.RegexOption
	var noExpiredOpt helper.NoExpiredOption
	for _, opt := range options {
		switch o := opt.(type) {
		case helper.RegexOption:
			regexOpt = o
		case helper.NoExpiredOption:
			noExpiredOpt = o
		}
	}
	if regexOpt != nil {
		var err error
		dec, err = regexWrapper(dec, *regexOpt)
		if err != nil {
			return nil, err
		}
	}
	if noExpiredOpt {
		dec = &noExpiredDecoder{
			dec: dec,
		}
	}
	return dec, nil
}

func (this *RedisBigKeys) printNodes() {
	t := termtables.CreateTable()
	t.AddHeaders("Master节点", "Slave节点")
	maxSize := len(this.masters)
	if len(this.masters) < len(this.slaves) {
		maxSize = len(this.slaves)
	}
	for i := 0; i < maxSize; i++ {
		col1 := ""
		col2 := ""
		if i+1 <= len(this.masters) {
			col1 = this.masters[i]
		}
		if i+1 <= len(this.slaves) {
			col2 = this.slaves[i]
		}
		t.AddRow(col1, col2)
	}
	fmt.Println(t.Render())
}

func (this *RedisBigKeys) connect() error {
	fmt.Printf("「连接」- 以单机模式连接至Redis：%s...\n", this.HostPort)
	rdb := redis.NewClient(&redis.Options{
		Addr:     this.HostPort,
		Password: this.Password,
	})
	defer func(rdb *redis.Client) {
		err := rdb.Close()
		if err != nil {
			fmt.Printf("「连接」- 关闭连接异常，忽略: %v\n", err)
		}
	}(rdb)
	infoStr := rdb.Info(this.Ctx).String()
	if strings.Contains(infoStr, "ERR invalid password") {
		return fmt.Errorf("「连接」- Redis登录失败：密码错误 ")
	} else {
		fmt.Println("「连接」- Redis登录成功.")
	}
	scanner := bufio.NewScanner(strings.NewReader(infoStr))
	info := make(map[string]string)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, ":") {
			lineArr := strings.Split(line, ":")
			info[lineArr[0]] = lineArr[1]
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Printf("error occurred: %v\n", err)
	}
	this.info = info

	var masters []string
	var slaves []string

	if info["redis_mode"] == "standalone" {
		fmt.Println("「连接」- 检测到Redis为 单机/哨兵模式...")
		if info["role"] == "master" {
			masters = append(masters, this.HostPort)
		} else {
			slaves = append(slaves, this.HostPort)
		}
	} else if info["redis_mode"] == "cluster" {
		fmt.Println("「连接」- 检测到集群为 集群模式...")
		fmt.Println("「连接」- 以集群模式重连Redis...")
		rdb2 := redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:    []string{this.HostPort},
			Password: this.Password, // 没有密码，默认值
		})
		defer func(rdb *redis.ClusterClient) {
			err := rdb.Close()
			if err != nil {
				fmt.Printf("「连接」- 关闭连接异常，忽略: %v\n", err)
			}
		}(rdb2)
		_ = rdb2.ForEachMaster(this.Ctx, func(ctx context.Context, shard *redis.Client) error {
			masters = append(masters, shard.Options().Addr)
			return shard.Ping(ctx).Err()
		})
		_ = rdb2.ForEachSlave(this.Ctx, func(ctx context.Context, shard *redis.Client) error {
			slaves = append(slaves, shard.Options().Addr)
			return shard.Ping(ctx).Err()
		})
	}
	this.masters = masters
	this.slaves = slaves
	fmt.Println("「连接」- 已扫描到的节点如下：")
	this.printNodes()
	return nil
}

func (this *RedisBigKeys) mkTmpDir() error {
	this.tmpDir = fmt.Sprintf("%s/redis-tools-%s", this.WorkDir, uuid.New())
	fmt.Printf("「准备」- 创建工作目录：%s...\n", this.tmpDir)
	err := os.Mkdir(this.tmpDir, 0755)
	if err != nil {
		return fmt.Errorf("「准备」- 创建临时目录失败：%s", err)
	}
	return nil
}

func (this *RedisBigKeys) dump() error {
	if this.UseMaster && len(this.masters) == 0 {
		return fmt.Errorf("「导出」- 用户选择使用Master节点进行分析，但没有可用的Master节点")
	}
	if !this.UseMaster && len(this.slaves) == 0 {
		return fmt.Errorf("「导出」- 用户选择使用Slave节点进行分析，但没有可用的Slave节点")
	}
	var nodes []string
	var rdbs []string
	if this.UseMaster {
		fmt.Println("「导出」- 使用Master节点进行分析...")
		nodes = this.masters
	} else {
		fmt.Println("「导出」- 使用Slave节点进行分析...")
		nodes = this.slaves
	}
	for i := range nodes {
		node := nodes[i]
		fmt.Printf("「导出」- 连接至：%s 以生成RDB文件...\n", node)
		nodeArr := strings.Split(node, ":")
		host := nodeArr[0]
		port := nodeArr[1]
		rdbPath := fmt.Sprintf("%s/redis-dump-%s.rdb", this.tmpDir, node)
		cmd := exec.Command("redis-cli", "-h", host, "-p", port, "-a", this.Password, "--no-auth-warning", "--rdb", rdbPath)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		err := cmd.Run()
		if err != nil {
			fmt.Printf("「导出」- 生成失败: %v，跳过.\n", err)
			continue
		}
		rdbs = append(rdbs, rdbPath)
	}
	if len(rdbs) == 0 {
		return fmt.Errorf("「导出」- 没有获取到任何rdb文件")
	}
	this.rdbs = rdbs
	return nil
}

func (this *RedisBigKeys) analyzeRDB(rdbPath string, options ...interface{}) error {
	fmt.Printf("「分析」- 开始分析RDB文件：%s...\n", path.Base(rdbPath))
	if rdbPath == "" {
		return errors.New("「分析」- RDB文件路径为空")
	}
	rdbFile, err := os.Open(rdbPath)
	if err != nil {
		return fmt.Errorf("「分析」- 打开RDB文件 %s 失败, %v", rdbPath, err)
	}
	defer func() {
		_ = rdbFile.Close()
	}()
	var dec decoder = core.NewDecoder(rdbFile)
	if dec, err = wrapDecoder(dec, options...); err != nil {
		return err
	}
	err = dec.Parse(func(object model.RedisObject) bool {
		this.topList.appendObjectToHeap(object)
		return true
	})
	if err != nil {
		return err
	}
	return nil
}

func (this *RedisBigKeys) analyze(options ...interface{}) error {
	this.topList = newRedisHeap(this.NumOfResult)
	//this.rdbs = []string{"D:\\662485\\Downloads\\redis-dump-3d196c09-eaf9-4070-88bf-6241a483b342.rdb"}
	for _, rdb := range this.rdbs {
		err := this.analyzeRDB(rdb, options...)
		if err != nil {
			return err
		}
	}
	return nil
}

func (this *RedisBigKeys) report() {
	fmt.Printf("\n# 扫描结果\n")
	t := termtables.CreateTable()
	t.AddHeaders("Key名称", "类型", "大小", "大小(K/M/GB)", "元素个数", "数据库")
	iter := this.topList.set.Iterator()
	for iter.Next() {
		object := iter.Value().(model.RedisObject)
		t.AddRow(object.GetKey(),
			strings.ToTitle(object.GetType()),
			strconv.Itoa(object.GetSize()),
			bytefmt.FormatSize(uint64(object.GetSize())),
			strconv.Itoa(object.GetElemCount()),
			strconv.Itoa(object.GetDBIndex()))
	}
	fmt.Println(t.Render())
}

func (this *RedisBigKeys) clean() {
	if this.NoDelete {
		fmt.Println("「清理」- 用户要求保留临时目录.")
		return
	}
	_, err := os.Stat(this.tmpDir)
	if err != nil {
		fmt.Printf("「清理」- 临时目录：%s 已不存在.\n", this.tmpDir)
		return
	}
	fmt.Printf("「清理」- 删除临时目录：%s...\n", this.tmpDir)
	err = os.RemoveAll(this.tmpDir)
	if err != nil {
		fmt.Printf("「清理」- 清理临时目录失败, %s", err)
		return
	}
}

func (this *RedisBigKeys) connectToGetDump() {
	var err error
	err = this.connect()
	if err != nil {
		fmt.Printf("连接Redis失败：%s.\n", err)
		return
	}
	err = this.dump()
	if err != nil {
		fmt.Printf("生成RDB文件失败：%s.\n", err)
		return
	}
}

func (this *RedisBigKeys) Run(options ...interface{}) {
	var err error
	if this.RdbFile != "" {
		var rdbs []string
		rdbs = append(rdbs, this.RdbFile)
		this.rdbs = rdbs
	} else {
		err := this.mkTmpDir()
		if err != nil {
			return
		}
		this.connectToGetDump()
		defer func() {
			this.clean()
		}()
	}
	err = this.analyze(options...)
	if err != nil {
		fmt.Printf("分析RDB失败：%s.\n", err)
		return
	}
	this.report()
}
