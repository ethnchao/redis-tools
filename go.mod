module redis-tools

go 1.21.0

require (
	github.com/bytedance/sonic v1.8.7
	github.com/emirpasic/gods v1.18.1
	github.com/google/uuid v1.3.1
	github.com/hdt3213/rdb v1.0.16
	github.com/lithammer/shortuuid/v4 v4.0.0
	github.com/redis/go-redis/v9 v9.1.0
	github.com/scylladb/termtables v0.0.0-20191203121021-c4c0b6d42ff4
)

require (
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/chenzhuoyu/base64x v0.0.0-20221115062448-fe3a3abad311 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/klauspost/cpuid/v2 v2.0.9 // indirect
	github.com/mattn/go-runewidth v0.0.15 // indirect
	github.com/rivo/uniseg v0.2.0 // indirect
	github.com/twitchyliquid64/golang-asm v0.15.1 // indirect
	golang.org/x/arch v0.0.0-20210923205945-b76863e36670 // indirect
)

replace github.com/hdt3213/rdb => github.com/ethnchao/rdb v1.0.17
