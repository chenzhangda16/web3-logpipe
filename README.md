好，开始“正儿八经开项目”了
我们先定一个可以长期演进、又不会一上来就太重的 Go 项目结构，把：
- 假链节点（MockChain）
- 多线程日志处理器（LogPipe）

放在同一个 repo 里。

下面我给你一个完整的目录树 + 每一块的职责说明，你照着 mktree 一键生成就行。

---

一、整体风格

- 单一 Go module：比如 github.com/chenchangda/web3-logpipe
- 两个可执行程序：
    - cmd/mockchain：造假链 + 暴露 HTTP RPC
    - cmd/logpipe：从 mockchain 拉流、多线程处理、写出结果
- 核心逻辑放在 internal/ 下，避免乱引用
- 日志、配置、脚本单独放
---

二、目录结构（建议版）

```
text
web3-logpipe/
├── go.mod
├── go.sum
├── README.md
├── cmd/
│   ├── mockchain/
│   │   └── main.go          # 启动“假链节点”HTTP RPC 服务
│   └── logpipe/
│       └── main.go          # 启动多线程日志处理器（LogPipe）
│
├── internal/
│   ├── mockchain/
│   │   ├── chain.go         # MockChain 结构体、NextBlock() 等
│   │   ├── generator.go     # 地址池、环图生成逻辑
│   │   └── http_server.go   # /block/next HTTP handler
│   │
│   └── logpipe/
│       ├── model/
│       │   ├── tx.go        # Tx、Block、EnrichedEvent 等基础结构
│       │   └── flags.go     # 风险标记/行为标签枚举（WHale, Loop, etc）
│       │
│       ├── fetcher/
│       │   └── http_fetcher.go  # 从 mockchain 拉 block → 展开成 Tx 流
│       │
│       ├── scheduler/
│       │   └── scheduler.go     # Tx → shardID 的调度逻辑 (按地址/池分片)
│       │
│       ├── worker/
│       │   ├── worker.go        # 单个 shard worker 的处理循环
│       │   └── pipeline.go      # per-Tx 富化管线（规则调用、打标签）
│       │
│       ├── writer/
│       │   ├── writer.go        # 从 shards 收集结果，批量写出
│       │   └── postgres.go      # v1 先做 stub，后面再接 PostgreSQL/文件
│       │
│       ├── rules/
│       │   ├── loop_detect.go   # 短环 / 自转 检测（先写简单版）
│       │   ├── volume_window.go # 大额 & 短周期进出检测（滑动窗口）
│       │   └── score.go         # 行为评分（RiskScore）
│       │
│       ├── state/
│       │   ├── inmem_state.go   # 每 shard 的内存状态（per address / pool）
│       │   └── wal.go           # v2/v3: WAL + checkpoint（先留空壳）
│       │
│       ├── config/
│       │   └── config.go        # 配置结构体、加载（env / flag）
│       │
│       ├── metrics/
│       │   └── metrics.go       # 内部指标（TPS、延迟、队列长度）
│       │
│       └── app/
│           └── app.go           # 把 fetcher + scheduler + workers + writer 串起来
│
├── pkg/
│   └── logx/
│       └── logx.go              # 简单日志封装（带组件名前缀）
│
├── configs/
│   ├── logpipe.dev.yaml         # 开发环境配置
│   └── logpipe.local.yaml
│
├── scripts/
│   ├── run_mockchain.sh         # 启动 mockchain
│   ├── run_logpipe.sh           # 启动 logpipe
│   └── bench_logpipe.sh         # 简单压测脚本（后面写）
│
├── testdata/
│   ├── sample_block.json        # 预录制 block / tx 供单测用
│   └── sample_events.json
│
└── internal_tests/
    ├── logpipe_concurrency_test.go # go test -race 的并发压力测试
    └── rules_test.go               # 行为检测规则的单测

```

你可以直接用 mktree 生成这一棵树（先忽略空文件），然后我们一层一层填。

---

三、每个关键子模块大致干什么？

我按你未来写代码的顺序来排。


1. cmd/mockchain/

- 读配置（端口、地址数量、每块交易数范围）
- 初始化 MockChain
- ListenAndServe(":8080")
  这是你本地“假 L1 节点”。
---

2. internal/mockchain/

- chain.go：定义 MockChain 结构体：
- generator.go：生成地址池 + 环地址组 + NextBlock()
- http_server.go：/block/next handler（call NextBlock()）
---

3. internal/logpipe/model/

统一你的数据结构，不要到处自己定义：

```go
type Tx struct {
    Hash      string
    From      string
    To        string
    Token     string
    Amount    int64
    Timestamp int64
    BlockNum  int64
}


type EnrichedEvent struct {
    Tx


    Whale        bool
    SelfLoop     bool
    ShortCycle   bool
    HighFreqIO   bool
    RiskScore    float64
    // 未来可加更多字段
}

```

所有 pipeline 模块 / 规则模块，用的都是这些 struct。
---

4. internal/logpipe/fetcher/

http_fetcher.go：

- 负责访问 mockchain 暴露的 /block/next
- 解码为 Block，展开为 Tx，放进 chan Tx
- 只关心 “把 HTTP 拉流 → Tx 流” 这一步
---

5. internal/logpipe/scheduler/

核心调度逻辑：Tx → shardID

```go
type Scheduler struct {
    shardCount int
    outChans   []chan model.Tx
}


func (s *Scheduler) Start(ctx context.Context, in <-chan model.Tx) { ... }


func (s *Scheduler) shardForTx(tx model.Tx) int {
    // 比如按地址或 Token 做 hash
}

```

保证同一个地址/池的 Tx 落在同一个 shard，
这样 shard 内可以保证局部顺序处理。
---

6. internal/logpipe/worker/

- worker.go：一个 shard 的 goroutine 循环：
- pipeline.go：对单个 Tx 的处理 pipeline：
---

7. internal/logpipe/state/

- ShardState：一个 shard 本地的“记忆”：
- wal.go：以后 v2/v3 再填（先可以做个空架子 + TODO），不耽误你先把多线程跑起来。
---

8. internal/logpipe/rules/

存纯“业务规则”逻辑，不带 goroutine：

- loop_detect.go：用 shard 内小图检测：A→B→A / A→B→C→A
- volume_window.go：滑动窗口 + 大额短周期进出判定
- score.go：把各种特征变成一个 RiskScore
  逻辑一律写成纯函数，方便单测，非常干净。
---

9. internal/logpipe/writer/

- writer.go：从所有 shard 收集 EnrichedEvent，可以先写：
    - 控制台打印
    - 或写到本地 JSONL 文件
    - 之后再接 PostgreSQL 或 RocksDB
- postgres.go：可以先留个 stub，提前想好接口：
---

10. internal/logpipe/app/

这里是“总导演”，把一切串起来：

```go
type App struct {
    cfg       config.Config
    fetcher   *fetcher.HTTPFetcher
    scheduler *scheduler.Scheduler
    workers   []*worker.Worker
    writer    *writer.BatchWriter
}


func (a *App) RunBackup(ctx context.Context) error { ... }

```

cmd/logpipe/main.go 只做三件事：

1. 解析参数 / 读取配置
2. 初始化 App
3. 调 App.RunBackup(ctx)
   你以后要加 metrics、graceful shutdown、debug HTTP，一律丢 App 里做。
---

三、下一步建议

你可以像这样推进：

1. 用 mktree 把上面这棵树先撸出来（空文件无所谓）
2. 先把：
    - internal/mockchain 搞定（MockChain + /block/next）
    - cmd/mockchain/main.go 写完，能跑
3. 然后在 internal/logpipe 里：
    - 先做最小 pipeline：
      fetcher → scheduler → 1 个 worker → writer（直接打印）
    - 确认 goroutine + channel 逻辑通顺
4. 再逐步加：
    - shard 多 worker
    - rules 的简单版本（比如只做大额检测 + 自转 A→A）
    - metrics + race 测试

你如果愿意，下一步我可以直接帮你把：

- NewMockChain + chain.go 的骨架代码写完整
- 再给一个最小可运行的 cmd/mockchain/main.go + cmd/logpipe/main.go 雏形

你就可以在本地 go run ./cmd/mockchain、再 go run ./cmd/logpipe 看 Tx 流动了。