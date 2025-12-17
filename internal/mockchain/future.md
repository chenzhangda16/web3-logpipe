太爽了，你现在已经开始给自己造“测试网 + 假黑产”了
好，那我们就顺着你的思路来：造一条假链 + 能产出带环的交易图 + 暴露 RPC 给 fetcher，当作 LogPipe 的 in。

我先给出整体架构，再给你一版可以直接跑的 Go 骨架代码。

---

1. 总体架构：我们多加一个“MockChain 节点”

先把系统拉平一下：

```
text
[MockChain 节点]  ──RPC──►  [LogFetcher] ─► [Scheduler/Shard] ─► [Workers] ─► [Writer(DB)]
(造假链 + 造环图)             (拉取 in)

```

这次你的需求是把左边这块补上：

- 负责 随机生成交易图（含环）
- 按“区块”组织
- 对外提供一个简单 RPC（HTTP/JSON 就够了，后面你想换 gRPC 再上）
---

2. 假链的数据模型设计

我们先用极简的数据结构表示一个“假区块”和“假交易”：

```go
type Tx struct {
    Hash      string `json:"hash"`
    From      string `json:"from"`
    To        string `json:"to"`
    Token     string `json:"token"`
    Amount    int64  `json:"amount"`
    Timestamp int64  `json:"timestamp"`
    BlockNum  int64  `json:"block_num"`
}


type Block struct {
    Number     int64 `json:"number"`
    Timestamp  int64 `json:"timestamp"`
    Txs        []Tx  `json:"txs"`
}

```

地址、Token 都用字符串，没必要一开始搞太重的结构。

---

3. 如何生成“随机环图”？（带业务味的简化版）

我们搞三类交易，强行往现实靠：

1. 随机转账（正常流）
    - A → B，随机地址，随机 Token+Amount
2. 短环自转（高可疑行为）
    - 长度 2 环：A → B → A
    - 长度 3 环：A → B → C → A
    - 这些环我们可以反复利用，模拟“短周期大额反复进出”
3. 小热点团伙（局部强连通子图）
    - 一组地址互相乱转（Tarjan 未来可以用来抓 SCC）

一个简单生成策略：

- 先预生成一批地址 addr[0..N-1]
- 再预生成几组“环地址”：
    - group1 = [A,B]
    - group2 = [C,D,E]
- 每出一个新块：
    - 80% 交易是随机 A→B
    - 20% 交易拿这些环地址制造 A→B→A / A→B→C→A 这类 pattern

这样：

- 你确定图里一定有短环
- 且环会多次被使用，模拟“短周期反复进出”
---

4. MockChain 节点：简单 HTTP RPC 设计

为了让 fetcher 好写，我们先来个最简单的接口：

- GET /block/next
    - 每调用一次，返回“下一块”（内部自增 blockNum）
    - 没有区块了就等 100ms 再生成一块（或者直接每次现造）

等价于：

fetcher 轮询 GET /block/next，把返回的 Block.Txs 当作 in。
4.1 MockChain 核心结构

```go
type MockChain struct {
    mu        sync.Mutex
    nextBlock int64
    addrs     []string
    loopPairs [][2]int // 用于 2 环 (A,B)
    loopTriples [][3]int // 用于 3 环 (A,B,C)
}

```

初始化时：

- 生成一批随机地址（其实可以用 hex 编出来）
- 选几对/三元组做环地址

4.2 生成下一块的函数

```go
func (mc *MockChain) NextBlock() Block {
    mc.mu.Lock()
    defer mc.mu.Unlock()


    blockNum := mc.nextBlock
    mc.nextBlock++


    now := time.Now().Unix()
    var txs []Tx


    // 每块随便来个 50～100 笔交易
    n := 50 + rand.Intn(50)


    for i := 0; i < n; i++ {
        p := rand.Float64()
        switch {
        case p < 0.2:
            // 20% 环交易
            txs = append(txs, mc.makeLoopTx(blockNum, now)...)
        default:
            // 80% 普通随机转账
            txs = append(txs, mc.makeRandomTx(blockNum, now))
        }
    }


    return Block{
        Number:    blockNum,
        Timestamp: now,
        Txs:       txs,
    }
}

```

4.3 环交易生成（2 环 + 3 环简易实现）

```go
func (mc *MockChain) makeRandomTx(blockNum, ts int64) Tx {
    fromIdx := rand.Intn(len(mc.addrs))
    toIdx := rand.Intn(len(mc.addrs))
    for toIdx == fromIdx {
        toIdx = rand.Intn(len(mc.addrs))
    }
    token := "TOKEN-" + strconv.Itoa(rand.Intn(5)) // 5 种 Token
    amount := int64(1+rand.Intn(1000)) * 1e6      // 随便搞个金额


    return Tx{
        Hash:      randomHash(),
        From:      mc.addrs[fromIdx],
        To:        mc.addrs[toIdx],
        Token:     token,
        Amount:    amount,
        Timestamp: ts,
        BlockNum:  blockNum,
    }
}


func (mc *MockChain) makeLoopTx(blockNum, ts int64) []Tx {
    // 随机从 2 环或者 3 环组中选一组
    if len(mc.loopPairs) == 0 && len(mc.loopTriples) == 0 {
        return []Tx{mc.makeRandomTx(blockNum, ts)} // fallback
    }


    if len(mc.loopPairs) > 0 && (len(mc.loopTriples) == 0 || rand.Float64() < 0.6) {
        // 用 2 环 A<->B
        pair := mc.loopPairs[rand.Intn(len(mc.loopPairs))]
        a, b := mc.addrs[pair[0]], mc.addrs[pair[1]]
        token := "TOKEN-LOOP"
        amount := int64(10+rand.Intn(100)) * 1e8


        return []Tx{
            {
                Hash:      randomHash(),
                From:      a,
                To:        b,
                Token:     token,
                Amount:    amount,
                Timestamp: ts,
                BlockNum:  blockNum,
            },
            {
                Hash:      randomHash(),
                From:      b,
                To:        a,
                Token:     token,
                Amount:    amount,
                Timestamp: ts,
                BlockNum:  blockNum,
            },
        }
    }


    // 否则用 3 环 A->B->C->A
    triple := mc.loopTriples[rand.Intn(len(mc.loopTriples))]
    a, b, c := mc.addrs[triple[0]], mc.addrs[triple[1]], mc.addrs[triple[2]]
    token := "TOKEN-LOOP3"
    amount := int64(20+rand.Intn(100)) * 1e8


    return []Tx{
        {Hash: randomHash(), From: a, To: b, Token: token, Amount: amount, Timestamp: ts, BlockNum: blockNum},
        {Hash: randomHash(), From: b, To: c, Token: token, Amount: amount, Timestamp: ts, BlockNum: blockNum},
        {Hash: randomHash(), From: c, To: a, Token: token, Amount: amount, Timestamp: ts, BlockNum: blockNum},
    }
}

```

4.4 HTTP handler：GET /block/next

```go
func main() {
    rand.Seed(time.Now().UnixNano())


    mc := NewMockChain(1000) // 比如 1000 个地址


    mux := http.NewServeMux()
    mux.HandleFunc("/block/next", func(w http.ResponseWriter, r *http.Request) {
        blk := mc.NextBlock()
        w.Header().Set("Content-Type", "application/json")
        _ = json.NewEncoder(w).Encode(blk)
    })


    srv := &http.Server{
        Addr:    ":8080",
        Handler: mux,
    }


    log.Println("MockChain listening on :8080")
    if err := srv.ListenAndServe(); err != nil {
        log.Fatal(err)
    }
}

```

NewMockChain 里把 addrs / loopPairs / loopTriples 初始好就行了（比如随机从 [0..N-1] 里采样）。

---

5. Fetcher 怎么用这个 RPC 生成 in？

在你的 LogPipe 侧（真正日志系统），只需要一个简单的 fetcher goroutine：

```go
type RawBlock struct {
    Number    int64 `json:"number"`
    Timestamp int64 `json:"timestamp"`
    Txs       []Tx  `json:"txs"`
}


func StartFetchLoop(ctx context.Context, out chan<- Tx) {
    client := &http.Client{Timeout: 5 * time.Second}


    go func() {
        defer close(out)
        for {
            select {
            case <-ctx.Done():
                return
            default:
            }


            resp, err := client.Get("http://localhost:8080/block/next")
            if err != nil {
                log.Printf("fetch error: %v", err)
                time.Sleep(time.Second)
                continue
            }


            var blk RawBlock
            if err := json.NewDecoder(resp.Body).Decode(&blk); err != nil {
                log.Printf("decode error: %v", err)
                _ = resp.Body.Close()
                time.Sleep(time.Second)
                continue
            }
            _ = resp.Body.Close()


            // 把区块里的 tx 按顺序塞入 out channel
            for _, tx := range blk.Txs {
                select {
                case <-ctx.Done():
                    return
                case out <- tx:
                }
            }
        }
    }()
}

```

然后在你的 LogPipe 主流程里：

```go
func main() {
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()


    rawTxCh := make(chan Tx, 1024)


    // 启动 fetcher
    StartFetchLoop(ctx, rawTxCh)


    // 这里接 rawTxCh，丢给 Scheduler 分 shard，再丢给 worker pool
    // ...
}

```

这样：

- mockchain 节点每次调用 /block/next 就“造一块带环交易的假块”
- fetcher 从 HTTP 拉 block → 展平为单笔 Tx → 当作 in 进入你的并发日志处理 pipeline
---

6. 你现在已经有了：

1. 一个“假的 L1 节点”：MockChain
    - 会产生有短环的交易图
    - 会产生短周期反复进出（通过重复利用环地址）
2. 一个“LogPipe 的输入源”：Fetcher
    - 把区块流转化为 Tx 流
    - 后面可以直接接：Scheduler / Shard Worker / Writer / Graph-Analyzer

等你把 LogPipe v1–v2 跑起来，再往里塞：

- 大额短周期检测
- 自转环检测（短环）
- Tarjan / SCC
- misbehavior 评分

你就真的是在本地跑一个 mini-Chainalysis + mini-indexer 了。

---
如果你愿意，下一步我可以帮你：

- 把 NewMockChain 具体写完（生成地址 + 环地址采样）
- 设计 LogPipe 的 Scheduler + Shard Worker 接口，把这个 Tx 流接下去
- 或者直接一起定义整个项目的目录结构（cmd/mockchain、cmd/logpipe, internal/mockchain, internal/logpipe），变成一个可长期演进的 repo。