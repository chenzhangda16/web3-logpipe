package ingest

import "sync"

// 复用 []byte，避免每条消息 make 造成 GC 压力
var msgBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 256*1024) // 初始 cap：按你 block JSON 常见大小估一下
		return &b
	},
}

func getMsgBuf(n int) []byte {
	p := msgBufPool.Get().(*[]byte)
	b := *p
	if cap(b) < n {
		b = make([]byte, n)
	} else {
		b = b[:n]
	}
	*p = b
	return b
}

func putMsgBuf(b []byte) {
	// 防止极端大消息把池子撑爆（可按你实际消息大小调）
	if cap(b) > 2*1024*1024 {
		return
	}
	msgBufPool.Put(&b)
}
