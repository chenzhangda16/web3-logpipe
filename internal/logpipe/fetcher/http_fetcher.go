package fetcher

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/chenzhangda16/web3-logpipe/internal/logpipe/model"
)

type RawBlock struct {
	Number    int64      `json:"number"`
	Timestamp int64      `json:"timestamp"`
	Txs       []model.Tx `json:"txs"`
}

type HTTPFetcher struct {
	client *http.Client
	url    string
}

func NewHTTPFetcher(url string) *HTTPFetcher {
	return &HTTPFetcher{
		client: &http.Client{Timeout: 5 * time.Second},
		url:    url,
	}
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

func (f *HTTPFetcher) fetchBlock() (RawBlock, error) {
	resp, err := f.client.Get(f.url + "/block/next")
	if err != nil {
		return RawBlock{}, err
	}
	defer resp.Body.Close()

	var blk RawBlock
	if err := json.NewDecoder(resp.Body).Decode(&blk); err != nil {
		return RawBlock{}, err
	}
	return blk, nil
}
