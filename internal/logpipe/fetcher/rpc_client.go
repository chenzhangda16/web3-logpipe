package fetcher

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/chenzhangda16/web3-logpipe/internal/mockchain/model"
)

type RPCClient struct {
	base string
	hc   *http.Client
}

type BlocksRangeResp struct {
	From    int64         `json:"from"`
	To      int64         `json:"to"`
	Blocks  []model.Block `json:"blocks"`
	Partial bool          `json:"partial"`
	LastOK  int64         `json:"last_ok"`
}

func NewRPCClient(base string) *RPCClient {
	base = strings.TrimRight(base, "/")
	return &RPCClient{
		base: base,
		hc: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

type HeadResp struct {
	HeadHash string `json:"head_hash"`
	HeadNum  int64  `json:"head_num"`
	Empty    bool   `json:"empty"`
}

type ChainHeadResp struct {
	HeadNum       int64  `json:"head_num"`
	HeadHash      string `json:"head_hash"`
	HeadTimestamp int64  `json:"head_timestamp"`
	Empty         bool   `json:"empty"`
}

type AtOrAfterResp struct {
	BlockNum       int64 `json:"block_num"`
	BlockTimestamp int64 `json:"block_timestamp"`
	// block field exists too but we don't need it for positioning
}

func (c *RPCClient) getJSON(ctx context.Context, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+path, nil)
	if err != nil {
		return err
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return fmt.Errorf("rpc %s status=%d", path, resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func (c *RPCClient) ChainHead(ctx context.Context) (ChainHeadResp, error) {
	var out ChainHeadResp
	err := c.getJSON(ctx, "/chain/head", &out)
	return out, err
}

func (c *RPCClient) BlockAtOrAfter(ctx context.Context, ts int64) (AtOrAfterResp, error) {
	var out AtOrAfterResp
	q := url.Values{}
	q.Set("ts", strconv.FormatInt(ts, 10))
	err := c.getJSON(ctx, "/block/at-or-after?"+q.Encode(), &out)
	return out, err
}

func (c *RPCClient) BlocksRange(ctx context.Context, from, to int64) (BlocksRangeResp, error) {
	var out BlocksRangeResp

	q := url.Values{}
	q.Set("from", strconv.FormatInt(from, 10))
	q.Set("to", strconv.FormatInt(to, 10))

	err := c.getJSON(ctx, "/blocks/range?"+q.Encode(), &out)
	return out, err
}

func (c *RPCClient) BlockByNumber(ctx context.Context, n int64) (model.Block, error) {
	var blk model.Block
	err := c.getJSON(ctx, "/block/by-number/"+strconv.FormatInt(n, 10), &blk)
	return blk, err
}
