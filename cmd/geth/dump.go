package main

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/log"
)

type Writer struct {
	encoder  *json.Encoder
	accounts chan *state.DumpAccount
	roots    chan common.Hash
	wg       sync.WaitGroup
}

// OnRoot implements DumpCollector interface
func (d *Writer) OnRoot(root common.Hash) {
	d.roots <- root
}

// OnAccount implements DumpCollector interface
func (d *Writer) OnAccount(account *state.DumpAccount) {
	d.accounts <- account
}

// OnRoot implements DumpCollector interface
func (d *Writer) onRoot(root common.Hash) {
	d.encoder.Encode(struct {
		Root common.Hash `json:"root"`
	}{root})
}

// OnAccount implements DumpCollector interface
func (d *Writer) onAccount(account state.DumpAccount) {
	d.encoder.Encode(account)
}

func (d *Writer) Run(ctx context.Context) {
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		for {
			select {
			case <-ctx.Done():
				{
					log.Info("writer exit by done")
					return
				}
			case acc := <-d.accounts:
				{
					d.onAccount(*acc)
				}
			case root := <-d.roots:
				{
					d.onRoot(root)
				}
			}
		}
	}()
}

func (d *Writer) Wait() {
	d.wg.Wait()
}
