// Copyright 2015 The go-ethereum Authors
// This file is part of go-ethereum.
//
// go-ethereum is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// go-ethereum is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with go-ethereum. If not, see <http://www.gnu.org/licenses/>.

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/cmd/utils"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/urfave/cli/v2"

	"github.com/fyInALT/eth-tools/state"
)

var (
	JSONFileFlag = &cli.StringFlag{
		Name:  "file",
		Usage: `File to store the dump json`,
		Value: "./dump.json",
	}
)

var (
	dumpGenesisCommand = &cli.Command{
		Action:    dumpGenesis,
		Name:      "dumpgenesis",
		Usage:     "Dumps genesis block JSON configuration to stdout",
		ArgsUsage: "",
		Flags:     append([]cli.Flag{utils.DataDirFlag}, utils.NetworkFlags...),
		Description: `
The dumpgenesis command prints the genesis configuration of the network preset
if one is set.  Otherwise it prints the genesis from the datadir.`,
	}

	dumpCommand = &cli.Command{
		Action:    dump,
		Name:      "dump",
		Usage:     "Dump a specific block from storage",
		ArgsUsage: "[? <blockHash> | <blockNum>]",
		Flags: slices.Concat([]cli.Flag{
			JSONFileFlag,
			utils.LogDebugFlag,
			utils.GCModeFlag,
			utils.CryptoKZGFlag,
			utils.CacheFlag,
			utils.IterativeOutputFlag,
			utils.ExcludeCodeFlag,
			utils.ExcludeStorageFlag,
			utils.IncludeIncompletesFlag,
			utils.StartKeyFlag,
			utils.DumpLimitFlag,
		}, utils.DatabaseFlags),
		Description: `
This command dumps out the state for a given block (or latest, if none provided).
`,
	}
)

func dumpGenesis(ctx *cli.Context) error {
	// dump whatever already exists in the datadir
	stack, _ := makeConfigNode(ctx)

	db, err := stack.OpenDatabase("chaindata", 0, 0, "", true)
	if err != nil {
		return err
	}
	defer db.Close()

	genesis, err := core.ReadGenesis(db)
	if err != nil {
		utils.Fatalf("failed to read genesis: %s", err)
	}

	if err := json.NewEncoder(os.Stdout).Encode(*genesis); err != nil {
		utils.Fatalf("could not encode stored genesis: %s", err)
	}

	return nil
}

func parseDumpConfig(ctx *cli.Context, db ethdb.Database) (*state.DumpConfig, common.Hash, error) {
	var header *types.Header
	if ctx.NArg() > 1 {
		return nil, common.Hash{}, fmt.Errorf("expected 1 argument (number or hash), got %d", ctx.NArg())
	}
	if ctx.NArg() == 1 {
		arg := ctx.Args().First()
		if hashish(arg) {
			hash := common.HexToHash(arg)
			if number := rawdb.ReadHeaderNumber(db, hash); number != nil {
				header = rawdb.ReadHeader(db, hash, *number)
			} else {
				return nil, common.Hash{}, fmt.Errorf("block %x not found", hash)
			}
		} else {
			number, err := strconv.ParseUint(arg, 10, 64)
			if err != nil {
				return nil, common.Hash{}, err
			}
			if hash := rawdb.ReadCanonicalHash(db, number); hash != (common.Hash{}) {
				header = rawdb.ReadHeader(db, hash, number)
			} else {
				return nil, common.Hash{}, fmt.Errorf("header for block %d not found", number)
			}
		}
	} else {
		// Use latest
		header = rawdb.ReadHeadHeader(db)
	}
	if header == nil {
		return nil, common.Hash{}, errors.New("no head block found")
	}
	startArg := common.FromHex(ctx.String(utils.StartKeyFlag.Name))
	var start common.Hash
	switch len(startArg) {
	case 0: // common.Hash
	case 32:
		start = common.BytesToHash(startArg)
	case 20:
		start = crypto.Keccak256Hash(startArg)
		log.Info("Converting start-address to hash", "address", common.BytesToAddress(startArg), "hash", start.Hex())
	default:
		return nil, common.Hash{}, fmt.Errorf("invalid start argument: %x. 20 or 32 hex-encoded bytes required", startArg)
	}
	conf := &state.DumpConfig{
		SkipCode:          ctx.Bool(utils.ExcludeCodeFlag.Name),
		SkipStorage:       ctx.Bool(utils.ExcludeStorageFlag.Name),
		OnlyWithAddresses: !ctx.Bool(utils.IncludeIncompletesFlag.Name),
		Start:             start.Bytes(),
		Max:               ctx.Uint64(utils.DumpLimitFlag.Name),
	}
	log.Info("State dump configured", "block", header.Number, "hash", header.Hash().Hex(),
		"skipcode", conf.SkipCode, "skipstorage", conf.SkipStorage,
		"start", hexutil.Encode(conf.Start), "limit", conf.Max)
	return conf, header.Root, nil
}

// Dump represents the full dump in a collected format, as one large map.
type DumpData struct {
	writer        *Writer
	startTime     time.Time
	lastTime      time.Time
	per10000count int64
	count         int64
	// Next can be set to represent that this dump is only partial, and Next
	// is where an iterator should be positioned in order to continue the dump.
	Next []byte `json:"next,omitempty"` // nil if no more accounts
}

// OnRoot implements DumpCollector interface
func (d *DumpData) OnRoot(root common.Hash) {
	log.Info("had dump root", "root", root.Hex())

	d.writer.OnRoot(root)
}

// OnAccount implements DumpCollector interface
func (d *DumpData) OnAccount(addr *common.Address, account state.DumpAccount) {
	dumpAccount := &state.DumpAccount{
		Balance:     account.Balance,
		Nonce:       account.Nonce,
		Root:        account.Root,
		CodeHash:    account.CodeHash,
		Code:        account.Code,
		Storage:     account.Storage,
		AddressHash: account.AddressHash,
		Address:     addr,
	}
	d.writer.OnAccount(dumpAccount)

	d.count += 1
	if d.count%2500 == 0 {
		d.per10000count += 1

		now := time.Now()
		all := now.Sub(d.startTime)
		last := now.Sub(d.lastTime)

		if d.per10000count < 5600 {
			per := all.Milliseconds() / d.per10000count
			need := per * (5601 - d.per10000count)
			timezone := int((8 * time.Hour).Seconds())
			shanghaiTimezone := time.FixedZone("Asia/Shanghai", timezone)

			log.Info(fmt.Sprintf("%s-%s had dump %v accounts, cost %d, need %d ms\n",
				d.lastTime.In(shanghaiTimezone).Format("15:04:05"),
				now.In(shanghaiTimezone).Format("15:04:05"),
				d.count, last.Microseconds(), need/1e3))
		} else {
			log.Info("had dump accounts", "count", d.count)
		}

		d.lastTime = now
	}
}

// Dump returns a JSON string representing the entire state as a single json-object
func Dump(ctx context.Context, file *os.File, s *state.StateDB, reader state.Reader, opts *state.DumpConfig) error {
	writer := &Writer{
		encoder:  json.NewEncoder(file),
		accounts: make(chan *state.DumpAccount, 4096),
		roots:    make(chan common.Hash, 4096),
	}
	writer.Run(ctx)

	dump := &DumpData{
		writer:    writer,
		startTime: time.Now(),
		lastTime:  time.Now(),
	}
	dump.Next = s.DumpToCollector(dump, opts)
	return nil
}

// printChainMetadata prints out chain metadata to stderr.
func printChainMetadata(db ethdb.KeyValueStore) {
	fmt.Fprintf(os.Stderr, "Chain metadata\n")
	for _, v := range rawdb.ReadChainMetadata(db) {
		fmt.Fprintf(os.Stderr, "  %s\n", strings.Join(v, ": "))
	}
	fmt.Fprintf(os.Stderr, "\n\n")
}

func DebugForDetail(accountCount uint64, msg string, ctx ...interface{}) {
	if accountCount > 272000 && accountCount < 300000 {
		log.Debug(msg, ctx...)
	}
}

func dump(ctx *cli.Context) error {
	log.SetDefault(log.NewLogger(log.NewTerminalHandlerWithLevel(os.Stderr, log.LevelTrace, false)))

	stack, _ := makeConfigNode(ctx)
	defer stack.Close()

	db := utils.MakeChainDatabase(ctx, stack, true)
	defer db.Close()

	// printChainMetadata(db)

	conf, root, err := parseDumpConfig(ctx, db)
	if err != nil {
		return err
	}
	conf.Max = 0
	// conf.OnlyWithAddresses = false

	triedb := utils.MakeTrieDatabase(ctx, db, true, true, false) // always enable preimage lookup
	defer triedb.Close()

	statedb := state.NewDatabase(triedb, nil)

	reader, err := statedb.Reader(root)
	if err != nil {
		return err
	}

	state, err := state.New(root, statedb)
	if err != nil {
		return err
	}

	fileName := JSONFileFlag.Value
	if fileName == "" {
		return fmt.Errorf("need use file")
	}

	if _, err := os.Stat(fileName); err == nil {
		return fmt.Errorf("file %s had exist", fileName)
	}

	file, err := os.Create(fileName)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	return Dump(ctx.Context, file, state, reader, conf)
}

// hashish returns true for strings that look like hashes.
func hashish(x string) bool {
	_, err := strconv.Atoi(x)
	return err != nil
}
