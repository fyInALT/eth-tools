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
	"bytes"
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
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/holiman/uint256"
	"github.com/urfave/cli/v2"
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
	dump.Next = DumpToCollector(s, reader, dump, opts)
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

// DumpToCollector iterates the state according to the given options and inserts
// the items into a collector for aggregation or serialization.
func DumpToCollector(s *state.StateDB, reader state.Reader, c state.DumpCollector, conf *state.DumpConfig) (nextKey []byte) {
	// Sanitize the input to allow nil configs
	if conf == nil {
		conf = new(state.DumpConfig)
	}
	var (
		missingPreimages int
		accounts         uint64
		start            = time.Now()
		logged           = time.Now()
	)
	log.Info("Trie dumping started", "root", s.GetTrie().Hash())
	c.OnRoot(s.GetTrie().Hash())

	trieIt, err := s.GetTrie().NodeIterator(conf.Start)
	if err != nil {
		log.Error("Trie dumping error", "err", err)
		return nil
	}
	it := trie.NewIterator(trieIt)
	for it.Next() {
		log.Debug("start process", "count", accounts, "missingPreimages", missingPreimages)

		isBreak := func() bool {
			var data types.StateAccount
			if err := rlp.DecodeBytes(it.Value, &data); err != nil {
				panic(err)
			}

			DebugForDetail(accounts, "data got")
			var (
				account = state.DumpAccount{
					Balance:     data.Balance.String(),
					Nonce:       data.Nonce,
					Root:        data.Root[:],
					CodeHash:    data.CodeHash,
					AddressHash: it.Key,
				}
				address   *common.Address
				addr      common.Address
				addrBytes = s.GetTrie().GetKey(it.Key)
			)

			DebugForDetail(accounts, "GetTrie got")

			if addrBytes == nil {
				DebugForDetail(accounts, "addrBytes is nil")
				missingPreimages++
				if missingPreimages%10000 == 0 {
					log.Debug("missing perimages", "count", missingPreimages)
				}
				if conf.OnlyWithAddresses {
					DebugForDetail(accounts, "return by OnlyWithAddresses")
					return false
				}
			} else {
				DebugForDetail(accounts, "addrBytes not nil")
				addr = common.BytesToAddress(addrBytes)
				address = &addr
				account.Address = address
			}
			obj := newObject(s, reader, addr, &data)
			DebugForDetail(accounts, "newObject")
			if !conf.SkipCode {
				account.Code = obj.Code()
				DebugForDetail(accounts, "got code")
			}
			if !conf.SkipStorage {
				account.Storage = make(map[common.Hash]string)
				tr, err := obj.getTrie()
				DebugForDetail(accounts, "got trie")
				if err != nil {
					log.Error("Failed to load storage trie", "err", err)
					return false
				}
				trieIt, err := tr.NodeIterator(nil)
				if err != nil {
					log.Error("Failed to create trie iterator", "err", err)
					return false
				}
				DebugForDetail(accounts, "got trieIt")
				storageIt := trie.NewIterator(trieIt)
				for storageIt.Next() {
					_, content, _, err := rlp.Split(storageIt.Value)
					if err != nil {
						log.Error("Failed to decode the value returned by iterator", "error", err)
						return false
					}
					account.Storage[common.BytesToHash(s.GetTrie().GetKey(storageIt.Key))] = common.Bytes2Hex(content)
				}
				DebugForDetail(accounts, "got storageIt")
			}
			c.OnAccount(address, account)
			DebugForDetail(accounts, "OnAccount finished")
			accounts++
			if time.Since(logged) > 8*time.Second {
				log.Info("Trie dumping in progress", "at", it.Key, "accounts", accounts,
					"elapsed", common.PrettyDuration(time.Since(start)))
				logged = time.Now()
			}
			if conf.Max > 0 && accounts >= conf.Max {
				if it.Next() {
					DebugForDetail(accounts, "next stopped")
					nextKey = it.Key
				}
				return true
			}

			return false
		}()

		log.Debug("stop process", "count", accounts, "missingPreimages", missingPreimages)

		if isBreak {
			break
		}
	}
	if missingPreimages > 0 {
		log.Warn("Dump incomplete due to missing preimages", "missing", missingPreimages)
	}
	log.Info("Trie dumping complete", "accounts", accounts,
		"elapsed", common.PrettyDuration(time.Since(start)))

	return nextKey
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

// stateObject represents an Ethereum account which is being modified.
//
// The usage pattern is as follows:
// - First you need to obtain a state object.
// - Account values as well as storages can be accessed and modified through the object.
// - Finally, call commit to return the changes of storage trie and update account data.
type stateObject struct {
	reader   state.Reader
	db       *state.StateDB
	address  common.Address      // address of ethereum account
	addrHash common.Hash         // hash of ethereum address of the account
	origin   *types.StateAccount // Account original data without any change applied, nil means it was not existent
	data     types.StateAccount  // Account data with all mutations applied in the scope of block

	// Write caches.
	trie state.Trie // storage trie, which becomes non-nil on first access
	code []byte     // contract bytecode, which gets set when code is loaded

	originStorage  state.Storage // Storage entries that have been accessed within the current block
	dirtyStorage   state.Storage // Storage entries that have been modified within the current transaction
	pendingStorage state.Storage // Storage entries that have been modified within the current block

	// uncommittedStorage tracks a set of storage entries that have been modified
	// but not yet committed since the "last commit operation", along with their
	// original values before mutation.
	//
	// Specifically, the commit will be performed after each transaction before
	// the byzantium fork, therefore the map is already reset at the transaction
	// boundary; however post the byzantium fork, the commit will only be performed
	// at the end of block, this set essentially tracks all the modifications
	// made within the block.
	uncommittedStorage state.Storage

	// Cache flags.
	dirtyCode bool // true if the code was updated

	// Flag whether the account was marked as self-destructed. The self-destructed
	// account is still accessible in the scope of same transaction.
	selfDestructed bool

	// This is an EIP-6780 flag indicating whether the object is eligible for
	// self-destruct according to EIP-6780. The flag could be set either when
	// the contract is just created within the current transaction, or when the
	// object was previously existent and is being deployed as a contract within
	// the current transaction.
	newContract bool
}

// newObject creates a state object.
func newObject(db *state.StateDB, reader state.Reader, address common.Address, acct *types.StateAccount) *stateObject {
	origin := acct
	if acct == nil {
		acct = types.NewEmptyStateAccount()
	}
	return &stateObject{
		reader:             reader,
		db:                 db,
		address:            address,
		addrHash:           crypto.Keccak256Hash(address[:]),
		origin:             origin,
		data:               *acct,
		originStorage:      make(state.Storage),
		dirtyStorage:       make(state.Storage),
		pendingStorage:     make(state.Storage),
		uncommittedStorage: make(state.Storage),
	}
}

// empty returns whether the account is considered empty.
func (s *stateObject) empty() bool {
	return s.data.Nonce == 0 && s.data.Balance.IsZero() && bytes.Equal(s.data.CodeHash, types.EmptyCodeHash.Bytes())
}

//
// Attribute accessors
//

// Address returns the address of the contract/account
func (s *stateObject) Address() common.Address {
	return s.address
}

// Code returns the contract code associated with this object, if any.
func (s *stateObject) Code() []byte {
	if len(s.code) != 0 {
		return s.code
	}
	if bytes.Equal(s.CodeHash(), types.EmptyCodeHash.Bytes()) {
		return nil
	}
	code, err := s.reader.Code(s.address, common.BytesToHash(s.CodeHash()))
	if err != nil {
		log.Warn("can't load code hash", "hash", s.CodeHash(), "error", err)
	}
	if len(code) == 0 {
		log.Warn("code is not found", "hash", s.CodeHash())
	}
	s.code = code
	return code
}

func (s *stateObject) CodeHash() []byte {
	return s.data.CodeHash
}

func (s *stateObject) Balance() *uint256.Int {
	return s.data.Balance
}

func (s *stateObject) Nonce() uint64 {
	return s.data.Nonce
}

func (s *stateObject) Root() common.Hash {
	return s.data.Root
}

// getTrie returns the associated storage trie. The trie will be opened if it's
// not loaded previously. An error will be returned if trie can't be loaded.
//
// If a new trie is opened, it will be cached within the state object to allow
// subsequent reads to expand the same trie instead of reloading from disk.
func (s *stateObject) getTrie() (state.Trie, error) {
	if s.trie == nil {
		tr, err := s.db.OpenStorageTrie(s.address)
		if err != nil {
			return nil, err
		}
		s.trie = tr
	}
	return s.trie, nil
}
