// Copyright 2018 Burak Sezer
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package snapshot

import (
	"context"
	"encoding/binary"
	"log"
	"sync"
	"time"

	"github.com/buraksezer/olric/internal/bufpool"
	"github.com/buraksezer/olric/internal/offheap"
	"github.com/dgraph-io/badger"
)

// TODO: Implement a global logger for Olric package

// sync dmaps to disk 10 times per second.
const defaultSyncInterval = 100 * time.Millisecond

const (
	opPut uint8 = 0
	opDel uint8 = 1
)

type OpLog struct {
	sync.Mutex

	m map[uint64]uint8
	o *offheap.Offheap
}

func (o *OpLog) Put(hkey uint64) {
	o.Lock()
	defer o.Unlock()

	o.m[hkey] = opPut
}

func (o *OpLog) Delete(hkey uint64) {
	o.Lock()
	defer o.Unlock()

	o.m[hkey] = opDel
}

type Snapshot struct {
	mu sync.RWMutex

	db           *badger.DB
	bufpool      *bufpool.BufPool
	oplogs       map[uint64]map[string]*OpLog
	syncInterval time.Duration
	wg           sync.WaitGroup
	ctx          context.Context
	cancel       context.CancelFunc
}

func New(opts *badger.Options, partitionCount uint64, syncInterval time.Duration) (*Snapshot, error) {
	if syncInterval.Seconds() == 0 {
		syncInterval = defaultSyncInterval
	}

	// TEMP START
	if opts == nil {
		opts = &badger.DefaultOptions
	}
	opts.Dir = "/Users/burak/badger-data"
	opts.ValueDir = "/Users/burak/badger-data"
	opts.ValueLogFileSize = 20971520
	// TEMP END

	db, err := badger.Open(*opts)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	s := &Snapshot{
		db:           db,
		bufpool:      bufpool.New(),
		oplogs:       make(map[uint64]map[string]*OpLog),
		syncInterval: syncInterval,
		ctx:          ctx,
		cancel:       cancel,
	}
	for partID := uint64(0); partID < partitionCount; partID++ {
		s.wg.Add(1)
		go s.scanPartition(partID)
	}
	return s, nil
}

// Shutdown closes a DB. It's crucial to call it to ensure all the pending updates make their way to disk.
func (s *Snapshot) Shutdown() error {
	s.cancel()
	select {
	case <-s.ctx.Done():
		return nil
	default:
	}
	// Wait for ongoing sync operations.
	s.wg.Wait()
	// Calling DB.Close() multiple times is not safe and would cause panic.
	return s.db.Close()
}

func (s *Snapshot) del(hkey uint64, bkey []byte, tx *badger.Txn) error {
	binary.BigEndian.PutUint64(bkey, hkey)
	err := tx.Delete(bkey)
	if err == badger.ErrTxnTooBig {
		if err = tx.Commit(nil); err != nil {
			return err
		}
		tx = s.db.NewTransaction(true)
		return tx.Delete(bkey)
	}
	return err
}

func (s *Snapshot) put(name string, hkey uint64, bkey []byte, tx *badger.Txn, off *offheap.Offheap) error {
	buf := s.bufpool.Get()
	defer s.bufpool.Put(buf)

	err := off.GetRaw(hkey, buf)
	if err != nil {
		return err
	}
	// TODO: We may want to set name-length before.
	_, err = buf.WriteString(name)
	if err != nil {
		return err
	}
	binary.BigEndian.PutUint64(bkey, hkey)
	err = tx.Set(bkey, buf.Bytes())
	if err == badger.ErrTxnTooBig {
		if err = tx.Commit(nil); err != nil {
			return err
		}
		tx = s.db.NewTransaction(true)
		return tx.Set(bkey, buf.Bytes())
	}
	return err
}

func (s *Snapshot) syncDMap(name string, oplog *OpLog) error {
	oplog.Lock()
	if len(oplog.m) == 0 {
		oplog.Unlock()
		return nil
	}
	// Work on this temporary copy to get rid of disk overhead.
	tmp := make(map[uint64]uint8)
	for hkey, op := range oplog.m {
		tmp[hkey] = op
		delete(oplog.m, hkey)
	}
	oplog.Unlock()

	var err error
	bkey := make([]byte, 8)
	tx := s.db.NewTransaction(true)
	for hkey, op := range tmp {
		if op == opPut {
			err = s.put(name, hkey, bkey, tx, oplog.o)
		} else {
			err = s.del(hkey, bkey, tx)
		}
		if err != nil {
			log.Printf("[ERROR] Failed to set hkey: %d on %s: %v", hkey, name, err)
			continue
		}
	}
	return tx.Commit(nil)
}

func (s *Snapshot) scanPartition(partID uint64) {
	defer s.wg.Done()
	ticker := time.NewTicker(s.syncInterval)
	defer ticker.Stop()

	sync := func() {
		// sync the dmaps to badger.
		s.mu.RLock()
		defer s.mu.RUnlock()

		part, ok := s.oplogs[partID]
		if !ok {
			// There are no registered DMap on this partition.
			return
		}
		for name, oplog := range part {
			err := s.syncDMap(name, oplog)
			if err != nil {
				log.Printf("[ERROR] Failed to sync DMap: %s: %v", name, err)
				return
			}
		}
	}
	for {
		select {
		case <-s.ctx.Done():
			// Olric instance has been closing. Call sync one last time.
			sync()
			return
		case <-ticker.C:
			sync()
		}
	}
}

func (s *Snapshot) RegisterDMap(partID uint64, name string, o *offheap.Offheap) *OpLog {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.oplogs[partID]; !ok {
		s.oplogs[partID] = make(map[string]*OpLog)
	}
	oplog := &OpLog{
		m: make(map[uint64]uint8),
		o: o,
	}
	s.oplogs[partID][name] = oplog
	return oplog
}

func (s *Snapshot) UnregisterDMap(partID uint64, name string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	dmaps, ok := s.oplogs[partID]
	if !ok {
		return
	}
	delete(dmaps, name)
	if len(dmaps) == 0 {
		delete(s.oplogs, partID)
	}
}
