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
	"os"
	"sync"
	"time"

	"github.com/buraksezer/olric/internal/offheap"
	"github.com/dgraph-io/badger"
	"github.com/vmihailenco/msgpack"
)

const (
	defaultSnapshotInterval         = 100 * time.Millisecond
	defaultGCInterval               = 5 * time.Minute
	defaultGCDiscardRatio   float64 = 0.7
)

const (
	opPut uint8 = 0
	opDel uint8 = 1
)

var (
	PrimaryDMapKey = []byte("primary-dmap-key")
	BackupDMapKey  = []byte("backup-dmap-key")
)

type onDiskDMaps map[uint64]map[string]struct{}

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

func dmapKey(name string) []byte {
	return []byte("dmap-keys-" + name)
}

type Snapshot struct {
	mu sync.RWMutex

	db               *badger.DB
	log              *log.Logger
	workers          map[uint64]context.CancelFunc
	oplogs           map[uint64]map[string]*OpLog
	snapshotInterval time.Duration
	wg               sync.WaitGroup
	ctx              context.Context
	cancel           context.CancelFunc
}

func New(opt *badger.Options, snapshotInterval, gcInterval time.Duration,
	gcDiscardRatio float64, logger *log.Logger) (*Snapshot, error) {
	if opt == nil {
		opt = &badger.DefaultOptions
	}
	if len(opt.Dir) == 0 {
		dir, err := os.Getwd()
		if err != nil {
			return nil, err
		}
		opt.Dir = dir
	}
	opt.ValueDir = opt.Dir

	if gcDiscardRatio == 0 {
		gcDiscardRatio = defaultGCDiscardRatio
	}

	if gcInterval.Seconds() == 0 {
		gcInterval = defaultGCInterval
	}

	if snapshotInterval.Seconds() == 0 {
		snapshotInterval = defaultSnapshotInterval
	}

	opt.ValueLogFileSize = 20971520
	db, err := badger.Open(*opt)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	s := &Snapshot{
		db:               db,
		log:              logger,
		workers:          make(map[uint64]context.CancelFunc),
		oplogs:           make(map[uint64]map[string]*OpLog),
		snapshotInterval: snapshotInterval,
		ctx:              ctx,
		cancel:           cancel,
	}
	s.wg.Add(1)
	go s.garbageCollection(gcInterval, gcDiscardRatio)
	return s, nil
}

// Shutdown closes a DB. It's crucial to call it to ensure all the pending updates make their way to disk.
func (s *Snapshot) Shutdown() error {
	select {
	case <-s.ctx.Done():
		return nil
	default:
	}
	s.cancel()
	// Wait for ongoing sync operations.
	s.wg.Wait()
	// Calling DB.Close() multiple times is not safe and would cause panic.
	return s.db.Close()
}

func (s *Snapshot) garbageCollection(gcInterval time.Duration, gcDiscardRatio float64) {
	defer s.wg.Done()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-time.After(gcInterval):
		again:
			err := s.db.RunValueLogGC(gcDiscardRatio)
			if err == nil {
				goto again
			}
			s.log.Printf("[DEBUG] BadgerDB GC returned: %v", err)
		}
	}
}

func (s *Snapshot) syncDMap(name string, oplog *OpLog) (map[uint64]uint8, error) {
	oplog.Lock()
	if len(oplog.m) == 0 {
		oplog.Unlock()
		return nil, nil
	}
	// Work on this temporary copy to get rid of disk overhead.
	tmp := make(map[uint64]uint8)
	for hkey, op := range oplog.m {
		tmp[hkey] = op
		delete(oplog.m, hkey)
	}
	oplog.Unlock()

	var hkeys map[uint64]struct{}
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(dmapKey(name))
		if err == badger.ErrKeyNotFound {
			hkeys = make(map[uint64]struct{})
			return nil
		}
		if err != nil {
			return err
		}
		raw, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		return msgpack.Unmarshal(raw, &hkeys)
	})
	if err != nil {
		return nil, err
	}

	failed := make(map[uint64]uint8)
	wb := s.db.NewWriteBatch()
	defer wb.Cancel()
	for hkey, op := range tmp {
		bkey := make([]byte, 8)
		binary.BigEndian.PutUint64(bkey, hkey)
		if op == opPut {
			val, err := oplog.o.GetRaw(hkey)
			if err == offheap.ErrKeyNotFound {
				continue
			}
			if err != nil {
				s.log.Printf("[ERROR] Failed to get HKey: %d from in-memory storage: %v", hkey, err)
				failed[hkey] = op
				continue
			}
			err = wb.Set(bkey, val, 0)
			if err != nil {
				s.log.Printf("[ERROR] Failed to set HKey: %d on %s: %v", hkey, name, err)
				failed[hkey] = op
				continue
			}
			hkeys[hkey] = struct{}{}
		} else {
			err = wb.Delete(bkey)
			if err != nil {
				s.log.Printf("[ERROR] Failed to delete HKey: %d on %s: %v", hkey, name, err)
				failed[hkey] = op
				continue
			}
			delete(hkeys, hkey)
		}
	}
	// Encode available keys to map hkeys to dmaps on Badger.
	data, _ := msgpack.Marshal(hkeys)
	err = wb.Set(dmapKey(name), data, 0)
	if err != nil {
		s.log.Printf("[ERROR] Failed to set dmap-keys for %s: %v", name, err)
		// Return the all hkeys to process again. We may lose all of them if this call
		// doesn't work.
		return tmp, wb.Flush()
	}
	// Failed keys will be processed again by the next call.
	return failed, wb.Flush()
}

func (s *Snapshot) worker(ctx context.Context, partID uint64) {
	defer s.wg.Done()

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
			failed, err := s.syncDMap(name, oplog)
			if err != nil {
				s.log.Printf("[ERROR] Failed to sync DMap: %s on PartID: %d: %v", name, partID, err)
			}
			if len(failed) != 0 {
				oplog.Lock()
				for hkey, op := range failed {
					_, ok := oplog.m[hkey]
					if !ok {
						// Add it again to process in the next call.
						oplog.m[hkey] = op
					}
				}
				oplog.Unlock()
			}
		}
	}
	for {
		select {
		case <-s.ctx.Done():
			// Olric instance has been closing. Call sync one last time.
			sync()
			return
		case <-ctx.Done():
			// Partition is empty.
			return
		case <-time.After(s.snapshotInterval):
			sync()
		}
	}
}

func (s *Snapshot) registerOnBadger(dkey []byte, partID uint64, name string) error {
	return s.db.Update(func(txn *badger.Txn) error {
		var value onDiskDMaps
		item, err := txn.Get(dkey)
		if err != nil && err != badger.ErrKeyNotFound {
			// Something went wrong.
			return err
		}

		if err == badger.ErrKeyNotFound {
			// Key not found.
			value = make(onDiskDMaps)
			err = nil
		} else {
			// err == nil
			valCopy, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			err = msgpack.Unmarshal(valCopy, &value)
			if err != nil {
				return err
			}
		}
		_, ok := value[partID]
		if !ok {
			value[partID] = make(map[string]struct{})
		}
		value[partID][name] = struct{}{}
		res, err := msgpack.Marshal(value)
		if err != nil {
			return err
		}
		return txn.Set(dkey, res)
	})
}

func (s *Snapshot) RegisterDMap(dkey []byte, partID uint64, name string, o *offheap.Offheap) (*OpLog, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.registerOnBadger(dkey, partID, name)
	if err != nil {
		return nil, err
	}
	if _, ok := s.oplogs[partID]; !ok {
		s.oplogs[partID] = make(map[string]*OpLog)
	}
	oplog := &OpLog{
		m: make(map[uint64]uint8),
		o: o,
	}
	s.oplogs[partID][name] = oplog

	if _, ok := s.workers[partID]; !ok {
		ctx, cancel := context.WithCancel(context.Background())
		s.workers[partID] = cancel
		s.wg.Add(1)
		go s.worker(ctx, partID)
	}
	return oplog, nil
}

func (s *Snapshot) unregisterOnBadger(dkey []byte, partID uint64, name string) error {
	return s.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(dkey)
		if err != nil {
			return err
		}

		var value onDiskDMaps
		valCopy, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		err = msgpack.Unmarshal(valCopy, &value)
		if err != nil {
			return err
		}
		delete(value[partID], name)
		if len(value[partID]) == 0 {
			delete(value, partID)
		}
		res, err := msgpack.Marshal(value)
		if err != nil {
			return err
		}
		return txn.Set(dkey, res)
	})
}

func (s *Snapshot) UnregisterDMap(dkey []byte, partID uint64, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	dmaps, ok := s.oplogs[partID]
	if !ok {
		return nil
	}
	if err := s.unregisterOnBadger(dkey, partID, name); err != nil {
		return err
	}
	delete(dmaps, name)
	if len(dmaps) == 0 {
		delete(s.oplogs, partID)
		s.workers[partID]()
	}

	return nil
}
