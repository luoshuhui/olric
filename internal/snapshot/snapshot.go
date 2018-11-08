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
	"log"
	"sync"
	"time"

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
	oplogs       map[uint64]map[string]*OpLog
	syncInterval time.Duration
	wg           sync.WaitGroup
	ctx          context.Context
	cancel       context.CancelFunc
}

func New(opt badger.Options, partitionCount uint64, syncInterval time.Duration) (*Snapshot, error) {
	if syncInterval.Seconds() == 0 {
		syncInterval = defaultSyncInterval
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &Snapshot{
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

func (s *Snapshot) syncDMap(name string, oplog *OpLog) error {
	oplog.Lock()
	if len(oplog.m) == 0 {
		oplog.Unlock()
		return nil
	}
	tmp := make(map[uint64]uint8)
	for hkey, op := range oplog.m {
		tmp[hkey] = op
		delete(oplog.m, hkey)
	}
	oplog.Unlock()
	// Work on this temporary copy to get rid of disk overhead.
	//for hkey, ok := range oplog.m {
	// Call Put/Delete on BadgerDB
	//}
	return nil
}

func (s *Snapshot) scanPartition(partID uint64) {
	defer s.wg.Done()
	ticker := time.NewTicker(s.syncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			// Olric instance has been closing.
			return
		case <-ticker.C:
			// sync the dmaps to badger.
			s.mu.RLock()
			part, ok := s.oplogs[partID]
			if !ok {
				s.mu.RUnlock()
				// There are no registered DMap on this partition.
				continue
			}
			for name, oplog := range part {
				err := s.syncDMap(name, oplog)
				if err != nil {
					log.Printf("[ERROR] Failed to sync DMap: %s: %v", name, err)
					continue
				}
			}
			s.mu.RUnlock()
		}
	}
}

func (s *Snapshot) RegisterDMap(partID uint64, name string) *OpLog {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.oplogs[partID]; !ok {
		s.oplogs[partID] = make(map[string]*OpLog)
	}
	oplog := &OpLog{
		m: make(map[uint64]uint8),
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
