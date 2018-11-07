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

package persistent

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/dgraph-io/badger"
)

// TODO: Implement a global logger for Olric package

// sync dmaps to disk 10 times per second.
const defaultSyncInterval = 100 * time.Millisecond

var (
	ErrAlreadySyncing = errors.New("partition is already syncing to disk")
	ErrNotSyncing     = errors.New("partition is not syncing")
)

type syncer struct {
	cancel context.CancelFunc
	done   chan struct{}
}

type Persistent struct {
	mu sync.RWMutex

	db           *badger.DB
	syncers      map[uint64]syncer
	syncInterval time.Duration
	wg           sync.WaitGroup
	ctx          context.Context
	cancel       context.CancelFunc
}

func New(opt *badger.Options, syncInterval time.Duration) (*Persistent, error) {
	if syncInterval.Seconds() == 0 {
		syncInterval = defaultSyncInterval
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Persistent{
		syncers:      make(map[uint64]syncer),
		syncInterval: syncInterval,
		ctx:          ctx,
		cancel:       cancel,
	}, nil
}

func (p *Persistent) syncDMaps(dmaps sync.Map) error {
	dmaps.Range(func(name interface{}, tmp interface{}) bool {
		return true
	})
	return nil
}

func (p *Persistent) startSync(ctx context.Context, dmaps sync.Map, done chan struct{}) {
	ticker := time.NewTicker(p.syncInterval)
	defer ticker.Stop()
	defer close(done)

	for {
		select {
		case <-ctx.Done():
			// FSCK called StopSync for this partition.
			return
		case <-p.ctx.Done():
			// Olric instance has been closing.
			return
		case <-ticker.C:
			// sync the dmaps to badger.
			err := p.syncDMaps(dmaps)
			if err != nil {
				log.Printf("[ERROR] Failed to call syncDMaps: %v", err)
			}
		}
	}
}

func (p *Persistent) StartSync(partID uint64, dmaps sync.Map, backup bool) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if _, ok := p.syncers[partID]; ok {
		return ErrAlreadySyncing
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := syncer{
		cancel: cancel,
		done:   make(chan struct{}),
	}
	p.wg.Add(1)
	go p.startSync(ctx, dmaps, s.done)
	p.syncers[partID] = s
	return nil
}

func (p *Persistent) StopSync(partID uint64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	s, ok := p.syncers[partID]
	if !ok {
		return ErrNotSyncing
	}
	// Signal the syncer to close and wait.
	s.cancel()
	// Wait for closing syncer.a
	<-s.done
	return nil
}
