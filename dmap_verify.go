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

package olricdb

import (
	"math/rand"
	"sync"
	"time"
)

func (db *OlricDB) verifyDMapsAtBackground() {
	defer db.wg.Done()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-db.ctx.Done():
			return
		case <-ticker.C:
			db.verifyDMaps()
		}
	}
}

func (db *OlricDB) verifyDMaps() {
	partID := uint64(rand.Intn(int(db.config.PartitionCount)))
	part := db.partitions[partID]
	part.RLock()
	if len(part.owners) == 0 {
		// This is a bug, if the instance had been bootstrapped.
		return
	}

	if len(part.m) == 0 {
		// No data on the partition
		return
	}

	var wg sync.WaitGroup
	dcount := 0
	for name, dm := range part.m {
		dcount++
		if dcount >= 20 {
			break
		}
		wg.Add(1)
		go db.scanDMapForVerification(partID, name, dm, &wg)
	}
	part.RUnlock()
	wg.Wait()
}

func (db *OlricDB) scanDMapForVerification(partID uint64, name string, dm *dmap, wg *sync.WaitGroup) {
	defer wg.Done()
	dm.Lock()
	defer dm.Unlock()
}
