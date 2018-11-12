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
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/buraksezer/olric/internal/offheap"
	"github.com/dgraph-io/badger"
	"github.com/vmihailenco/msgpack"
)

var ErrLoaderDone = errors.New("loader done")

type Loader struct {
	s     *Snapshot
	dmaps map[uint64]map[string]struct{}
}

func (s *Snapshot) NewLoader(dkey []byte) (*Loader, error) {
	var dmaps map[uint64]map[string]struct{}
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(dkey)
		if err != nil {
			return err
		}
		value, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		return msgpack.Unmarshal(value, &dmaps)
	})
	if err != nil {
		return nil, err
	}
	return &Loader{
		s:     s,
		dmaps: dmaps,
	}, nil
}

func (l *Loader) loadFromBadger(hkeys map[uint64]struct{}) (*offheap.Offheap, error) {
	o, err := offheap.New(1 << 20)
	if err != nil {
		return nil, err
	}
	err = l.s.db.View(func(txn *badger.Txn) error {
		bkey := make([]byte, 8)
		for hkey, _ := range hkeys {
			fmt.Println(hkey)
			binary.BigEndian.PutUint64(bkey, hkey)
			item, err := txn.Get(bkey)
			if err != nil {
				return err
			}
			err = item.Value(func(val []byte) error {
				return o.PutRaw(hkey, val)
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
	return o, err
}

func (l *Loader) Next() (uint64, string, *offheap.Offheap, error) {
	for partID, dmaps := range l.dmaps {
		for name, _ := range dmaps {
			var hkeys map[uint64]struct{}
			err := l.s.db.View(func(txn *badger.Txn) error {
				item, err := txn.Get(dmapKey(name))
				if err != nil {
					return err
				}
				value, err := item.ValueCopy(nil)
				if err != nil {
					return err
				}
				return msgpack.Unmarshal(value, &hkeys)
			})
			o, err := l.loadFromBadger(hkeys)
			if err != nil {
				return 0, "", nil, err
			}

			delete(l.dmaps[partID], name)
			if len(l.dmaps[partID]) == 0 {
				delete(l.dmaps, partID)
			}
			return partID, name, o, nil
		}
	}
	return 0, "", nil, ErrLoaderDone
}
