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
	"encoding/binary"
	"fmt"
)

type versionVector struct {
	HKey, RVer, KVer uint64
}

func vectorCmp(v1, v2 *versionVector) bool {
	return v1.HKey == v2.HKey && v1.KVer == v2.KVer && v1.RVer == v2.RVer
}

func (m *merkleTree) hash(v *versionVector) uint64 {
	for i, item := range []uint64{v.HKey, v.RVer, v.KVer} {
		binary.LittleEndian.PutUint64(m.tmp, item)
		copy(m.res[i*8:], m.tmp)
	}
	return m.hasher.Sum64(m.res)
}

func (m *merkleTree) recalcParentHash(phash, hver uint64) uint64 {
	for i, item := range []uint64{phash, hver} {
		binary.LittleEndian.PutUint64(m.tmp, item)
		copy(m.rres[i*8:], m.tmp)
	}
	return m.hasher.Sum64(m.rres)
}

type mnode struct {
	hash   uint64
	vector *versionVector
	left   *mnode
	right  *mnode
}

type merkleTree struct {
	hasher         Hasher
	parent         *mnode
	res, rres, tmp []byte
}

func newMerkleTree(hasher Hasher) *merkleTree {
	if hasher == nil {
		hasher = newDefaultHasher()
	}
	return &merkleTree{
		res:    make([]byte, 24),
		tmp:    make([]byte, 8),
		rres:   make([]byte, 16),
		hasher: hasher,
	}
}

func (m *merkleTree) insertRecursive(n *mnode, v *versionVector, hver uint64) uint64 {
	var next *mnode
	if n.vector.HKey < v.HKey {
		if n.left != nil {
			next = n.left
		}
	} else {
		if n.right != nil {
			next = n.right
		}
	}

	if next != nil {
		n.hash = m.recalcParentHash(n.hash, m.insertRecursive(next, v, hver))
		return n.hash
	}

	leaf := &mnode{
		hash:   hver,
		vector: v,
	}
	if n.vector.HKey < v.HKey {
		n.left = leaf
	} else {
		n.right = leaf
	}
	n.hash = m.recalcParentHash(n.hash, leaf.hash)
	return n.hash
}

func (m *merkleTree) insert(v *versionVector) {
	hver := m.hash(v)
	if m.parent == nil {
		m.parent = &mnode{
			hash:   hver,
			vector: v,
		}
		return
	}
	m.parent.hash = m.recalcParentHash(
		m.parent.hash,
		m.insertRecursive(m.parent, v, hver),
	)
}

func (m *merkleTree) diff(t *merkleTree) []uint64 {
	return nil
}

// Walk traverses a tree depth-first,
// sending each Value on a channel.
func Walk(t *mnode, ch chan *mnode) {
	if t == nil {
		return
	}
	Walk(t.left, ch)
	ch <- t
	Walk(t.right, ch)
}

// Walker launches Walk in a new goroutine,
// and returns a read-only channel of values.
func Walker(t *mnode) <-chan *mnode {
	ch := make(chan *mnode)
	go func() {
		Walk(t, ch)
		close(ch)
	}()
	return ch
}

// Compare reads values from two Walkers
// that run simultaneously, and returns true
// if t1 and t2 have the same contents.
func Compare(t1, t2 *mnode) bool {
	c1, c2 := Walker(t1), Walker(t2)
	for {
		v1, ok1 := <-c1
		v2, ok2 := <-c2
		if !ok1 || !ok2 {
			return ok1 == ok2
		}
		if v1.hash == v2.hash {
			fmt.Println("hash'ler ayni. kontrole gerek yok")
			return true
		}
		if !vectorCmp(v1.vector, v2.vector) {
			break
		}
	}
	return false
}

// 1- Primary-owner sends a versionVector list o build a merkle tree.
// 2- Backup node scans its copy for incoming hkeys and build its own merkle tree
// 3- Backup node builds a second merkle tree for incoming versionVector list.
// 4- Compares the merkle trees to find differences:
//	* Backup's tree may has out-dated items
//	* Backup's tree may has missing items
//	* Backup's tree may has
