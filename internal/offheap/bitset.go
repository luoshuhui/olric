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

package offheap

const (
	// maximum item count in a bucket
	bucketCap uint64 = 1 << 3

	// the wordSize of a bit set
	wordSize = uint8(bucketCap)

	// log2WordSize is lg(wordSize)
	log2WordSize = uint8(3)
)

const (
	hasTTL uint8 = iota
	hasKey
	hasLastUsed
)

// wordsNeeded calculates the number of words needed for i bits
func wordsNeeded(i uint8) int {
	if i > ((^uint8(0)) - wordSize + 1) {
		return int((^uint8(0)) >> log2WordSize)
	}
	return int((i + (wordSize - 1)) >> log2WordSize)
}

var wordCount = wordsNeeded(uint8(bucketCap))

func (t *table) bset(i uint8) {
	t.memory[t.offset : t.offset+wordCount][i>>log2WordSize] |= 1 << (i & (wordSize - 1))
}

func (t *table) btest(i uint8) bool {
	return t.memory[t.offset : t.offset+wordCount][i>>log2WordSize]&(1<<(i&(wordSize-1))) != 0
}
