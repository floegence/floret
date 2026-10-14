package sessiontree

import "math/bits"

type querySet interface {
	count() int
	has(*queryRecord) bool
	each(func(*queryRecord) bool)
}
type queryRecordMap map[string]*queryRecord

func (set queryRecordMap) count() int                   { return len(set) }
func (set queryRecordMap) has(record *queryRecord) bool { return set[record.key] == record }
func (set queryRecordMap) each(visit func(*queryRecord) bool) {
	for _, record := range set {
		if !visit(record) {
			return
		}
	}
}

// Sparse bitmap blocks avoid one string/map entry per matching record in a
// common n-gram. Posting and record slots are reused when facts are replaced.
type queryPosting struct {
	index  *queryIndex
	slot   uint32
	key    string
	blocks map[uint32]uint64
	total  int
}

func (set *queryPosting) count() int {
	if set == nil {
		return 0
	}
	return set.total
}
func (set *queryPosting) has(record *queryRecord) bool {
	if set == nil {
		return false
	}
	return set.blocks[record.indexID/64]&(uint64(1)<<(record.indexID%64)) != 0
}
func (set *queryPosting) each(visit func(*queryRecord) bool) {
	if set == nil {
		return
	}
	for block, word := range set.blocks {
		for word != 0 {
			bit := bits.TrailingZeros64(word)
			word &= word - 1
			record := set.index.recordsByID[block*64+uint32(bit)]
			if record != nil && !visit(record) {
				return
			}
		}
	}
}
func (set *queryPosting) add(record *queryRecord) {
	block := record.indexID / 64
	mask := uint64(1) << (record.indexID % 64)
	if set.blocks[block]&mask == 0 {
		set.blocks[block] |= mask
		set.total++
	}
}
func (set *queryPosting) remove(record *queryRecord) {
	block := record.indexID / 64
	mask := uint64(1) << (record.indexID % 64)
	if set.blocks[block]&mask != 0 {
		word := set.blocks[block] &^ mask
		if word == 0 {
			delete(set.blocks, block)
		} else {
			set.blocks[block] = word
		}
		set.total--
	}
}
func (idx *queryIndex) addPosting(key string, record *queryRecord) {
	posting := idx.postings[key]
	if posting == nil {
		var slot uint32
		if n := len(idx.freePostingSlots); n > 0 {
			slot = idx.freePostingSlots[n-1]
			idx.freePostingSlots = idx.freePostingSlots[:n-1]
		} else {
			slot = uint32(len(idx.postingSlots))
			idx.postingSlots = append(idx.postingSlots, nil)
		}
		posting = &queryPosting{index: idx, slot: slot, key: key, blocks: map[uint32]uint64{}}
		idx.postings[key] = posting
		idx.postingSlots[slot] = posting
	}
	posting.add(record)
	record.postings = append(record.postings, posting.slot)
}
func (idx *queryIndex) allocateRecordSlot(record *queryRecord) {
	if n := len(idx.freeRecordSlots); n > 0 {
		record.indexID = idx.freeRecordSlots[n-1]
		idx.freeRecordSlots = idx.freeRecordSlots[:n-1]
		idx.recordsByID[record.indexID] = record
	} else {
		record.indexID = uint32(len(idx.recordsByID))
		idx.recordsByID = append(idx.recordsByID, record)
	}
}
