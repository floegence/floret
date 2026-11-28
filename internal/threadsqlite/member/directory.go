package member

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"slices"

	"github.com/floegence/floret/v7/storage/spi"
)

const directoryHeader = 56
const directorySlot = 48

type DirectoryKey [sha256.Size]byte

func ActivityKey(section byte, path string, ordinal int) DirectoryKey {
	hash := sha256.New()
	hash.Write([]byte{section})
	hash.Write(binary.BigEndian.AppendUint64(nil, uint64(ordinal)))
	hash.Write([]byte(path))
	return DirectoryKey(hash.Sum(nil))
}

type ActivityDirectory struct {
	journal     *Journal
	object      int64
	size, count int
	page        int
	cache       []byte
}

func (journal *Journal) PutActivityDirectory(ref BodyRef, records map[DirectoryKey][]byte) error {
	if journal.readOnly || ref.End <= BlockBytes || len(records) == 0 {
		return spi.ErrInvalidArgument
	}
	keys := make([]DirectoryKey, 0, len(records))
	for key := range records {
		keys = append(keys, key)
	}
	slices.SortFunc(keys, func(a, b DirectoryKey) int { return bytes.Compare(a[:], b[:]) })
	size := directoryHeader + len(keys)*directorySlot
	for _, key := range keys {
		size += len(records[key])
	}
	digest, err := hex.DecodeString(ref.Digest)
	if err != nil || len(digest) != 32 {
		return ErrContentIntegrity
	}
	data := make([]byte, directoryHeader+len(keys)*directorySlot, size)
	copy(data, "FLORIDX2")
	copy(data[8:], digest)
	binary.BigEndian.PutUint64(data[40:], uint64(len(keys)))
	binary.BigEndian.PutUint64(data[48:], uint64(size))
	for index, key := range keys {
		offset := directoryHeader + index*directorySlot
		copy(data[offset:], key[:])
		binary.BigEndian.PutUint64(data[offset+32:], uint64(len(data)))
		value := records[key]
		binary.BigEndian.PutUint64(data[offset+40:], uint64(len(value)))
		data = append(data, value...)
	}
	_, err = journal.tx.ExecContext(journal.ctx, `DELETE FROM `+journal.database+`content_blocks WHERE object_seq=? AND part=1`, ref.Object)
	if err != nil {
		return err
	}
	for offset := 0; offset < len(data); offset += BlockBytes {
		block := data[offset:min(offset+BlockBytes, len(data))]
		sum := sha256.Sum256(block)
		_, err = journal.tx.ExecContext(journal.ctx, `INSERT INTO `+journal.database+`content_blocks(object_seq,part,block_no,start_byte,bytes,sha256) VALUES(?,1,?,?,?,?)`, ref.Object, offset/BlockBytes, offset, block, sum[:])
		if err != nil {
			return err
		}
	}
	return nil
}

func (journal *Journal) ActivityDirectory(ref BodyRef) (*ActivityDirectory, error) {
	d := &ActivityDirectory{journal: journal, object: ref.Object, page: -1}
	if err := d.load(0); err != nil {
		return nil, err
	}
	if len(d.cache) < directoryHeader || string(d.cache[:8]) != "FLORIDX2" || hex.EncodeToString(d.cache[8:40]) != ref.Digest {
		return nil, ErrContentIntegrity
	}
	d.count, d.size = int(binary.BigEndian.Uint64(d.cache[40:])), int(binary.BigEndian.Uint64(d.cache[48:]))
	if d.count < 1 || d.size < directoryHeader || d.count > (d.size-directoryHeader)/directorySlot || len(d.cache) != min(d.size, BlockBytes) {
		return nil, ErrContentIntegrity
	}
	return d, nil
}

func (d *ActivityDirectory) load(page int) error {
	if d.page == page {
		return nil
	}
	var block, digest []byte
	var start int
	err := d.journal.tx.QueryRowContext(d.journal.ctx, `SELECT start_byte,bytes,sha256 FROM `+d.journal.database+`content_blocks WHERE object_seq=? AND part=1 AND block_no=?`, d.object, page).Scan(&start, &block, &digest)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrContentIntegrity
	}
	if err != nil {
		return err
	}
	sum := sha256.Sum256(block)
	if start != page*BlockBytes || !bytes.Equal(digest, sum[:]) || d.size > 0 && len(block) != min(BlockBytes, d.size-start) {
		return ErrContentIntegrity
	}
	d.page, d.cache = page, block
	return nil
}

func (d *ActivityDirectory) read(start, size int) ([]byte, error) {
	if start < 0 || start > d.size || size < 0 || size > d.size-start {
		return nil, ErrContentIntegrity
	}
	var result []byte
	for size > 0 {
		if err := d.load(start / BlockBytes); err != nil {
			return nil, err
		}
		offset := start % BlockBytes
		n := min(size, len(d.cache)-offset)
		if n < 1 {
			return nil, ErrContentIntegrity
		}
		result = append(result, d.cache[offset:offset+n]...)
		start += n
		size -= n
	}
	return result, nil
}

func (d *ActivityDirectory) Lookup(key DirectoryKey) ([]byte, error) {
	lo, hi := 0, d.count
	for lo < hi {
		mid := lo + (hi-lo)/2
		slot, err := d.read(directoryHeader+mid*directorySlot, directorySlot)
		if err != nil {
			return nil, err
		}
		switch bytes.Compare(slot[:32], key[:]) {
		case -1:
			lo = mid + 1
		case 1:
			hi = mid
		default:
			start, size := int(binary.BigEndian.Uint64(slot[32:])), int(binary.BigEndian.Uint64(slot[40:]))
			if start < directoryHeader+d.count*directorySlot {
				return nil, ErrContentIntegrity
			}
			return d.read(start, size)
		}
	}
	return nil, spi.ErrNotFound
}
