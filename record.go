package skiff

import (
	"encoding/binary"
	"hash/crc32"
)

// Record kinds in the WAL.
const (
	kindPut    byte = 1
	kindDelete byte = 2
)

// Wire format (little-endian):
//
//	crc32 | kind | keyLen u32 | valLen u32 | key | value
//
// crc32 covers kind..value. A torn write leaves a partial trailer that fails
// the length or checksum check on replay.
const headerSize = 4 + 1 + 4 + 4 // crc + kind + keyLen + valLen

func encodeRecord(kind byte, key, value []byte) []byte {
	n := headerSize + len(key) + len(value)
	buf := make([]byte, n)
	buf[4] = kind
	binary.LittleEndian.PutUint32(buf[5:9], uint32(len(key)))
	binary.LittleEndian.PutUint32(buf[9:13], uint32(len(value)))
	copy(buf[13:], key)
	copy(buf[13+len(key):], value)
	sum := crc32.ChecksumIEEE(buf[4:])
	binary.LittleEndian.PutUint32(buf[0:4], sum)
	return buf
}

// decodeRecord parses one record from b starting at offset off.
// Returns kind, key, value, nextOffset, ok.
// ok=false means the bytes from off are a torn/invalid tail (not a complete record).
func decodeRecord(b []byte, off int) (kind byte, key, value []byte, next int, ok bool) {
	if off+headerSize > len(b) {
		return 0, nil, nil, off, false
	}
	sum := binary.LittleEndian.Uint32(b[off : off+4])
	kind = b[off+4]
	klen := int(binary.LittleEndian.Uint32(b[off+5 : off+9]))
	vlen := int(binary.LittleEndian.Uint32(b[off+9 : off+13]))
	if klen < 0 || vlen < 0 {
		return 0, nil, nil, off, false
	}
	total := headerSize + klen + vlen
	if off+total > len(b) {
		return 0, nil, nil, off, false
	}
	payload := b[off+4 : off+total]
	if crc32.ChecksumIEEE(payload) != sum {
		return 0, nil, nil, off, false
	}
	if kind != kindPut && kind != kindDelete {
		return 0, nil, nil, off, false
	}
	key = make([]byte, klen)
	copy(key, b[off+13:off+13+klen])
	value = make([]byte, vlen)
	copy(value, b[off+13+klen:off+total])
	return kind, key, value, off + total, true
}
