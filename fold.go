package skiff

// foldLatest returns the most recent put payload per key from a WAL buffer.
func foldLatest(b []byte) map[string]struct {
	key, value []byte
} {
	type slot struct{ key, value []byte }
	out := map[string]slot{}
	off := 0
	for off < len(b) {
		kind, key, value, next, ok := decodeRecord(b, off)
		if !ok {
			break
		}
		if kind == kindPut {
			ks := string(key)
			out[ks] = slot{
				key:   append([]byte(nil), key...),
				value: append([]byte(nil), value...),
			}
		}
		off = next
	}
	ret := make(map[string]struct {
		key, value []byte
	}, len(out))
	for k, s := range out {
		ret[k] = struct{ key, value []byte }{key: s.key, value: s.value}
	}
	return ret
}
