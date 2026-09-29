# skiff

A small Bitcask-style durable key/value store in Go.

## API

- `Open(dir)` / `Close` / `Sync`
- `Put` / `Get` / `Delete` / `Has` / `Len`
- `Compact` — rewrite the log, dropping obsolete history
- `Backup` / `Restore` — snapshot the live keyspace

Empty values are first-class: `Put(k, []byte{})` stores an empty blob. That is
not the same as a missing key (`Get` → `ErrNotFound`).

## License

Apache-2.0
