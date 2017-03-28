# rdb [![Build Status](https://travis-ci.org/matthewjhe/rdb.svg?branch=master)](https://travis-ci.org/matthewjhe/rdb) [![GoDoc](https://godoc.org/github.com/matthewjhe/rdb?status.svg)](http://godoc.org/github.com/matthewjhe/rdb)

**WARNING**: This project is not production ready.

Package rdb is a Redis RDB File parser written in [Go](https://golang.org/).

## Feature

- Concurrent parsing.
- Built in memory report.
- Simple filter support.
- Layered skipping strategies.

## Install

`go get -u github.com/matthewjhe/rdb`

## Usage

### Basic Example

The basic use case is very simple, just implements Filter interface and pass it to Parse:

```go
package main

import (
    "github.com/matthewjhe/rdb"
)

func (f filter) Key(k rdb.Key) bool { return false }
func (f filter) Type(t rdb.Type) bool   { return false }
func (f filter) Database(db rdb.DB) bool { return false }
func (f filter) Set(v *rdb.Set) { }
func (f filter) List(v *rdb.List) { }
func (f filter) Hash(v *rdb.Hash) { }
func (f filter) String(v *rdb.String) { }
func (f filter) SortedSet(v *rdb.SortedSet) { }

func main() {
    const file = "/tmp/dump.rdb"
    reader, err := rdb.NewBufferReader(file, 0)
    if err != nil {
        panic(err)
    }

    if err := rdb.Parse(reader, rdb.WithFilter(filter{})); err != nil {
        panic(err)
    }
}
```

### Skipping

**NOTE**: RDB file is read sequentially, when we say skips a database, we also need to parse this database's every single key,
we just simply skips actions like `decompress`, `unzip ziplist`, etc. It is impossible to skips a `key` without reading its metadata.

- Global Skip Strategy

Global skip strategy applies to the whole parsing lifetime, it can be overwritten by Filter's `Database` method,
it will be restored when parsing a database completely.

Global skip strategy is set by a ParseOption when rdb.Parse is called:

```go
    // reads memory report only
    strategy := rdb.WithStrategy(rdb.SkipExpiry | rdb.SkipMeta | rdb.SkipValue)
    err := rdb.Parse(reader, strategy, rdb.WithFilter(filter{}))
    // ...
```

- Database Skip Strategy

Database skip strategy applies to a database lifetime, it is set by Filter's `Database` method, and it can be overwritten
by Filter's `Key` or `Type` method. Database skip strategy is set to global skip strategy by default.

```go
func (f filter) Database(db rdb.DB) bool {
    // skips this database
    db.Skip(rdb.SkipAll)

    // skips database's metadata and key's expiry
    // db.Skip(rdb.SkipMeta | rdb.SkipExpiry)
    return false
}
```

- Type and Key Skip Strategy

Type and key skip strategy overwrite database skip strategy. It only applies the key and value which will read next.

```go
func (f filter) Type(t rdb.Type) bool {
    // skips this type
    t.Skip(rdb.SkipAll)
    return false
}

func (f filter) Key(key rdb.Key) bool {
    // skips this key
    // key.Skip(rdb.SkipAll)

    // skips value
    key.Skip(rdb.SkipValue)
    return false
}
```

### Syncing

By default, rdb uses multiple goroutines to parse keys which would cause keys disorder.
If the order of keys is important, use `EnableSync` ParseOption.

```go
    rdb.Parse(reader, rdb.WithFilter(filter{}), rdb.EnableSync())
```

### Aborting

Sometimes we want to abort the parsing process, this can be done by return `true` when one of the following methods is called:
Note that the abort action is taking effect immediately when the function returns.

```go
func (f filter) Key(k rdb.Key) bool {
    return true
}
func (f filter) Type(t rdb.Type) bool   {
    return true
}
func (f filter) Database(db rdb.DB) bool {
    return true
}
```

## References

- [redis-rdb-tools](https://github.com/sripathikrishnan/redis-rdb-tools) for Redis RDB format and memory profile.

## License

[MIT](https://github.com/matthewjhe/rdb/blob/master/LICENSE)
