package rdb

import (
	"strconv"
	"sync"
)

// Redis value encodings.
const (
	EncodingString       = 0
	EncodingList         = 1
	EncodingSet          = 2
	EncodingSortedSet    = 3
	EncodingHash         = 4
	EncodingSortedSet2   = 5
	EncodingZipmap       = 9
	EncodingZiplist      = 10
	EncodingIntset       = 11
	EncodingSortedSetZip = 12
	EncodingHashZip      = 13
	EncodingQuicklist    = 14
)

// Redis types.
const (
	TypeSet       = "set"
	TypeList      = "list"
	TypeHash      = "hash"
	TypeString    = "string"
	TypeSortedSet = "sortedset"
)

// A Filter controls the parser's behaviors.
//
// NOTE: Filter is not safe for concurrent use.
type Filter interface {
	Key(key Key) bool
	Type(typ Type) bool
	Database(db DB) bool

	Set(s *Set)
	List(l *List)
	Hash(h *Hash)
	String(s *String)
	SortedSet(s *SortedSet)
}

// Key represents a redis key.
type Key struct {
	Encoding byte
	DB       int
	Expiry   int
	Key      string

	p      *Parser
	memory uint64
}

// Skip sets next item's skipping strategy.
func (k Key) Skip(strategy int) {
	k.p.strategy.running = strategy
}

// Type represents a redis type.
type Type struct {
	Encoding byte

	p *Parser
}

// Skip sets next item's skipping strategy.
func (t Type) Skip(strategy int) {
	t.p.strategy.running = strategy
}

// DB represents a redis database.
type DB struct {
	p *Parser

	Num int
}

// Skip sets next database's skip strategy.
func (db DB) Skip(strategy int) {
	db.p.setStrategy(strategy)
}

// Set represents redis set.
type Set struct {
	Key    Key
	Values map[interface{}]struct{}
	memory uint64
}

// Memory reports memory used by s.
func (s Set) Memory() uint64 {
	return s.Key.memory + s.memory
}

// List represents redis list.
type List struct {
	Key    Key
	Values []string
	memory uint64
}

// Memory reports memory used by l.
func (l List) Memory() uint64 {
	return l.Key.memory + l.memory
}

// Hash represents redis hash.
type Hash struct {
	Key    Key
	Values map[string]string
	memory uint64
}

// Memory reports memory used by l.
func (h Hash) Memory() uint64 {
	return h.Key.memory + h.memory
}

// String represents redis sds.
type String struct {
	Key    Key
	Value  string
	memory uint64
}

// Memory reports memory used by s.
func (s String) Memory() uint64 {
	return s.Key.memory + s.memory
}

// SortedSet represents redis sortedset.
type SortedSet struct {
	Key    Key
	Values map[string]float64
	memory uint64
}

// Memory reports memory used by ss.
func (ss SortedSet) Memory() uint64 {
	return ss.Key.memory + ss.memory
}

var (
	redisTypePool = &sync.Pool{
		New: func() interface{} {
			return new(redisType)
		},
	}
	valuePool = &sync.Pool{
		New: func() interface{} {
			return new(value)
		},
	}
)

type redisType struct {
	key    Key
	values []*value

	i interface{}
}

func (rt *redisType) reset() {
	i := rt.i
	rt.i = nil
	for _, value := range rt.values {
		value.reset()
	}
	redisTypePool.Put(i)
}

func (rt *redisType) decompress() (err error) {
	for _, v := range rt.values {
		if v.c {
			v.b, err = readLZF(v.b, len(v.b), v.l)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (rt *redisType) set(set *Set) error {
	set.memory = 0
	set.Key = rt.key
	set.Values = make(map[interface{}]struct{})
	switch set.Key.Encoding {
	case EncodingSet:
		set.memory += _overhead.hash(len(rt.values))
		values := rt.values
		for i := 0; i < len(values); i++ {
			set.memory += values[i].m + _overhead.hashEntry() + _overhead.root()
			if b := values[i].b; b != nil {
				set.Values[bytes2string(b)] = struct{}{}
			}
		}
	case EncodingIntset:
		set.memory += uint64(rt.values[0].l)
		inset, err := rt.values[0].readIntset()
		if err != nil {
			return err
		}
		for _, v := range inset {
			set.Values[v] = struct{}{}
		}
	}
	return nil
}

func (rt *redisType) list(list *List) (err error) {
	list.memory = 0
	list.Key = rt.key
	switch list.Key.Encoding {
	case EncodingList:
		list.Values = make([]string, len(rt.values))
		values := rt.values
		list.memory += _overhead.linkedlist()
		for i := 0; i < len(values); i++ {
			list.memory += values[i].m + _overhead.linkedlistEntry() + _overhead.root()
			if b := values[i].b; b != nil {
				list.Values[i] = bytes2string(b)
			}
		}
	case EncodingZiplist:
		list.memory += uint64(rt.values[0].l)
		list.Values, err = rt.values[0].readZiplist()
		if err != nil {
			return err
		}
	case EncodingQuicklist:
		list.memory += _overhead.quicklist(len(rt.values))
		for _, value := range rt.values {
			list.memory += uint64(value.l)
			values, err := value.readZiplist()
			if err != nil {
				return err
			}
			list.Values = append(list.Values, values...)
		}
	}
	return nil
}

func (rt *redisType) hash(hash *Hash) error {
	hash.memory = 0
	hash.Key = rt.key
	hash.Values = make(map[string]string)
	switch hash.Key.Encoding {
	case EncodingHashZip:
		hash.memory += uint64(rt.values[0].l)
		values, err := rt.values[0].readZiplist()
		if err != nil {
			return err
		}
		for i := 0; i < len(values); i += 2 {
			hash.Values[values[i]] = values[i+1]
		}
	case EncodingZipmap:
		hash.memory += uint64(rt.values[0].l)
		values, err := rt.values[0].readZipmap()
		if err != nil {
			return err
		}
		for i := 0; i < len(values); i += 2 {
			hash.Values[values[i]] = values[i+1]
		}
	case EncodingHash:
		hash.memory += _overhead.hash(len(rt.values) / 2)
		values := rt.values
		for i := 0; i < len(values); i += 2 {
			hash.memory += values[i].m + values[i+1].m + _overhead.hashEntry() + 2*_overhead.root()
			if k, v := values[i].b, values[i+1].b; k != nil && v != nil {
				hash.Values[bytes2string(k)] = bytes2string(v)
			}
		}
	}
	return nil
}

func (rt *redisType) string(s *String) *String {
	s.memory = 0
	s.Key = rt.key
	if b := rt.values[0].b; b != nil {
		s.Value = bytes2string(b)
	}
	s.memory = rt.values[0].m
	return s
}

func (rt *redisType) sortedset(ss *SortedSet) error {
	ss.memory = 0
	ss.Key = rt.key
	ss.Values = make(map[string]float64)
	switch ss.Key.Encoding {
	case EncodingSortedSet, EncodingSortedSet2:
		ss.memory += _overhead.skiplist(len(rt.values) / 2)
		values := rt.values
		for i := 0; i < len(values); i += 2 {
			ss.memory += values[i].m + 8 + _overhead.root() + _overhead.skiplistEntry()
			if k := values[i].b; k != nil {
				ss.Values[bytes2string(k)] = values[i+1].f
			}
		}
	case EncodingSortedSetZip:
		ss.memory += uint64(rt.values[0].l)
		values, err := rt.values[0].readZiplist()
		if err != nil {
			return err
		}
		for i := 0; i < len(values); i += 2 {
			f, err := strconv.ParseFloat(values[i+1], 64)
			if err != nil {
				return err
			}
			ss.Values[values[i]] = f
		}
	}
	return nil
}

type value struct {
	c bool
	l int
	m uint64
	f float64
	b []byte
	i interface{}
}

func newValue(c bool, l int, m uint64, b []byte) *value {
	i := valuePool.Get()
	v := i.(*value)
	v.c = c
	v.l = l
	v.m = m
	v.b = b
	v.i = i
	return v
}

func newDoubleValue(f float64) *value {
	i := valuePool.Get()
	v := i.(*value)
	v.m = 8
	v.f = f
	return v
}

func (v *value) reset() {
	i := v.i
	v.l = 0
	v.m = 0
	v.b = nil
	v.c = false
	valuePool.Put(i)
}

func (v *value) readZiplist() ([]string, error) {
	if v.b == nil {
		return nil, nil
	}

	// <zlbytes><zltail><zllen><entry><entry><zlend>

	r := &MemReader{b: v.b}
	// zlbytes: 4 byte unsigned integer in little endian format
	r.Discard(4)

	// zltail: 4 byte unsigned integer in little endian format
	r.Discard(4)

	// zllen: 2 byte unsigned integer in little endian format
	zllen, err := r.little16()
	if err != nil {
		return nil, err
	}

	values := make([]string, zllen)
	for j := 0; j < int(zllen); j++ {
		// <length-prev-entry><special-flag><raw-bytes-of-entry>
		b, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		if b == 0xfe {
			// ignore length-prev-entry
			// if b == 254, next 4 bytes are used to store the length
			r.Discard(4)
		}

		first, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		switch first >> 6 {
		case 0:
			// 00: 6 bits string value length
			values[j], err = r.readString(int(first) & 0x3f)
			if err != nil {
				return nil, err
			}
		case 1:
			// 01: 14 bits string value length
			second, err := r.ReadByte()
			if err != nil {
				return nil, err
			}
			l := int(second) | (int(first)&0x3f)<<8
			values[j], err = r.readString(l)
			if err != nil {
				return nil, err
			}
		case 2:
			// 10: 4 bytes string value length
			l, err := r.big32()
			if err != nil {
				return nil, err
			}
			values[j], err = r.readString(l)
			if err != nil {
				return nil, err
			}
		case 3:
			switch (first >> 4) & 0x03 {
			case 0:
				// 1100: 2 bytes as a 16 bit signed integer
				i16, err := r.little16()
				if err != nil {
					return nil, err
				}
				values[j] = strconv.Itoa(i16)
			case 1:
				// 1101: 4 bytes as a 32 bit signed integer
				i32, err := r.little32()
				if err != nil {
					return nil, err
				}
				values[j] = strconv.Itoa(i32)
			case 2:
				// 1110: 8 bytes as a 64 bit signed integer
				i64, err := r.little64()
				if err != nil {
					return nil, err
				}
				values[j] = strconv.Itoa(i64)
			}
			fallthrough
		default:
			switch {
			case first == 240:
				// 11110000: 3 bytes as a 24 bit signed integer
				bs, err := r.ReadBytes(3)
				if err != nil {
					return nil, err
				}
				i32 := uint32(bs[2])<<24 | uint32(bs[1])<<16 | uint32(bs[0])<<8
				values[j] = strconv.Itoa(int(int32(i32) >> 8))
			case first == 254:
				// 11111110: 1 bytes as an 8 bit signed integer
				b, err = r.ReadByte()
				if err != nil {
					return nil, err
				}
				values[j] = strconv.Itoa(int(int8(b)))
			case first >= 241 && first <= 253:
				// 1111xxxx: 4 bit unsigned integer(0 - 12)
				values[j] = strconv.Itoa(int(first) - 241)
			}
		}
	}
	// zlend: always 255

	return values, nil
}

func (v *value) readZipmap() ([]string, error) {
	if v.b == nil {
		return nil, nil
	}

	// <zmlen><len>"foo"<len><free>"bar"<len>"hello"<len><free>"world"<zmend>

	r := &MemReader{b: v.b}
	zmlen, err := r.ReadByte()
	if err != nil {
		return nil, err
	}

	var str string
	var values []string
	if zmlen <= 254 {
		values = make([]string, zmlen*2)
	} else {
		values = make([]string, 512*2)
	}
	for {
		str, err = readZipmapEntry(r, false)
		if err != nil {
			return nil, err
		}
		values = append(values, str)

		str, err = readZipmapEntry(r, true)
		if err != nil {
			return nil, err
		}
		values = append(values, str)

		if r.i < len(r.b) && r.b[r.i] == 255 {
			// zmend: always 255
			return values, nil
		}
	}
}

func (v *value) readIntset() ([]int, error) {
	if v.b == nil {
		return nil, nil
	}

	// <encoding><length-of-contents><contents>

	r := &MemReader{b: v.b}
	// encoding: 32 bit unsigned integer
	encoding, err := r.little32()
	if err != nil {
		return nil, err
	}

	// length-of-contents: 32 bit unsigned integer
	lengthOfContents, err := r.little32()
	if err != nil {
		return nil, err
	}

	values := make([]int, lengthOfContents)
	for j := 0; j < int(lengthOfContents); j++ {
		switch encoding {
		case 2:
			values[j], err = r.little16()
		case 4:
			values[j], err = r.little32()
		case 8:
			values[j], err = r.little64()
		}
		if err != nil {
			return nil, err
		}
	}
	return values, nil
}
