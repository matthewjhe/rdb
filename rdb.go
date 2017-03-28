package rdb

import (
	"bytes"
	"encoding/binary"
	stderr "errors"
	"fmt"
	"io"
	"log"
	"math"
	"regexp"
	"runtime"
	"strconv"
	"sync"

	"github.com/pkg/errors"
)

const (
	tokenAUX     = 0xFA // information about the RDB generated
	tokenResize  = 0xFB // hint about the size of the keys in the currently selected database
	tokenExpMSec = 0xFC // expiry time in ms
	tokenExpSec  = 0xFD // expiry time in seconds
	tokenDB      = 0xFE // database selector
	tokenEOF     = 0xFF // end of RDB file
)

// Parse strategies
const (
	SkipMeta = 1 << iota
	SkipExpiry
	SkipValue
	SkipAll
)

// Parse errors
var (
	ErrInvalidRDB            = stderr.New("Invalid RDB file")
	ErrUnsupportedRDB        = stderr.New("Unsupported RDB version")
	ErrInvalidZipmapEntry    = stderr.New("Invalid zipmap entry")
	ErrInvalidLengthEncoding = stderr.New("Invalid length encoding")
	ErrInvalidCompressedData = stderr.New("Invalid compressed data")
)

// ParseOption configures the behaviors when parsing a rdb file.
type ParseOption func(*Parser)

// WithFilter returns a ParseOption which sets a parse filter.
func WithFilter(filter Filter) ParseOption {
	return func(p *Parser) {
		p.filter = filter
	}
}

// WithStrategy returns a ParseOption which sets default parse strategy.
func WithStrategy(strategy int) ParseOption {
	return func(p *Parser) {
		p.setStrategy(strategy)
	}
}

const (
	filterBufferSize = 512
)

// EnableSync returns a ParseOption which disable async filtering.
func EnableSync() ParseOption {
	return func(p *Parser) {
		p.sync = make(chan *redisType, filterBufferSize)
	}
}

// state represents parser's current state.
type state struct {
	skip       bool   // skipping current key or value?
	compressed bool   // current key or value is compressed?
	memory     uint64 // current key or value memory usage
}

// state represents parser's skipping strategy.
type strategy struct {
	running int
	global  int
}

// Parser represents a Redis RDB parser.
type Parser struct {
	sync.WaitGroup
	Reader

	version  string
	filter   Filter
	state    state
	strategy strategy

	sync  chan *redisType
	async chan *redisType

	err     chan error
	sizeint uint64
}

// Parse parses a Redis RDB file.
func Parse(r Reader, opts ...ParseOption) error {
	p := new(Parser)
	p.Reader = r
	p.err = make(chan error, 1)
	p.sizeint = 8

	// "REDIS" string
	magic, err := p.readString(5)
	if err != nil {
		return err
	}
	if magic != "REDIS" {
		return errors.WithStack(ErrInvalidRDB)
	}

	version, err := p.readString(4)
	if err != nil {
		return err
	}
	v, err := strconv.Atoi(version)
	if err != nil {
		return err
	}
	if v < 1 || v > 8 {
		return errors.WithStack(ErrUnsupportedRDB)
	}
	p.version = version

	for _, opt := range opts {
		opt(p)
	}

	if p.filter != nil {
		ch := p.sync
		workers := 1
		if p.sync == nil {
			p.async = make(chan *redisType, filterBufferSize)
			ch = p.async
			workers = runtime.NumCPU()
		}
		if workers == 1 {
			// at least on worker
			workers++
		}

		for i := 1; i < workers; i++ {
			p.Add(1)
			go p.filterWorker(ch)
		}
	}
	return p.Parse()
}

func (p *Parser) filterWorker(ch <-chan *redisType) {
	var (
		set       = new(Set)
		list      = new(List)
		hash      = new(Hash)
		sds       = new(String)
		sortedset = new(SortedSet)
	)

	defer p.Done()

	for {
		rt, ok := <-ch
		if !ok {
			return
		}
		if err := rt.decompress(); err != nil {
			p.close(err)
			return
		}
		switch Encoding2Type(rt.key.Encoding) {
		case TypeSet:
			if err := rt.set(set); err != nil {
				p.close(err)
				return
			}
			p.filter.Set(set)
		case TypeList:
			if err := rt.list(list); err != nil {
				p.close(err)
				return
			}
			p.filter.List(list)
		case TypeHash:
			if err := rt.hash(hash); err != nil {
				p.close(err)
				return
			}
			p.filter.Hash(hash)
		case TypeString:
			p.filter.String(rt.string(sds))
		case TypeSortedSet:
			if err := rt.sortedset(sortedset); err != nil {
				p.close(err)
				return
			}
			p.filter.SortedSet(sortedset)
		}

		rt.reset()
	}
}

func (p *Parser) close(err error) {
	select {
	case p.err <- err:
	default:
	}
}

func (p *Parser) skipStage(strategies ...int) bool {
	p.state.skip = false
	for _, strategy := range strategies {
		if p.strategy.running&strategy != 0 {
			p.state.skip = true
			return true
		}
	}
	return false
}

func (p *Parser) getMemory() uint64 {
	m := p.state.memory
	p.state.memory = 0
	return m
}

func (p *Parser) readString(n int) (string, error) {
	b, err := p.ReadBytes(n)
	if err != nil {
		return "", err
	}
	return bytes2string(b), nil
}

func (p *Parser) readRawString(memory bool) (string, error) {
	b, length, err := p.readRawBytes(memory)
	if err != nil {
		return "", err
	}
	if b == nil {
		return "", nil
	}
	if p.state.compressed {
		p.state.compressed = false
		b, err = readLZF(b, len(b), length)
		if err != nil {
			return "", err
		}
	}
	return bytes2string(b), nil
}

func (p *Parser) readValue(memory bool) (*value, error) {
	b, length, err := p.readRawBytes(memory)
	if err != nil {
		return nil, err
	}
	c := p.state.compressed
	p.state.compressed = false
	return newValue(c, length, p.getMemory(), b), nil
}

func (p *Parser) readDoubleValue(encoding byte) (*value, error) {
	f, err := p.readDouble(encoding)
	if err != nil {
		return nil, err
	}
	return newDoubleValue(f), nil
}

func (p *Parser) readDouble(encoding byte) (float64, error) {
	if encoding == EncodingSortedSet2 {
		if p.state.skip {
			p.Discard(8)
			return 0, nil
		}
		bs, err := p.ReadBytes(8)
		if err != nil {
			return 0, err
		}
		var f64 float64
		err = binary.Read(bytes.NewReader(bs), binary.LittleEndian, &f64)
		return f64, err
	}

	b, err := p.ReadByte()
	if err != nil {
		return 0, err
	}
	switch b {
	case 253:
		return math.NaN(), nil
	case 254:
		return math.Inf(0), nil
	case 255:
		return math.Inf(-1), nil
	default:
		if p.state.skip {
			p.Discard(int(b))
			return 0, nil
		}
		str, err := p.readString(int(b))
		if err != nil {
			return 0, err
		}
		var f float64
		fmt.Sscanf(str, "%lg", &f)
		return f, nil
	}
}

var (
	isInt = regexp.MustCompile("^(?:[-+]?(?:0|[1-9][0-9]*))$")
)

// readRawBytes reads a redis string from input.
// It returns the string as bytes format along with how much space it consumed.
// If memory is true, it calculates memory used in redis instance by this string.
func (p *Parser) readRawBytes(memory bool) ([]byte, int, error) {
	length, encoded, err := p.readLength(true)
	if err != nil {
		return nil, 0, err
	}

	if !encoded {
		bs, err := p.ReadBytes(length)
		if err != nil {
			return nil, 0, err
		}
		if memory {
			if length <= 32 && isInt.Match(bs) {
				p.state.memory = p.sizeint
			} else {
				p.state.memory = _overhead.alloc(length)
			}
		}
		if p.state.skip {
			return nil, length, nil
		}
		return bs, length, nil
	}

	switch length {
	case 3:
		return p.readCompressed(memory)
	case 2:
		// 1110: an 32 bit integer
		i32, err := p.little32()
		if err != nil {
			return nil, 0, err
		}
		var buf = make([]byte, 0, 11)
		bs, err := strconv.AppendInt(buf[:], int64(i32), 10), nil
		if err != nil {
			return nil, 0, err
		}
		p.state.memory += 8
		if p.state.skip {
			return nil, len(bs), nil
		}
		return bs, len(bs), nil
	case 1:
		// 1101: an 16 bit integer
		i16, err := p.little16()
		if err != nil {
			return nil, 0, err
		}
		var buf = make([]byte, 0, 6)
		bs, err := strconv.AppendInt(buf[:], int64(i16), 10), nil
		if err != nil {
			return nil, 0, err
		}
		if i16 < 0 || i16 >= 10000 {
			p.state.memory += 8
		}
		if p.state.skip {
			return nil, len(bs), nil
		}
		return bs, len(bs), nil
	case 0:
		// 1100: an 8 bit integer
		b, err := p.ReadByte()
		if err != nil {
			return nil, 0, err
		}
		var buf = make([]byte, 0, 4)
		bs, err := strconv.AppendInt(buf[:], int64(int8(b)), 10), nil
		if err != nil {
			return nil, 0, err
		}
		if int8(b) < 0 {
			p.state.memory += 8
		}
		if p.state.skip {
			return nil, len(bs), nil
		}
		return bs, len(bs), nil
	}
	return nil, 0, errors.WithStack(ErrInvalidLengthEncoding)
}

func (p *Parser) readCompressed(memory bool) ([]byte, int, error) {
	// <compressed-len><uncompressed-len><compressed-content>
	clen, _, err := p.readLength(false)
	if err != nil {
		return nil, 0, err
	}
	ulen, _, err := p.readLength(false)
	if err != nil {
		return nil, 0, err
	}

	if memory {
		p.state.memory += _overhead.alloc(ulen)
	}
	if p.state.skip {
		p.Discard(clen)
		return nil, ulen, nil
	}

	buf, err := p.ReadBytes(clen)
	if err != nil {
		return nil, 0, err
	}
	p.state.compressed = true
	return buf, ulen, nil
}

func (p *Parser) readLength(withEncoding bool) (int, bool, error) {
	first, err := p.ReadByte()
	if err != nil {
		return 0, false, err
	}
	switch first {
	case 0x80:
		i32, err := p.big32()
		if err != nil {
			return 0, false, err
		}
		return i32, false, nil
	case 0x81:
		i64, err := p.big64()
		if err != nil {
			return 0, false, err
		}
		return i64, false, nil
	default:
		switch first >> 6 {
		case 0:
			// 00: 6 bits
			return int(first & 0x3f), false, nil
		case 1:
			// 01: 14 bits
			next, err := p.ReadByte()
			if err != nil {
				return 0, false, err
			}
			return int(next) | int(first&0x3f)<<8, false, nil
		case 3:
			// 11: encoded in a special format
			if !withEncoding {
				return 0, false, errors.WithStack(ErrInvalidLengthEncoding)
			}
			return int(first & 0x3f), true, nil
		}
	}
	return 0, false, errors.WithStack(ErrInvalidLengthEncoding)
}

// Parse parses a Redis RDB file.
func (p *Parser) Parse() error {
	var (
		exp             = -1
		currentDB       = DB{p: p}
		currentKey      = Key{p: p}
		currentType     = Type{p: p}
		defaultStrategy = p.strategy.global
	)

	if c, ok := p.Reader.(io.Closer); ok {
		defer c.Close()
	}

	defer func() {
		if p.sync != nil {
			close(p.sync)
		}
		if p.async != nil {
			close(p.async)
		}
		p.Wait()
	}()

	for {
		select {
		case err := <-p.err:
			return err
		default:
		}

		b, err := p.ReadByte()
		if err != nil {
			return err
		}

		switch b {
		case tokenDB:
			// restore default strategy
			p.setStrategy(defaultStrategy)
			num, _, err := p.readLength(false)
			if err != nil {
				return err
			}
			currentKey.DB = num
			currentDB.Num = num
			if p.database(currentDB) {
				return nil
			}

		case tokenAUX:
			p.skipStage(SkipMeta, SkipAll)
			key, err := p.readRawString(false)
			if err != nil {
				return err
			}
			value, err := p.readRawString(false)
			if err != nil {
				return err
			}
			if !p.skipStage(SkipMeta, SkipAll) {
				fmt.Printf("AUX: %s: %s\n", key, value)
			}

		case tokenResize:
			dbSize, _, err := p.readLength(false)
			if err != nil {
				return err
			}
			expiresSize, _, err := p.readLength(false)
			if err != nil {
				return err
			}
			if !p.skipStage(SkipMeta, SkipAll) {
				fmt.Printf("db_size: %d, expires_size: %d\n", dbSize, expiresSize)
			}

		case tokenExpMSec:
			if p.skipStage(SkipExpiry, SkipAll) {
				p.Discard(8)
				break
			}
			exp, err = p.little64()
			if err != nil {
				return err
			}

		case tokenExpSec:
			if p.skipStage(SkipExpiry, SkipAll) {
				p.Discard(4)
				break
			}
			exp, err = p.little32()
			if err != nil {
				return err
			}

		case tokenEOF:
			return nil

		default:
			p.skipStage(SkipAll)
			currentType.Encoding = b
			if p.typ(currentType) {
				return nil
			}

			p.skipStage(SkipAll)
			key, err := p.readRawString(true)
			if err != nil {
				return err
			}
			currentKey.Key = key
			currentKey.Expiry = exp
			currentKey.Encoding = b
			currentKey.memory = p.getMemory() + _overhead.top(exp)
			if p.key(currentKey) {
				return nil
			}
			exp = -1

			p.skipStage(SkipValue, SkipAll)
			switch b {
			case EncodingString:
				value, err := p.readValue(true)
				if err != nil {
					return err
				}
				p.filterRedisType(currentKey, value)

			case EncodingSet, EncodingList:
				size, _, err := p.readLength(false)
				if err != nil {
					return err
				}

				if b == EncodingList {
					p.sizeint = 4
				}
				values := make([]*value, size)
				for i := 0; i < size; i++ {
					values[i], err = p.readValue(true)
					if err != nil {
						return err
					}
				}
				p.filterRedisType(currentKey, values...)

			case EncodingHash:
				size, _, err := p.readLength(false)
				if err != nil {
					return err
				}

				values := make([]*value, size*2)
				for i := 0; i < size*2; i++ {
					values[i], err = p.readValue(true)
					if err != nil {
						return err
					}
				}
				p.filterRedisType(currentKey, values...)

			case EncodingSortedSet, EncodingSortedSet2:
				size, _, err := p.readLength(false)
				if err != nil {
					return err
				}
				values := make([]*value, size*2)
				for i := 0; i < size; i++ {
					values[2*i], err = p.readValue(true)
					if err != nil {
						return err
					}
					values[2*i+1], err = p.readDoubleValue(b)
					if err != nil {
						return err
					}
				}
				p.filterRedisType(currentKey, values...)

			case EncodingZipmap, EncodingZiplist, EncodingHashZip,
				EncodingSortedSetZip, EncodingIntset:
				value, err := p.readValue(false)
				if err != nil {
					return err
				}
				p.filterRedisType(currentKey, value)

			case EncodingQuicklist:
				// quicklist ziplist size
				size, _, err := p.readLength(false)
				if err != nil {
					return err
				}
				values := make([]*value, size)
				for i := 0; i < size; i++ {
					values[i], err = p.readValue(false)
					if err != nil {
						return err
					}
				}
				p.filterRedisType(currentKey, values...)

			default:
				log.Printf("unsupported encoding: %d, %x\n", b, b)
				return nil
			}
		}
		// restore default state
		p.clearstate()
	}
}

func (p *Parser) clearstate() {
	p.state.memory = 0
	p.state.skip = false
	p.state.compressed = false
	p.strategy.running = p.strategy.global
	p.sizeint = 8
}

func (p *Parser) setStrategy(strategy int) {
	p.strategy.global = strategy
	p.strategy.running = strategy
}

// filter callback helpers

func (p *Parser) key(key Key) bool {
	if p.filter != nil && !p.state.skip {
		return p.filter.Key(key)
	}
	return false
}

func (p *Parser) typ(typ Type) bool {
	if p.filter != nil && !p.state.skip {
		return p.filter.Type(typ)
	}
	return false
}

func (p *Parser) database(db DB) bool {
	if p.filter != nil && !p.state.skip {
		return p.filter.Database(db)
	}
	return false
}

func (p *Parser) filterRedisType(key Key, values ...*value) {
	p.skipStage(SkipAll)
	if p.filter == nil || p.state.skip {
		return
	}
	i := redisTypePool.Get()
	rt := i.(*redisType)
	rt.key = key
	rt.values = values
	rt.i = i

	select {
	case p.async <- rt:
	case p.sync <- rt:
	case err := <-p.err:
		p.close(err)
	}
}
