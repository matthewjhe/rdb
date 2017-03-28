package rdb

import (
	"math/rand"
	"sort"
	"time"
	"unsafe"

	"github.com/pkg/errors"
)

// Encoding2Type returns the actual redis type of encoding.
func Encoding2Type(encoding byte) string {
	switch encoding {
	case EncodingString:
		return TypeString
	case EncodingList, EncodingZiplist, EncodingQuicklist:
		return TypeList
	case EncodingSet, EncodingIntset:
		return TypeSet
	case EncodingSortedSet, EncodingSortedSet2, EncodingSortedSetZip:
		return TypeSortedSet
	case EncodingHash, EncodingZipmap, EncodingHashZip:
		return TypeHash
	}
	return "unknown"
}

// Encoding2String returns the string format of encoding.
func Encoding2String(encoding byte) string {
	switch encoding {
	case EncodingString:
		return "string"
	case EncodingList:
		return "linkedlist"
	case EncodingZiplist:
		return "ziplist"
	case EncodingQuicklist:
		return "quicklist"
	case EncodingSet:
		return "hashtable"
	case EncodingIntset:
		return "intset"
	case EncodingSortedSet:
		return "skiplist"
	case EncodingSortedSet2:
		return "skiplist"
	case EncodingSortedSetZip:
		return "ziplist"
	case EncodingHash:
		return "hashtable"
	case EncodingZipmap:
		return "zipmap"
	case EncodingHashZip:
		return "ziplist"
	}
	return "unknown"
}

func bytes2string(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func readLZF(buf []byte, clen, ulen int) ([]byte, error) {
	if clen > len(buf) {
		return nil, nil
	}

	ip, op := 0, 0
	out := make([]byte, ulen)
	for ip < clen {
		ctrl := int(buf[ip])
		ip++

		if ctrl < (1 << 5) {
			ctrl++
			if ctrl+ip > clen || ctrl+op > ulen {
				return nil, errors.WithStack(ErrInvalidCompressedData)
			}

			copy(out[op:op+ctrl], buf[ip:ip+ctrl])
			ip += ctrl
			op += ctrl
			continue
		}

		length := ctrl >> 5
		ref := op - ((ctrl & 0x1f) << 8) - 1

		if length == 7 {
			length += int(buf[ip])
			ip++

			if ip >= clen {
				return nil, errors.WithStack(ErrInvalidCompressedData)
			}
		}

		ref -= int(buf[ip])
		ip++
		length += 2
		if op+length > ulen || ref < 0 {
			return nil, errors.WithStack(ErrInvalidCompressedData)
		}

		for i := 0; i < length; i++ {
			out[op+i] = out[ref+i]
		}
		op += length
	}

	return out[:op], nil
}

func readZipmapEntry(r *MemReader, value bool) (string, error) {
	b, err := r.ReadByte()
	if err != nil {
		return "", err
	}

	switch {
	case b < 254:
		if value {
			n, err := r.ReadByte()
			if err != nil {
				return "", err
			}
			if n > 0 {
				r.Discard(int(n))
			}
		}
		return r.readString(int(b))
	case b == 254:
		l, err := r.little32()
		if err != nil {
			return "", err
		}
		if value {
			n, err := r.ReadByte()
			if err != nil {
				return "", err
			}
			if n > 0 {
				r.Discard(int(n))
			}
		}
		return r.readString(l)
	default:
		return "", errors.WithStack(ErrInvalidZipmapEntry)
	}
}

type overhead struct{}

func (o overhead) alloc(l int) uint64 {
	switch {
	case l < 1<<5:
		return o.jemalloc(l + 2)
	case l < 1<<8:
		return o.jemalloc(l + 2 + 2)
	case l < 1<<16:
		return o.jemalloc(l + 2 + 4)
	case l < 1<<32:
		return o.jemalloc(l + 2 + 8)
	default:
		return o.jemalloc(l + 2 + 16)
	}
}

func (o overhead) top(expiry int) uint64 {
	// Each top level object is an entry in a dictionary, and so we have to include
	// the overhead of a dictionary entry
	return o.hashEntry() + o.root() + o.expiry(expiry)
}

func (o overhead) hashEntry() uint64 {
	// See https://github.com/antirez/redis/blob/unstable/src/dict.h
	// Each dictEntry has 2 pointers + int64
	return 2*o.arch() + 8
}

func (o overhead) root() uint64 {
	return o.arch() + 8
}

func (o overhead) expiry(expiry int) uint64 {
	if expiry < 0 {
		return 0
	}
	// Key expiry is stored in a hashtable, so we have to pay for the cost of a hashtable entry
	// The timestamp itself is stored as an int64, which is a 8 bytes
	return o.hashEntry() + 8
}

func (o overhead) skiplist(size int) uint64 {
	return 2*o.arch() + o.hash(size) + 2*o.arch() + 16
}

func (o overhead) skiplistEntry() uint64 {
	return o.hashEntry() + 2*o.arch() + 8 + (o.arch()+8)*o.zsetRandomLevel()
}

func (o overhead) zsetRandomLevel() uint64 {
	level := uint64(1)
	rint := rand.Intn(0xFFFF)
	max := 0.25 * 0xFFFF
	for float64(rint) < max {
		level++
		rint = rand.Intn(0xFFFF)
	}
	if level < 32 {
		return level
	}
	return 32
}

func (o overhead) hash(size int) uint64 {
	return 4 + 7*o.arch() + 4*o.arch() + uint64(float64(o.nextpow(size)*o.arch())*1.5)
}

func (o overhead) nextpow(size int) uint64 {
	power := uint64(1)
	for power <= uint64(size) {
		power <<= 1
	}
	return power
}

func (o overhead) quicklist(size int) uint64 {
	return 2*o.arch() + o.arch() + 2*4 + (4*o.arch()+o.arch()+2*4)*uint64(size)
}

func (o overhead) linkedlist() uint64 {
	return o.arch() + 5*o.arch()
}

func (o overhead) linkedlistEntry() uint64 {
	return 3 * o.arch()
}

func (o overhead) arch() uint64 {
	// 64bit only
	return 8
}

func (o overhead) jemalloc(size int) uint64 {
	i := sort.Search(len(allocSpec), func(i int) bool { return allocSpec[i] >= uint64(size) })
	if i > len(allocSpec) {
		return uint64(size)
	}
	return allocSpec[i]
}

var allocSpec = []uint64{
	8, 16, 24, 32, 40, 48, 56, 64, 80, 96, 112, 128, 160, 192, 224, 256, 320, 384, 448, 512, 640, 768, 896, 1024,
	1280, 1536, 1792, 2048, 2560, 3072, 3584, 4096, 5120, 6144, 7168, 8192, 10240, 12288, 14336, 16384, 20480, 24576,
	28672, 32768, 40960, 49152, 57344, 65536, 81920, 98304, 114688, 131072, 163840, 196608, 229376, 262144, 327680,
	393216, 458752, 524288, 655360, 786432, 917504, 1048576, 1310720, 1572864, 1835008, 2097152, 2621440, 3145728,
	3670016, 4194304, 5242880, 6291456, 7340032, 8388608, 10485760, 12582912, 14680064, 16777216, 20971520, 25165824,
	29360128, 33554432, 41943040, 50331648, 58720256, 67108864, 83886080, 100663296, 117440512, 134217728, 167772160,
	201326592, 234881024, 268435456, 335544320, 402653184, 469762048, 536870912, 671088640, 805306368, 939524096,
	1073741824, 1342177280, 1610612736, 1879048192, 2147483648, 2684354560, 3221225472, 3758096384, 4294967296,
	5368709120, 6442450944, 7516192768, 8589934592, 10737418240, 12884901888, 15032385536, 17179869184, 21474836480,
	25769803776, 30064771072, 34359738368, 42949672960, 51539607552, 60129542144, 68719476736, 85899345920,
	103079215104, 120259084288, 137438953472, 171798691840, 206158430208, 240518168576, 274877906944, 343597383680,
	412316860416, 481036337152, 549755813888, 687194767360, 824633720832, 962072674304, 1099511627776, 1374389534720,
	1649267441664, 1924145348608, 2199023255552, 2748779069440, 3298534883328, 3848290697216, 4398046511104,
	5497558138880, 6597069766656, 7696581394432, 8796093022208, 10995116277760, 13194139533312, 15393162788864,
	17592186044416, 21990232555520, 26388279066624, 30786325577728, 35184372088832, 43980465111040, 52776558133248,
	61572651155456, 70368744177664, 87960930222080, 105553116266496, 123145302310912, 140737488355328, 175921860444160,
	211106232532992, 246290604621824, 281474976710656, 351843720888320, 422212465065984, 492581209243648,
	562949953421312, 703687441776640, 844424930131968, 985162418487296, 1125899906842624, 1407374883553280,
	1688849860263936, 1970324836974592, 2251799813685248, 2814749767106560, 3377699720527872, 3940649673949184,
	4503599627370496, 5629499534213120, 6755399441055744, 7881299347898368, 9007199254740992, 11258999068426240,
	13510798882111488, 15762598695796736, 18014398509481984, 22517998136852480, 27021597764222976, 31525197391593472,
	36028797018963968, 45035996273704960, 54043195528445952, 63050394783186944, 72057594037927936, 90071992547409920,
	108086391056891904, 126100789566373888, 144115188075855872, 180143985094819840, 216172782113783808,
	252201579132747776, 288230376151711744, 360287970189639680, 432345564227567616, 504403158265495552,
	576460752303423488, 720575940379279360, 864691128455135232, 1008806316530991104, 1152921504606846976,
	1441151880758558720, 1729382256910270464, 2017612633061982208, 2305843009213693952, 2882303761517117440,
	3458764513820540928, 4035225266123964416, 4611686018427387904, 5764607523034234880, 6917529027641081856,
	8070450532247928832, 9223372036854775808, 11529215046068469760, 13835058055282163712, 16140901064495857664,
}

var _overhead overhead

func init() {
	rand.Seed(time.Now().Unix())
}
