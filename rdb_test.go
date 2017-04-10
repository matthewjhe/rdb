package rdb

import (
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/pkg/errors"
)

const (
	debug = false
)

func TestMain(m *testing.M) {
	data, err := ioutil.ReadFile("testdata/.20kbytes")
	if err != nil {
		log.Fatal(err)
	}
	_20kbytes = strings.TrimSpace(string(data))
	initTestCases()
	os.Exit(m.Run())
}

func TestParse(t *testing.T) {
	for i, test := range testParseCases {
		test.reset()
		mem, err := NewMemReader(test.file)
		if err != nil {
			t.Fatal(err, i)
		}
		if got := Parse(mem, test.options...); errors.Cause(got) != test.want {
			t.Fatalf("index: %v, got: %+v, want: %v", i, got, test.want)
		}
		test.validate(t)

		test.reset()
		buffer, err := NewBufferReader(test.file, 0)
		if err != nil {
			t.Fatal(err, i)
		}
		if got := Parse(buffer, test.options...); errors.Cause(got) != test.want {
			t.Fatalf("index: %v, got: %+v, want: %v", i, got, test.want)
		}
		test.validate(t)
	}
}

var testParseCases []testParseCase

func add(tests ...testParseCase) {
	testParseCases = append(testParseCases, tests...)
}

func initTestCases() {
	bigString := &stringMapFilter{
		want: map[string]string{
			"20kbytes": _20kbytes,
			"40kbytes": _20kbytes + _20kbytes,
			"80kbytes": _20kbytes + _20kbytes + _20kbytes + _20kbytes,
		},
		wantEncoding: "string",
	}
	add(testParseCase{
		want:       nil,
		file:       "testdata/dumps/big_string.rdb",
		options:    []ParseOption{WithFilter(bigString)},
		validators: []validator{bigString},
	})

	quicklist := &listFilter{
		total:        1806,
		wantEncoding: "quicklist",
		in:           []string{"1470323953955869026", "1470323959757084081"},
	}
	add(testParseCase{
		want:       nil,
		file:       "testdata/dumps/quicklist.rdb",
		options:    []ParseOption{WithFilter(quicklist)},
		validators: []validator{quicklist},
	})

	version8 := &sortedsetFilter{
		want: map[string]float64{
			"finalfield": 2.718,
		},
		wantEncoding: "skiplist",
	}
	add(testParseCase{
		want:       nil,
		file:       "testdata/dumps/rdb_version_8_with_64b_length_and_scores.rdb",
		options:    []ParseOption{WithFilter(version8)},
		validators: []validator{version8},
	})

	version5 := &stringMapFilter{
		want: map[string]string{
			"abcd":         "efgh",
			"abcdef":       "abcdef",
			"foo":          "bar",
			"bar":          "baz",
			"longerstring": "thisisalongerstring.idontknowwhatitmeans",
		},
		wantEncoding: "string",
	}
	add(testParseCase{
		want:       nil,
		file:       "testdata/dumps/rdb_version_5_with_checksum.rdb",
		options:    []ParseOption{WithFilter(version5)},
		validators: []validator{version5},
	})

	sortedsetAsZiplist := &sortedsetFilter{
		want: map[string]float64{
			"8b6ba6718a786daefa69438148361901": 1,
			"cb7a24bb7528f934b841b34c3a73e0c7": 2.37,
			"523af537946b79c4f8369ed39ba78605": 3.423,
		},
		wantEncoding: "ziplist",
	}
	add(testParseCase{
		want:       nil,
		file:       "testdata/dumps/sorted_set_as_ziplist.rdb",
		options:    []ParseOption{WithFilter(sortedsetAsZiplist)},
		validators: []validator{sortedsetAsZiplist},
	})

	set := &setFilter{
		want: map[interface{}]struct{}{
			"alpha": {},
			"beta":  {},
			"gamma": {},
			"delta": {},
			"phi":   {},
			"kappa": {},
		},
		wantEncoding: "hashtable",
	}
	add(testParseCase{
		want:       nil,
		file:       "testdata/dumps/regular_set.rdb",
		options:    []ParseOption{WithFilter(set)},
		validators: []validator{set},
	})

	intset64 := &setFilter{
		want: map[interface{}]struct{}{
			0x7ffefffefffefffe: {},
			0x7ffefffefffefffd: {},
			0x7ffefffefffefffc: {},
		},
		wantEncoding: "intset",
	}
	add(testParseCase{
		want:       nil,
		file:       "testdata/dumps/intset_64.rdb",
		options:    []ParseOption{WithFilter(intset64)},
		validators: []validator{intset64},
	})

	intset32 := &setFilter{
		want: map[interface{}]struct{}{
			0x7ffefffe: {},
			0x7ffefffd: {},
			0x7ffefffc: {},
		},
		wantEncoding: "intset",
	}
	add(testParseCase{
		want:       nil,
		file:       "testdata/dumps/intset_32.rdb",
		options:    []ParseOption{WithFilter(intset32)},
		validators: []validator{intset32},
	})

	intset16 := &setFilter{
		want: map[interface{}]struct{}{
			0x7ffe: {},
			0x7ffd: {},
			0x7ffc: {},
		},
		wantEncoding: "intset",
	}
	add(testParseCase{
		want:       nil,
		file:       "testdata/dumps/intset_16.rdb",
		options:    []ParseOption{WithFilter(intset16)},
		validators: []validator{intset16},
	})

	linkedlist := &listFilter{
		total:        1000,
		wantEncoding: "linkedlist",
		in:           []string{"JYY4GIFI0ETHKP4VAJF5333082J4R1UPNPLE329YT0EYPGHSJQ", "TKBXHJOX9Q99ICF4V78XTCA2Y1UYW6ERL35JCIL1O0KSGXS58S"},
	}
	add(testParseCase{
		want:       nil,
		file:       "testdata/dumps/linkedlist.rdb",
		options:    []ParseOption{WithFilter(linkedlist)},
		validators: []validator{linkedlist},
	})

	expectedNums := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, -2, 13, 25, -61, 63, 16380, -16000, 65535, -65523, 4194304, 0x7fffffffffffffff}
	ziplistWithInteger := &listFilter{
		wantEncoding: "ziplist",
	}
	for _, v := range expectedNums {
		ziplistWithInteger.want = append(ziplistWithInteger.want, strconv.Itoa(v))
	}
	add(testParseCase{
		want:       nil,
		file:       "testdata/dumps/ziplist_with_integers.rdb",
		options:    []ParseOption{WithFilter(ziplistWithInteger)},
		validators: []validator{ziplistWithInteger},
	})

	ziplistSimple := &listFilter{
		wantEncoding: "ziplist",
		want:         []string{"aj2410", "cc953a17a8e096e76a44169ad3f9ac87c5f8248a403274416179aa9fbd852344"},
	}
	add(testParseCase{
		want:       nil,
		file:       "testdata/dumps/ziplist_that_doesnt_compress.rdb",
		options:    []ParseOption{WithFilter(ziplistSimple)},
		validators: []validator{ziplistSimple},
	})

	ziplistCompression := &listFilter{
		wantEncoding: "ziplist",
		want:         []string{"aaaaaa", "aaaaaaaaaaaa", "aaaaaaaaaaaaaaaaaa", "aaaaaaaaaaaaaaaaaaaaaaaa", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
	}
	add(testParseCase{
		want:       nil,
		file:       "testdata/dumps/ziplist_that_compresses_easily.rdb",
		options:    []ParseOption{WithFilter(ziplistCompression)},
		validators: []validator{ziplistCompression},
	})

	dictionary := &stringMapFilter{
		want: map[string]string{
			"ZMU5WEJDG7KU89AOG5LJT6K7HMNB3DEI43M6EYTJ83VRJ6XNXQ": "T63SOS8DQJF0Q0VJEZ0D1IQFCYTIPSBOUIAI9SB0OV57MQR1FI",
			"UHS5ESW4HLK8XOGTM39IK1SJEUGVV9WOPK6JYA5QBZSJU84491": "6VULTCV52FXJ8MGVSFTZVAGK2JXZMGQ5F8OVJI0X6GEDDR27RZ",
		},
		wantEncoding: "hashtable",
	}
	add(testParseCase{
		want:       nil,
		file:       "testdata/dumps/dictionary.rdb",
		options:    []ParseOption{WithFilter(dictionary)},
		validators: []validator{dictionary},
	})

	hashAsZiplist := &stringMapFilter{
		want: map[string]string{
			"a":     "aa",
			"aa":    "aaaa",
			"aaaaa": "aaaaaaaaaaaaaa",
		},
		wantEncoding: "ziplist",
	}
	add(testParseCase{
		want:       nil,
		file:       "testdata/dumps/hash_as_ziplist.rdb",
		options:    []ParseOption{WithFilter(hashAsZiplist)},
		validators: []validator{hashAsZiplist},
	})

	zipmapBigValue := &stringMapFilter{
		want: map[string]string{
			"253bytes": _253bytes,
			"254bytes": _254bytes,
			"255bytes": _255bytes,
			"300bytes": _300bytes,
			"20kbytes": _20kbytes,
		},
		wantEncoding: "zipmap",
	}
	add(testParseCase{
		want:       nil,
		file:       "testdata/dumps/zipmap_with_big_values.rdb",
		options:    []ParseOption{WithFilter(zipmapBigValue)},
		validators: []validator{zipmapBigValue},
	})

	zipmapSimple := &stringMapFilter{
		want: map[string]string{
			"MKD1G6": "2",
			"YNNXK":  "F7TI",
		},
		wantEncoding: "zipmap",
	}
	add(testParseCase{
		want:       nil,
		file:       "testdata/dumps/zipmap_that_doesnt_compress.rdb",
		options:    []ParseOption{WithFilter(zipmapSimple)},
		validators: []validator{zipmapSimple},
	})

	zipmapCompression := &stringMapFilter{
		want: map[string]string{
			"a":     "aa",
			"aa":    "aaaa",
			"aaaaa": "aaaaaaaaaaaaaa",
		},
		wantEncoding: "zipmap",
	}
	add(testParseCase{
		want:       nil,
		file:       "testdata/dumps/zipmap_that_compresses_easily.rdb",
		options:    []ParseOption{WithFilter(zipmapCompression)},
		validators: []validator{zipmapCompression},
	})

	stringKeyWithCompression := &stringMapFilter{
		want: map[string]string{
			strings.Repeat("a", 200): "Key that redis should compress easily",
		},
		wantEncoding: "string",
	}
	add(testParseCase{
		want:       nil,
		file:       "testdata/dumps/easily_compressible_string_key.rdb",
		options:    []ParseOption{WithFilter(stringKeyWithCompression)},
		validators: []validator{stringKeyWithCompression},
	})

	integerKeys := &stringMapFilter{
		want: map[string]string{
			strconv.Itoa(125):         "Positive 8 bit integer",
			strconv.Itoa(0xABAB):      "Positive 16 bit integer",
			strconv.Itoa(0x0AEDD325):  "Positive 32 bit integer",
			strconv.Itoa(-123):        "Negative 8 bit integer",
			strconv.Itoa(-0x7325):     "Negative 16 bit integer",
			strconv.Itoa(-0x0AEDD325): "Negative 32 bit integer",
		},
		wantEncoding: "string",
	}
	add(testParseCase{
		want:       nil,
		file:       "testdata/dumps/integer_keys.rdb",
		options:    []ParseOption{WithFilter(integerKeys)},
		validators: []validator{integerKeys},
	})

	keysWithExpiry := &keysWithExpiryFilter{
		want: map[string]int{
			"expires_ms_precision": 1671963072573,
		},
	}
	add(testParseCase{
		want:       nil,
		file:       "testdata/dumps/keys_with_expiry.rdb",
		options:    []ParseOption{WithFilter(keysWithExpiry)},
		validators: []validator{keysWithExpiry},
	})

	multipleDatabase := &multipleDatabaseFilter{
		want: []int{0, 2},
	}
	add(testParseCase{
		want: nil,
		file: "testdata/dumps/multiple_databases.rdb",
		options: []ParseOption{
			EnableSync(),
			WithStrategy(0),
			WithFilter(multipleDatabase),
		},
		validators: []validator{multipleDatabase},
	})

	add(testParseCase{
		want: nil,
		file: "testdata/dumps/empty_database.rdb",
	})
}

// multiple databases

type multipleDatabaseFilter struct {
	testEmptyFilter

	got      []int
	want     []int
	wantOnly map[int]struct{}
}

func (f *multipleDatabaseFilter) Database(db DB) bool {
	if _, ok := f.wantOnly[db.Num]; !ok && len(f.wantOnly) > 0 {
		return false
	}
	f.got = append(f.got, db.Num)
	return false
}

func (f *multipleDatabaseFilter) reset() {
	if f.got != nil {
		f.got = f.got[:0]
	}
}

func (f *multipleDatabaseFilter) validate(t *testing.T) {
	if len(f.got) != len(f.want) {
		t.Fatalf("want: %v, got: %v", f.want, f.got)
	}
	for i, w := range f.want {
		if f.got[i] != w {
			t.Fatalf("want: %v, got: %v", f.want, f.got)
		}
	}
}

// keys with expiry

type keysWithExpiryFilter struct {
	testEmptyFilter

	got  map[string]int
	want map[string]int
}

func (f *keysWithExpiryFilter) String(s *String) {
	f.Lock()
	defer f.Unlock()
	if f.got == nil {
		f.got = make(map[string]int)
	}
	f.got[s.Key.Key] = s.Key.Expiry
}

func (f *keysWithExpiryFilter) reset() {
	for k := range f.got {
		delete(f.got, k)
	}
}

func (f *keysWithExpiryFilter) validate(t *testing.T) {
	for k, v := range f.want {
		if g, ok := f.got[k]; !ok || g != v {
			t.Fatalf("key: %v, want: %v, got: %v", k, v, g)
		}
	}
}

// string map

type stringMapFilter struct {
	testEmptyFilter

	got  map[string]string
	want map[string]string

	gotEncoding  string
	wantEncoding string
}

func (f *stringMapFilter) String(s *String) {
	f.Lock()
	defer f.Unlock()
	if f.got == nil {
		f.got = make(map[string]string)
	}
	f.gotEncoding = Encoding2String(s.Key.Encoding)
	f.got[s.Key.Key] = s.Value
}

func (f *stringMapFilter) Hash(h *Hash) {
	f.Lock()
	defer f.Unlock()
	if f.got == nil {
		f.got = make(map[string]string)
	}
	f.gotEncoding = Encoding2String(h.Key.Encoding)
	for k, v := range h.Values {
		f.got[k] = v
	}
}

func (f *stringMapFilter) reset() {
	for k := range f.got {
		delete(f.got, k)
	}
}

func (f *stringMapFilter) validate(t *testing.T) {
	for k, v := range f.want {
		if g, ok := f.got[k]; !ok || g != v {
			t.Fatalf("key: %v, want: %v, got: %v", k, v, g)
		}
	}
	if f.wantEncoding != "" && f.wantEncoding != f.gotEncoding {
		t.Fatalf("want: %v, got: %v", f.wantEncoding, f.gotEncoding)
	}
}

// list

type listFilter struct {
	testEmptyFilter

	got  []string
	want []string

	in    []string
	total int

	gotEncoding  string
	wantEncoding string
}

func (f *listFilter) List(l *List) {
	f.Lock()
	defer f.Unlock()
	f.gotEncoding = Encoding2String(l.Key.Encoding)
	for _, v := range l.Values {
		f.got = append(f.got, v)
	}
}

func (f *listFilter) reset() {
	if f.got != nil {
		f.got = f.got[:0]
	}
}

func (f *listFilter) validate(t *testing.T) {
	for i, v := range f.want {
		if f.got[i] != v {
			t.Fatalf("want: %v, got: %v", f.want, f.got)
		}
	}
	if f.wantEncoding != "" && f.wantEncoding != f.gotEncoding {
		t.Fatalf("want: %v, got: %v", f.wantEncoding, f.gotEncoding)
	}
	if len(f.in) == 0 {
		return
	}
	if f.total != len(f.got) {
		t.Fatalf("want: %v, got: %v", f.total, len(f.got))
	}
	for _, w := range f.in {
		found := false
		for _, g := range f.got {
			if g == w {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("want: %v, got: %v", f.want, f.got)
		}
	}
}

// set

type setFilter struct {
	testEmptyFilter

	got  map[interface{}]struct{}
	want map[interface{}]struct{}

	gotEncoding  string
	wantEncoding string
}

func (f *setFilter) Set(s *Set) {
	f.Lock()
	defer f.Unlock()
	if f.got == nil {
		f.got = make(map[interface{}]struct{})
	}
	f.gotEncoding = Encoding2String(s.Key.Encoding)
	for k, v := range s.Values {
		f.got[k] = v
	}
}

func (f *setFilter) reset() {
	for k := range f.got {
		delete(f.got, k)
	}
}

func (f *setFilter) validate(t *testing.T) {
	for k, v := range f.want {
		if g, ok := f.got[k]; !ok || g != v {
			t.Fatalf("key: %v, want: %v, got: %v", k, v, g)
		}
	}
	if f.wantEncoding != "" && f.wantEncoding != f.gotEncoding {
		t.Fatalf("want: %v, got: %v", f.wantEncoding, f.gotEncoding)
	}
}

// sortedset

type sortedsetFilter struct {
	testEmptyFilter

	got  map[string]float64
	want map[string]float64

	gotEncoding  string
	wantEncoding string
}

func (f *sortedsetFilter) SortedSet(ss *SortedSet) {
	f.Lock()
	defer f.Unlock()
	if f.got == nil {
		f.got = make(map[string]float64)
	}
	f.gotEncoding = Encoding2String(ss.Key.Encoding)
	for k, v := range ss.Values {
		f.got[k] = v
	}
}

func (f *sortedsetFilter) reset() {
	for k := range f.got {
		delete(f.got, k)
	}
}

func (f *sortedsetFilter) validate(t *testing.T) {
	for k, v := range f.want {
		if g, ok := f.got[k]; !ok || g != v {
			t.Fatalf("key: %v, want: %v, got: %v", k, v, g)
		}
	}
	if f.wantEncoding != "" && f.wantEncoding != f.gotEncoding {
		t.Fatalf("want: %v, got: %v", f.wantEncoding, f.gotEncoding)
	}
}

// test helpers

type testEmptyFilter struct {
	sync.Mutex
	t *testing.T
}

func (f *testEmptyFilter) Key(k Key) bool     { return false }
func (f *testEmptyFilter) Type(_ Type) bool   { return false }
func (f *testEmptyFilter) Database(_ DB) bool { return false }
func (f *testEmptyFilter) Set(v *Set) {
	if debug {
		log.Println("set:", v.Key.Key, v.Values)
	}
}
func (f *testEmptyFilter) List(v *List) {
	if debug {
		log.Println("list:", v.Key.Key, v.Values)
	}
}
func (f *testEmptyFilter) Hash(v *Hash) {
	if debug {
		log.Println("hash:", v.Key.Key, v.Values)
	}
}
func (f *testEmptyFilter) String(v *String) {
	if debug {
		log.Println("string:", v.Key.Key, v.Value)
	}
}
func (f *testEmptyFilter) SortedSet(v *SortedSet) {
	if debug {
		log.Println("sortedset:", v.Key.Key, v.Values)
	}
}

type validator interface {
	reset()
	validate(t *testing.T)
}

type testParseCase struct {
	want       error
	file       string
	options    []ParseOption
	validators []validator
}

func (tc testParseCase) reset() {
	for _, v := range tc.validators {
		v.reset()
	}
}

func (tc testParseCase) validate(t *testing.T) {
	for _, v := range tc.validators {
		v.validate(t)
	}
}

// test data

var (
	_253bytes = `NYKK5QA4TDYJFZH0FCVT39DWI89IH7HV9HV162MULYY9S6H67MGS6YZJ54Q2NISW9U69VC6ZK3OJV6J095P0P5YNSEHGCBJGYNZ8BPK3GEFBB8ZMGPT2Y33WNSETHINMSZ4VKWUE8CXE0Y9FO7L5ZZ02EO26TLXF5NUQ0KMA98973QY62ZO1M1WDDZNS25F37KGBQ8W4R5V1YJRR2XNSQKZ4VY7GW6X038UYQG30ZM0JY1NNMJ12BKQPF2IDQ`
	_254bytes = `IZ3PNCQQV5RG4XOAXDN7IPWJKEK0LWRARBE3393UYD89PSQFC40AG4RCNW2M4YAVJR0WD8AVO2F8KFDGUV0TGU8GF8M2HZLZ9RDX6V0XKIOXJJ3EMWQGFEY7E56RAOPTA60G6SQRZ59ZBUKA6OMEW3K0LH464C7XKAX3K8AXDUX63VGX99JDCW1W2KTXPQRN1R1PY5LXNXPW7AAIYUM2PUKN2YN2MXWS5HR8TPMKYJIFTLK2DNQNGTVAWMULON`
	_255bytes = `6EUW8XSNBHMEPY991GZVZH4ITUQVKXQYL7UBYS614RDQSE7BDRUW00M6Y4W6WUQBDFVHH6V2EIAEQGLV72K4UY7XXKL6K6XH6IN4QVS15GU1AAH9UI40UXEA8IZ5CZRRK6SAV3R3X283O2OO9KG4K0DG0HZX1MLFDQHXGCC96M9YUVKXOEC5X35Q4EKET0SDFDSBF1QKGAVS9202EL7MP2KPOYAUKU1SZJW5OP30WAPSM9OG97EBHW2XOWGICZG`
	_300bytes = `IJXP54329MQ96A2M28QF6SFX3XGNWGAII3M32MSIMR0O478AMZKNXDUYD5JGMHJRB9A85RZ3DC3AIS62YSDW2BDJ97IBSH7FKOVFWKJYS7XBMIBX0Z1WNLQRY7D27PFPBBGBDFDCKL0FIOBYEADX6G5UK3B0XYMGS0379GRY6F0FY5Q9JUCJLGOGDNNP8XW3SJX2L872UJZZL8G871G9THKYQ2WKPFEBIHOOTIGDNWC15NL5324W8FYDP97JHKCSMLWXNMSTYIUE7F22ZGR4NZK3T0UTBZ2AFRCT5LMT3P6B`
	_20kbytes string
)
