package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"regexp"
	"runtime"
	"strconv"
	"time"

	"github.com/matthewjhe/rdb"
)

var (
	f        filter
	patterns strings

	b = flag.Int("b", 0, "Read buffer size.")
	m = flag.Int64("m", 1<<30, "Maximum memory mapping size.")
	o = flag.String("o", "", "Output file.")
)

type strings []string

func (s *strings) Set(v string) error {
	*s = append(*s, v)
	return nil
}

func (s *strings) String() string {
	return ""
}

type ints []int

func (i *ints) Set(v string) error {
	i32, err := strconv.ParseInt(v, 10, 32)
	if err != nil {
		return err
	}
	*i = append(*i, int(i32))
	return nil
}

func (i *ints) String() string {
	return ""
}

type filter struct {
	dbs      ints
	keys     strings
	types    strings
	patterns []*regexp.Regexp

	debug   bool
	file    string
	out     io.Writer
	writeCh chan string

	dbAbort  bool
	keyAbort bool
}

func (f *filter) error(err error) {
	if f.debug {
		fmt.Fprintf(os.Stderr, "%+v\n", err)
		os.Exit(2)
	}
	fmt.Fprintln(os.Stderr, err)
	os.Exit(2)
}

func (f *filter) Key(key rdb.Key) bool {
	if len(f.keys) == 0 && len(f.patterns) == 0 {
		return f.keyAbort
	}
	for i, k := range f.keys {
		if k == key.Key {
			if len(f.keys) == 1 {
				f.keyAbort = true
			}
			f.keys = append(f.keys[0:i], f.keys[i+1:]...)
			return false
		}
	}
	for _, p := range f.patterns {
		if p.MatchString(key.Key) {
			return false
		}
	}
	key.Skip(rdb.SkipAll)
	return false
}

func (f *filter) Type(typ rdb.Type) bool {
	if len(f.types) == 0 {
		return false
	}
	for _, t := range f.types {
		if rdb.Encoding2Type(typ.Encoding) == t {
			return false
		}
	}
	typ.Skip(rdb.SkipAll)
	return false
}

func (f *filter) Database(db rdb.DB) bool {
	if len(f.dbs) == 0 {
		return f.dbAbort
	}
	for i, d := range f.dbs {
		if db.Num == d {
			if len(f.dbs) == 1 {
				f.dbAbort = true
			}
			f.dbs = append(f.dbs[0:i], f.dbs[i+1:]...)
			return false
		}
	}
	db.Skip(rdb.SkipAll)
	return false
}

func (f *filter) Set(v *rdb.Set) {
	f.writeCh <- fmt.Sprintf(
		"%v,%v,%v,%v,%v",
		v.Key.DB,
		rdb.Encoding2Type(v.Key.Encoding),
		rdb.Encoding2String(v.Key.Encoding),
		strconv.Quote(v.Key.Key),
		v.Memory(),
	)
}

func (f *filter) List(v *rdb.List) {
	f.writeCh <- fmt.Sprintf(
		"%v,%v,%v,%v,%v",
		v.Key.DB,
		rdb.Encoding2Type(v.Key.Encoding),
		rdb.Encoding2String(v.Key.Encoding),
		strconv.Quote(v.Key.Key),
		v.Memory(),
	)
}

func (f *filter) Hash(v *rdb.Hash) {
	f.writeCh <- fmt.Sprintf(
		"%v,%v,%v,%v,%v",
		v.Key.DB,
		rdb.Encoding2Type(v.Key.Encoding),
		rdb.Encoding2String(v.Key.Encoding),
		strconv.Quote(v.Key.Key),
		v.Memory(),
	)
}

func (f *filter) String(v *rdb.String) {
	f.writeCh <- fmt.Sprintf(
		"%v,%v,%v,%v,%v",
		v.Key.DB,
		rdb.Encoding2Type(v.Key.Encoding),
		rdb.Encoding2String(v.Key.Encoding),
		strconv.Quote(v.Key.Key),
		v.Memory(),
	)
}

func (f *filter) SortedSet(v *rdb.SortedSet) {
	f.writeCh <- fmt.Sprintf(
		"%v,%v,%v,%v,%v",
		v.Key.DB,
		rdb.Encoding2Type(v.Key.Encoding),
		rdb.Encoding2String(v.Key.Encoding),
		strconv.Quote(v.Key.Key),
		v.Memory(),
	)
}

func (f *filter) batchWrite() <-chan struct{} {
	wait := make(chan struct{})
	f.writeCh = make(chan string, 512)
	f.writeCh <- "db,type,encoding,key,mem"
	go func() {
		timer := time.NewTimer(200 * time.Millisecond)
		defer timer.Stop()

		var ok bool
		var str, next string
		for {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(200 * time.Millisecond)

		BATCH:
			for i := 0; i < 100; i++ {
				select {
				case next, ok = <-f.writeCh:
					if ok {
						str += next + "\n"
					} else {
						break BATCH
					}
				default:
					select {
					case next, ok = <-f.writeCh:
						if ok {
							str += next + "\n"
						} else {
							break BATCH
						}
					case <-timer.C:
						break BATCH
					}
				}
			}

			io.WriteString(f.out, str)
			if !ok {
				close(wait)
				return
			}
			str, next = "", ""
		}
	}()
	return wait
}

func main() {
	flag.Parse()
	if f.file == "" {
		fmt.Fprintf(os.Stderr, "Usage: %s [options] -f /path/to/dump.rdb\n", os.Args[0])
		fmt.Fprintln(os.Stderr)
		fmt.Fprintf(os.Stderr, "Options:\n\n")
		flag.PrintDefaults()
		os.Exit(1)
	}
	if len(patterns) != 0 {
		for _, p := range patterns {
			f.patterns = append(f.patterns, regexp.MustCompile(p))
		}
	}

	var r rdb.Reader
	fi, err := os.Stat(f.file)
	if err != nil {
		f.error(err)
	}
	if fi.Size() > *m {
		r, err = rdb.NewBufferReader(f.file, *b)
	} else {
		r, err = rdb.NewMemReader(f.file)
	}
	if err != nil {
		f.error(err)
	}

	f.out = os.Stdout
	if *o != "" {
		of, err := os.OpenFile(*o, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0666)
		if err != nil {
			f.error(err)
		}
		defer of.Close()

		f.out = of
	}

	wait := f.batchWrite()
	strategy := rdb.WithStrategy(rdb.SkipExpiry | rdb.SkipMeta | rdb.SkipValue)
	if err := rdb.Parse(r, rdb.WithFilter(&f), strategy); err != nil {
		f.error(err)
	}
	close(f.writeCh)
	<-wait
}

func init() {
	flag.StringVar(&f.file, "f", "", "Redis RDB file path.")
	flag.BoolVar(&f.debug, "d", false, "Enable debug output.")
	flag.Var(&f.keys, "k", "Keys to inspect. Multiple keys can provided.")
	flag.Var(&f.types, "t", "Types to inspect. Multiple types can provided.")
	flag.Var(&f.dbs, "db", "Databases to inspect. Multiple databases can provided.")
	flag.Var(&patterns, "p", "Key match patterns. Multiple patterns can provided.")

	if cpu := runtime.NumCPU(); cpu == 1 {
		runtime.GOMAXPROCS(3)
	} else {
		runtime.GOMAXPROCS(cpu + 1)
	}
}
