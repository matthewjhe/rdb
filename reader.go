package rdb

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"
)

// Reader is the interface that wraps the operations against rdb data.
//
// Discard skips the next n bytes.
// ReadByte reads and returns a single byte.
// ReadBytes reads and returns exactly n bytes.
type Reader interface {
	numberReader

	Discard(n int)
	ReadByte() (byte, error)
	ReadBytes(n int) ([]byte, error)
}

// numberReader is the interface that converts byte sequences into number.
type numberReader interface {
	big32() (int, error)
	big64() (int, error)
	little16() (int, error)
	little32() (int, error)
	little64() (int, error)
}

// MemReader is a Reader that reads rdb data from memory.
type MemReader struct {
	i int
	b []byte
}

// NewMemReader memory-maps the named file and returns a MemReader that reads from it.
func NewMemReader(file string) (Reader, error) {
	b, err := mmap(file)
	if err != nil {
		return nil, err
	}
	return &MemReader{b: b}, nil
}

// Discard skips the next n bytes.
func (r *MemReader) Discard(n int) {
	r.i += n
}

// ReadByte reads and returns a single byte.
// If no byte is available, returns an error.
func (r *MemReader) ReadByte() (byte, error) {
	if 1 > len(r.b[r.i:]) {
		return 0, io.ErrUnexpectedEOF
	}
	r.i++
	return r.b[r.i-1], nil
}

// ReadBytes reads and returns exactly n bytes.
// If fewer than n bytes is available, returns an error.
//
// NOTE: It's not safe to modify the returned slice.
func (r *MemReader) ReadBytes(n int) ([]byte, error) {
	if n > len(r.b[r.i:]) {
		return nil, io.ErrUnexpectedEOF
	}
	r.i += n
	return r.b[r.i-n : r.i], nil
}

func (r *MemReader) readString(n int) (string, error) {
	b, err := r.ReadBytes(n)
	if err != nil {
		return "", err
	}
	return bytes2string(b), nil
}

// helper funcs that converts byte sequences into number.

func (r *MemReader) little16() (int, error) {
	b, err := r.ReadBytes(2)
	if err != nil {
		return 0, err
	}
	return int(int16(binary.LittleEndian.Uint16(b))), nil
}

func (r *MemReader) little32() (int, error) {
	b, err := r.ReadBytes(4)
	if err != nil {
		return 0, err
	}
	return int(int32(binary.LittleEndian.Uint32(b))), nil
}

func (r *MemReader) little64() (int, error) {
	b, err := r.ReadBytes(8)
	if err != nil {
		return 0, err
	}
	return int(int64(binary.LittleEndian.Uint64(b))), nil
}

func (r *MemReader) big32() (int, error) {
	b, err := r.ReadBytes(4)
	if err != nil {
		return 0, err
	}
	return int(int32(binary.BigEndian.Uint32(b))), nil
}

func (r *MemReader) big64() (int, error) {
	b, err := r.ReadBytes(8)
	if err != nil {
		return 0, err
	}
	return int(int64(binary.BigEndian.Uint64(b))), nil
}

// BufferReader is a Reader that reads from a *bufio.Reader.
type BufferReader struct {
	*bufio.Reader

	buf  [8]byte
	file *os.File
}

// NewBufferReader returns a new BufferReader reading from file.
// It's buffer has at least the specified size. If size == 0, use default size.
func NewBufferReader(file string, size int) (Reader, error) {
	if size == 0 {
		size = 4096
	}
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	return &BufferReader{
		file:   f,
		Reader: bufio.NewReaderSize(f, size),
	}, nil
}

// Close closes the file.
func (r *BufferReader) Close() error {
	if r.file != nil {
		f := r.file
		r.file = nil
		return f.Close()
	}
	return nil
}

// Discard skips the next n bytes.
func (r *BufferReader) Discard(n int) {
	r.Reader.Discard(n)
}

// ReadBytes reads and returns exactly n bytes.
// If ReadBytes reads fewer than n bytes, it also returns an error.
func (r *BufferReader) ReadBytes(n int) ([]byte, error) {
	buf := make([]byte, n)
	_, err := io.ReadFull(r, buf)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

// helper funcs that converts byte sequences into number.

func (r *BufferReader) readBytes(n int) ([]byte, error) {
	_, err := io.ReadFull(r, r.buf[:n])
	if err != nil {
		return nil, err
	}
	return r.buf[:n], nil
}

func (r *BufferReader) little16() (int, error) {
	b, err := r.readBytes(2)
	if err != nil {
		return 0, err
	}
	return int(int16(binary.LittleEndian.Uint16(b))), nil
}

func (r *BufferReader) little32() (int, error) {
	b, err := r.readBytes(4)
	if err != nil {
		return 0, err
	}
	return int(int32(binary.LittleEndian.Uint32(b))), nil
}

func (r *BufferReader) little64() (int, error) {
	b, err := r.readBytes(8)
	if err != nil {
		return 0, err
	}
	return int(int64(binary.LittleEndian.Uint64(b))), nil
}

func (r *BufferReader) big32() (int, error) {
	b, err := r.readBytes(4)
	if err != nil {
		return 0, err
	}
	return int(int32(binary.BigEndian.Uint32(b))), nil
}

func (r *BufferReader) big64() (int, error) {
	b, err := r.readBytes(8)
	if err != nil {
		return 0, err
	}
	return int(int64(binary.BigEndian.Uint64(b))), nil
}
