package rdb

import (
	"bytes"
	"io/ioutil"
	"testing"
)

func TestMmap(t *testing.T) {
	const filename = "mmap_test.go"
	got, err := mmap(filename)
	if err != nil {
		t.Fatal(err)
	}
	want, err := ioutil.ReadFile(filename)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("got: %v, want: %v", got, want)
	}
}
