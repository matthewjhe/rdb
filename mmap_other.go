// +build !linux

package rdb

import "io/ioutil"

func mmap(filename string) ([]byte, error) {
	return ioutil.ReadFile(filename)
}
