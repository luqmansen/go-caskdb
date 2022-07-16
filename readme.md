# go-caskdb

[![codecov](https://codecov.io/gh/luqmansen/go-caskdb/branch/master/graph/badge.svg)](https://codecov.io/gh/luqmansen/go-caskdb)
[![Actions Status](https://github.com/luqmansen/go-caskdb/actions/workflows/test.yml/badge.svg)](https://github.com/luqmansen/go-caskdb/actions/workflows/test.yml)

[Riak's Bitcask paper](https://riak.com/assets/bitcask-intro.pdf) implementation in Golang

## Todo

- [ ] Implement key deletion
- [ ] Implement CRC
- [ ] Implement Max file size
- [ ] Implement Log Merging
  - [ ] Implement merge trigger
    - [ ] Fragmentation
    - [ ] Dead bytes
  - [ ] Implement merge interval
- [ ] Add support for ranged query

## Benchmark

| Ops                             | Result                                                      |
|---------------------------------|-------------------------------------------------------------|
| Unbuffered Write                | `BenchmarkDiskStorage_Set-8   	  651841	      1737 ns/op`
| Buffered Write                  | `BenchmarkDiskStorage_Set-8   	 2569089	       501.8 ns/op` |
| Buffered Write + Sync after set | `BenchmarkDiskStorage_Set-8   	    7879	    313756 ns/op`

## Credits

This repo is inspired by [py-caskdb](https://github.com/avinassh/py-caskdb/)