package go_caskdb

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func Test_HeaderDecoding(t *testing.T) {
	t.Parallel()

	type args struct {
		timestamp uint32
		keySize   uint32
		valueSize uint32
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "key & value dataLength 1B",
			args: args{
				timestamp: uint32(time.Now().Unix()),
				keySize:   1,
				valueSize: 1,
			},
		},
		{
			name: "key & value dataLength 1B + 1 bit",
			args: args{
				timestamp: uint32(time.Now().Unix()),
				keySize:   256,
				valueSize: 256,
			},
		},
		{
			name: "key & value dataLength 4B",
			args: args{
				timestamp: uint32(time.Now().Nanosecond()),
				keySize:   4294967295,
				valueSize: 4294967295,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := encodeHeader(tt.args.timestamp, tt.args.keySize, tt.args.valueSize)
			ts, ks, vs := decodeHeader(b)
			assert.Equal(t, tt.args.timestamp, ts)
			assert.Equal(t, tt.args.keySize, ks)
			assert.Equal(t, tt.args.valueSize, vs)
			assert.Equal(t, 12, len(b)) // encoded header should exactly 12 Byte in length
		})
	}
}

func Test_encodeKV(t *testing.T) {
	t.Parallel()

	type args struct {
		timestamp uint32
		key       string
		value     string
	}
	tests := []struct {
		name string
		args args
		want int64
	}{
		{
			name: "normal kv",
			args: args{
				timestamp: uint32(time.Now().UnixNano()),
				key:       "hello",
				value:     "world",
			},
			want: headerLength + 10,
		},
		{
			name: "empty kv",
			args: args{
				timestamp: uint32(time.Now().UnixNano()),
				key:       "",
				value:     "",
			},
			want: headerLength + 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dataLength, data := encodeKV(tt.args.timestamp, tt.args.key, tt.args.value)
			assert.Equal(t, tt.want, dataLength)
			ts, k, v := decodeKV(data)
			assert.Equal(t, tt.args.timestamp, ts)
			assert.Equal(t, tt.args.key, k)
			assert.Equal(t, tt.args.value, v)
		})
	}
}
