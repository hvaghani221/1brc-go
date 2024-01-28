package main

import (
	"strconv"
	"testing"
)

var floats [][]byte = [][]byte{
	[]byte("12.3"),
	[]byte("-12.3"),
	[]byte("2.3"),
	[]byte("-2.3"),
}

func BenchmarkFloatStrconv(b *testing.B) {
	var total float64

	for i := 0; i < b.N; i++ {
		for _, f := range floats {
			res, _ := strconv.ParseFloat(string(f), 64)
			total += res
		}
	}
	b.Log(total)
}

func BenchmarkFloatCustom(b *testing.B) {
	var total int64

	for i := 0; i < b.N; i++ {
		for _, f := range floats {
			var number int64

			negative := false
			if f[0] == '-' {
				negative = true
				f = f[1:]
			}

			if f[1] == '.' {
				// 1.2\n
				number = int64(f[0])*10 + int64(f[2]) - '0'*(10+1)
			} else if f[2] == '.' {
				// 12.3\n
				number = int64(f[0])*100 + int64(f[1])*10 + int64(f[3]) - '0'*(100+10+1)
			} else {
				panic("unexpected position: " + string(f))
			}

			if negative {
				number = -number
			}

			total += number

		}
	}
	b.Log(float64(total) / 10.0)
}
