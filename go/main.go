package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"runtime"
	"sort"
	"sync"
	"time"
)

var (
	bufferPool  chan []byte
	workerCount int = 2
	multiplier  int = 2
	queueSize   int = workerCount * multiplier
)

const bufferSize = 1024 * 256

type measurement struct {
	min, max, total, count int64
}

func (m measurement) print(name string) string {
	return fmt.Sprintf("%s=%.1f/%.1f/%.1f", name, toFloat(m.min), toFloat(m.total)/float64(m.count), toFloat(m.max))
}

func toFloat(n int64) float64 {
	return float64(n) / 10.0
}

func main() {
	if len(os.Args) != 2 {
		panic("Missing filename")
	}

	// profile, err := os.Create("runtime.prof")
	// if err != nil {
	// 	panic(err)
	// }
	//
	// pprof.WriteHeapProfile(profile)
	// defer pprof.StopCPUProfile()

	filename := os.Args[1]
	file, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	workerCount = runtime.NumCPU()
	queueSize = workerCount * multiplier

	bufferPool = make(chan []byte, queueSize)

	result := processFile(file)
	printResult(os.Stdout, result)
}

func processFile(file *os.File) map[string]*measurement {
	startTime := time.Now()
	stat, err := file.Stat()
	if err != nil {
		panic(err)
	}

	fileSize := stat.Size()
	prev := make([]byte, 0, 1024)
	bytesRead := 0

	var wg sync.WaitGroup

	queue := make(chan []byte, workerCount*multiplier)
	result := make([]*hashmap, workerCount)

	wg.Add(workerCount)
	for i := 0; i < workerCount; i++ {
		i := i
		go func() {
			result[i] = processQueue(queue)
			wg.Done()
		}()
	}

	for {
		buffer := getBuffer()
		buffer = append(buffer, prev...)
		tmpBuffer := buffer[len(prev):cap(buffer)]

		n, err := file.Read(tmpBuffer)
		if err == io.EOF {
			break
		}
		bytesRead += n
		buffer = buffer[:n+len(prev)]

		lastIndex := bytes.LastIndexByte(buffer, '\n')
		prev = prev[:len(buffer)-lastIndex-1]
		copy(prev, buffer[lastIndex+1:])

		// memState := runtime.MemStats{}
		// runtime.ReadMemStats(&memState)
		queue <- buffer[0 : lastIndex+1]
		fmt.Fprintf(os.Stderr, "\rTime: %20s, Completed: %.2f%%, Speed: %.2f MB/s", // Heap: %0.2f MB, GC: %d
			time.Since(startTime),
			float64(bytesRead)/float64(fileSize)*100.0,
			float64(bytesRead)/1024.0/1024.0/float64(time.Since(startTime).Seconds()),
			// float64(memState.Alloc)/1024.0/1024.0,
			// memState.NumGC,
		)
	}
	close(queue)
	wg.Wait()
	// fmt.Fprintf(os.Stderr, "\r")

	finalResult := make(map[string]*measurement, 1000)

	for i := 0; i < workerCount; i++ {
		for j, entry := range result[i].values {
			name := string(result[i].name[j])
			if e := finalResult[name]; e == nil {
				entry := entry
				finalResult[name] = &entry
			} else {
				e.min = min(e.min, entry.min)
				e.max = max(e.max, entry.max)
				e.count += entry.count
				e.total += entry.total
			}
		}
	}

	return finalResult
}

func processQueue(queue chan []byte) *hashmap {
	hashmap := newHashmap(1 << 12)
	for buffer := range queue {
		processChunk(buffer, hashmap)
		go putBuffer(buffer)
	}

	return hashmap
}

func processChunk(chunk []byte, hashmap *hashmap) {
	for len(chunk) > 0 {
		i := 0
		for chunk[i] != ';' {
			i += 1
		}
		name := chunk[0:i]
		hash := fnv1a(name)
		chunk = chunk[i+1:]
		i = 0
		var number int64
		{
			negative := false
			if chunk[i] == '-' {
				negative = true
				chunk = chunk[1:]
			}

			if chunk[1] == '.' {
				// 1.2\n
				number = int64(chunk[0])*10 + int64(chunk[2]) - '0'*(10+1)
				chunk = chunk[4:]
			} else if chunk[2] == '.' {
				// 12.3\n
				number = int64(chunk[0])*100 + int64(chunk[1])*10 + int64(chunk[3]) - '0'*(100+10+1)
				chunk = chunk[5:]
			} else {
				panic("unexpected position: " + string(chunk[0:100]))
			}

			if negative {
				number = -number
			}
		}

		// e := result[name]
		if _, e := hashmap.get(hash); e == nil {
			e := measurement{
				min:   number,
				max:   number,
				total: number,
				count: 1,
			}
			// result[name] = e
			hashmap.add(hash, name, e)
		} else {
			e.count += 1
			e.total += number
			e.min = min(e.min, number)
			e.max = max(e.max, number)
		}

	}
}

const (
	FNVPrime    uint64 = 1099511628211
	OffsetBasis uint64 = 14695981039346656037
)

func fnv1a(name []byte) uint64 {
	hash := OffsetBasis
	for _, b := range name {
		hash ^= uint64(b)
		hash *= FNVPrime
	}

	return hash
}

func printResult(w io.Writer, result map[string]*measurement) {
	names := make([]string, 0, len(result))
	for name := range result {
		names = append(names, name)
	}
	sort.Strings(names)
	fmt.Fprintf(w, "{")
	i := 0
	for ; i < len(names)-1; i += 1 {
		fmt.Fprint(w, result[names[i]].print(names[i]))
		fmt.Fprintf(w, ", ")
	}
	fmt.Fprint(w, result[names[i]].print(names[i]))
	fmt.Fprintf(w, "}\n")
}

func getBuffer() []byte {
	select {
	case buffer := <-bufferPool:
		return buffer
	default:
		return make([]byte, 0, bufferSize)
	}
}

func putBuffer(buffer []byte) {
	buffer = buffer[:0]
	bufferPool <- buffer
}

type hashentry struct {
	key   uint64
	index int64
}
type hashmap struct {
	table  [][]hashentry
	name   [][]byte
	values []measurement
	size   int
}

func newHashmap(size int) *hashmap {
	return &hashmap{
		table:  make([][]hashentry, size),
		name:   make([][]byte, 0, size*4),
		values: make([]measurement, 0, size*4),
		size:   size,
	}
}

func (h *hashmap) add(key uint64, name []byte, value measurement) {
	i := key & uint64(h.size-1)

	if h.table[i] == nil {
		h.table[i] = make([]hashentry, 0, 16)
	}

	h.table[i] = append(h.table[i], hashentry{
		key:   key,
		index: int64(len(h.values)),
	})

	nameCopy := make([]byte, len(name))
	copy(nameCopy, name)

	h.name = append(h.name, nameCopy)
	h.values = append(h.values, value)
}

func (h *hashmap) get(key uint64) ([]byte, *measurement) {
	i := key & uint64(h.size-1)

	for _, e := range h.table[i] {
		if e.key == key {
			return h.name[e.index], &h.values[e.index]
		}
	}
	return nil, nil
}
