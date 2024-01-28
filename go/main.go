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

type entry struct {
	min, max, total, count int64
}

func (e entry) print(name string) string {
	return fmt.Sprintf("%s=%.1f/%.1f/%.1f", name, toFloat(e.min), toFloat(e.total)/float64(e.count), toFloat(e.max))
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

func processFile(file *os.File) map[string]*entry {
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
	result := make([]map[string]*entry, workerCount)

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
	fmt.Fprintf(os.Stderr, "\r")

	finalResult := make(map[string]*entry, 1000)

	for i := 0; i < workerCount; i++ {
		for k, v := range result[i] {
			if e := finalResult[k]; e == nil {
				finalResult[k] = v
			} else {
				e.min = min(e.min, v.min)
				e.max = max(e.max, v.max)
				e.count += v.count
				e.total += v.total
			}
		}
	}

	return finalResult

}

func processQueue(queue chan []byte) map[string]*entry {
	result := make(map[string]*entry, 10000)
	for buffer := range queue {
		processChunk(buffer, result)
		go putBuffer(buffer)
	}

	return result
}

func processChunk(chunk []byte, result map[string]*entry) {
	for len(chunk) > 0 {
		i := 0
		for chunk[i] != ';' {
			i += 1
		}
		name := string(chunk[0:i])
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

		e := result[name]
		if e == nil {
			e = &entry{
				min:   number,
				max:   number,
				total: number,
				count: 1,
			}
			result[name] = e
		} else {
			e.count += 1
			e.total += number
			e.min = min(e.min, number)
			e.max = max(e.max, number)
		}

	}
}

func printResult(w io.Writer, result map[string]*entry) {
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

