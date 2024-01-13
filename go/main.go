package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strconv"
)

type processor struct {
	result map[string]data
}

// var result map[string]data = make(map[string]data, 1024*16)

type data struct {
	min   float64
	max   float64
	total float64
	count int
}

func (d data) print(name string) string {
	return fmt.Sprintf("%s=%.1f/%.1f/%.1f", name, d.min, d.total/float64(d.count), d.max)
}

func New() processor {
	return processor{
		result: make(map[string]data, 16*1024),
	}
}
func main() {
	path := flag.String("path", "../data/weather_stations.csv", "data file path")
	flag.Parse()

	file, err := os.Open(*path)
	if err != nil {
		log.Fatal(err)
	}

	defer file.Close()

	p := New()

	if err := p.parseInput(file); err != nil {
		log.Fatal(err)
	}

	p.printResult(os.Stdout)
}

func (p processor) parseInput(input io.Reader) error {
	reader := bufio.NewReader(input)
	for {
		line, _, err := reader.ReadLine()
		if err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}
		split := bytes.SplitN(line, []byte{';'}, 2)
		name := string(split[0])
		float, err := strconv.ParseFloat(string(split[1]), 64)
		if err != nil {
			return err
		}

		value, ok := p.result[name]
		if !ok {
			p.result[name] = data{
				count: 1,
				min:   float,
				max:   float,
				total: float,
			}
			continue
		}
		value.count += 1
		value.total += float
		if float < value.min {
			value.min = float
		} else if float > value.max {
			value.max = float
		}
		p.result[name] = value
	}
}

func (p processor) printResult(w io.Writer) {
	names := make([]string, 0, len(p.result))
	for name := range p.result {
		names = append(names, name)
	}
	sort.Strings(names)
	fmt.Fprintf(w, "{")
	i := 0
	for ; i < len(names)-1; i += 1 {
		fmt.Fprint(w, p.result[names[i]].print(names[i]))
		fmt.Fprintf(w, ", ")
	}
	fmt.Fprint(w, p.result[names[i]].print(names[i]))
	fmt.Fprintf(w, "}\n")
}
