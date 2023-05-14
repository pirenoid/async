package main

import (
	"sort"
	"strconv"
	"strings"
	"sync"
)

const th = 6

func pipeline(in chan interface{}, out chan interface{}, j job, wg *sync.WaitGroup) {
	defer wg.Done()
	defer close(out)
	j(in, out)
}

func ExecutePipeline(jobs ...job) {
	wg := &sync.WaitGroup{}
	in := make(chan interface{})
	out := make(chan interface{})

	for _, j := range jobs {
		wg.Add(1)
		go pipeline(in, out, j, wg)
		in = out
		out = make(chan interface{})
	}
	wg.Wait()
}

func makeCrc32Md5(crc32Md5Chan chan string, data string, wg *sync.WaitGroup, m *sync.Mutex) {
	defer wg.Done()
	m.Lock()
	md5 := DataSignerMd5(data)
	m.Unlock()
	crc32Md5 := DataSignerCrc32(md5)

	crc32Md5Chan <- crc32Md5
}

func makeCrc32(crc32Chan chan string, data string, wg *sync.WaitGroup) {
	defer wg.Done()
	crc32 := DataSignerCrc32(data)
	crc32Chan <- crc32
}

func resultSingleHash(crc32Chan, crc32Md5Chan chan string, out chan interface{}, wg *sync.WaitGroup) {
	defer wg.Done()
	crc32 := <-crc32Chan
	crc32Md5 := <-crc32Md5Chan
	res := crc32 + "~" + crc32Md5
	out <- res
}

func SingleHash(in, out chan interface{}) {
	wg := &sync.WaitGroup{}
	m := &sync.Mutex{}
	for s := range in {
		var data string
		switch s.(type) {
		case int:
			data = strconv.Itoa(s.(int))
		case string:
			data = s.(string)
		}

		// crc32(md5(data))
		crc32Md5Chan := make(chan string, 1)
		wg.Add(1)
		go makeCrc32Md5(crc32Md5Chan, data, wg, m)

		// crc32(data)
		wg.Add(1)
		crc32Chan := make(chan string, 1)
		go makeCrc32(crc32Chan, data, wg)

		// crc32(data)+"~"+crc32(md5(data))
		wg.Add(1)
		go resultSingleHash(crc32Chan, crc32Md5Chan, out, wg)
	}

	wg.Wait()
}

func crc32MultiHash(data string, i int, wgMH *sync.WaitGroup, m *sync.Mutex, result []string) {
	defer wgMH.Done()
	crc32 := DataSignerCrc32(strconv.Itoa(i) + data)
	m.Lock()
	result[i] = crc32
	m.Unlock()
}

func resultMultiHash(out chan interface{}, wg *sync.WaitGroup, wgMH *sync.WaitGroup, result []string) {
	defer wg.Done()
	wgMH.Wait()
	resString := ""
	for i := 0; i < th; i++ {
		resString += result[i]
	}
	out <- resString
}

func MultiHash(in, out chan interface{}) {
	wg := &sync.WaitGroup{}
	wgMH := &sync.WaitGroup{}
	for s := range in {
		var data string
		switch s.(type) {
		case int:
			data = strconv.Itoa(s.(int))
		case string:
			data = s.(string)
		}

		result := make([]string, 6)
		m := &sync.Mutex{}

		// crc32(th+data)
		for i := 0; i < 6; i++ {
			wgMH.Add(1)
			go crc32MultiHash(data, i, wgMH, m, result)
		}

		wg.Add(1)
		go resultMultiHash(out, wg, wgMH, result)
	}

	wg.Wait()
}

func CombineResults(in, out chan interface{}) {
	var strs []string
	for s := range in {
		strs = append(strs, s.(string))
	}
	sort.Strings(strs)
	res := strings.Join(strs, "_")
	out <- res
}
