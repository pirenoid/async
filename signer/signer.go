package main

import (
	"sort"
	"strconv"
	"strings"
	"sync"
)

func ExecutePipeline(jobs ...job) {
	wg := &sync.WaitGroup{}
	in := make(chan interface{})
	out := make(chan interface{})

	for _, j := range jobs {
		wg.Add(1)
		go func(in chan interface{}, out chan interface{}, j job) {
			defer wg.Done()
			defer close(out)
			j(in, out)
		}(in, out, j)
		in = out
		out = make(chan interface{})
	}
	wg.Wait()
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

		// crc32(data)+"~"+crc32(md5(data))

		// crc32(md5(data))
		crc32Md5Chan := make(chan string, 1)
		wg.Add(1)
		go func(crc32Md5Chan chan string, data string) {
			defer wg.Done()
			m.Lock()
			md5 := DataSignerMd5(data)
			m.Unlock()
			crc32Md5 := DataSignerCrc32(md5)

			crc32Md5Chan <- crc32Md5
		}(crc32Md5Chan, data)

		// crc32(data)

		wg.Add(1)
		crc32Chan := make(chan string, 1)
		go func(crc32Chan chan string, data string) {
			defer wg.Done()
			crc32 := DataSignerCrc32(data)
			crc32Chan <- crc32
		}(crc32Chan, data)

		wg.Add(1)
		go func(crc32Chan, crc32Md5Chan chan string, out chan interface{}) {
			defer wg.Done()
			crc32 := <-crc32Chan
			crc32Md5 := <-crc32Md5Chan
			res := crc32 + "~" + crc32Md5
			out <- res
		}(crc32Chan, crc32Md5Chan, out)
	}

	wg.Wait()
}

// crc32(th+data)

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

		for i := 0; i < 6; i++ {
			wgMH.Add(1)
			go func(data string, i int) {
				defer wgMH.Done()
				crc32 := DataSignerCrc32(strconv.Itoa(i) + data)
				m.Lock()
				result[i] = crc32
				m.Unlock()
			}(data, i)
		}

		wg.Add(1)
		go func(out chan interface{}) {
			defer wg.Done()
			wgMH.Wait()
			resString := ""
			for i := 0; i < 6; i++ {
				resString += result[i]
			}
			out <- resString
		}(out)
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
