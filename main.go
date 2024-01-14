/*
  _  __     _    _                        _          ______         ______
 | |/ /    | |  (_)     /\               | |        |  ____|       |  ____|
 | ' / __ _| | ___     /  \   _ __   __ _| | __     | |__ _ __     | |__ _ __
 |  < / _` | |/ / |   / /\ \ | '_ \ / _` | |/ /     |  __| '__|    |  __| '__|
 | . \ (_| |   <| |  / ____ \| | | | (_| |   <      | |  | |       | |  | |
 |_|\_\__,_|_|\_\_| /_/    \_\_| |_|\__,_|_|\_\     |_|  |_|       |_|  |_|


*/

package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"sync"
	"sync/atomic"
	"time"
)

const (
	LOG_INTERVAL            = 1 * time.Second
	MAX_CONCURRENT_REQUESTS = 10000
	REQUEST_RETRY_DELAY     = 1 * time.Second
	PACKAGE_ITERATION_DELAY = 1 * time.Microsecond
)

// Used for limiting concurrent requests
type RequestManager struct {
	// current amount of concurrent requests, use atomic for thread safety
	totalRequests atomic.Int64

	// requests finished
	requestsFinished atomic.Int64

	maxConcurrentRequests int64

	// Time to sleep when the maximum number of concurrent requests is reached
	// after which the request will be tried again
	requestRetryDelay time.Duration
}

func (client *RequestManager) getRunningRequests() int64 {
	return client.totalRequests.Load() - client.requestsFinished.Load()
}

// Makes a request and returns the response
// If the maximum number of concurrent requests is reached, it will wait until a request is finished
// and then try again
func (client *RequestManager) Request(method string, request_url string) (response *http.Response, err error) {
	for client.getRunningRequests() >= client.maxConcurrentRequests {
		time.Sleep(client.requestRetryDelay)
	}

	u, err := url.Parse(request_url)
	if err != nil {
		return nil, err
	}

	request := http.Request{Method: method, URL: u}

	client.totalRequests.Add(1)
	response, err = http.DefaultClient.Do(&request)
	client.requestsFinished.Add(1)

	return response, err
}

// Convenience methods
func (client *RequestManager) Get(request_url string) (response *http.Response, err error) {
	return client.Request("GET", request_url)
}

func (client *RequestManager) Head(request_url string) (response *http.Response, err error) {
	return client.Request("HEAD", request_url)
}

// Every package can have multiple distributions
type Distribution struct {
	pkg  *Package
	name string
	url  string
}

func (dist Distribution) getSizeBytes(c *RequestManager) (int, error) {
	response, err := c.Head(dist.url)
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()
	return int(response.ContentLength), nil
}

type Package struct {
	name string
	url  string
}

func (pkg Package) getDistributions(c *RequestManager) ([]Distribution, error) {
	response, err := c.Get(pkg.url)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	body, err := io.ReadAll(response.Body)

	if err != nil {
		return nil, err
	}

	urls := exctractUrls(body)
	distributions := make([]Distribution, len(urls))
	for i, anchorTag := range urls {
		if err != nil {
			return nil, err
		}

		distributions[i] = Distribution{pkg: &pkg, url: anchorTag.href, name: anchorTag.text}

	}
	return distributions, nil
}

type AnchorTag struct {
	href string
	text string
}

func exctractUrls(body []byte) []AnchorTag {
	anchor_regex := regexp.MustCompile(`<a href=\"([^\"]*)".*>([^<]*)</a>`)
	anchors := anchor_regex.FindAllSubmatch(body, -1)
	urls := make([]AnchorTag, len(anchors))
	for i, anchor := range anchors {
		urls[i] = AnchorTag{href: string(anchor[1]), text: string(anchor[2])}
	}
	return urls
}

func getPackages(registry_url string) (packages []Package, err error) {
	response, err := http.Get(registry_url)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}

	urls := exctractUrls(body)
	packages = make([]Package, len(urls))
	for i, anchorTag := range urls {
		u, err := url.Parse(anchorTag.href)
		if err != nil {
			return nil, err
		}

		package_url := response.Request.URL.ResolveReference(u)

		packages[i] = Package{name: anchorTag.text, url: package_url.String()}
	}
	return packages, nil
}

func main() {
	packages, err := getPackages("https://pypi.org/simple/")
	if err != nil {
		panic(err)
	}

	requestManager := &RequestManager{
		maxConcurrentRequests: MAX_CONCURRENT_REQUESTS,
		requestRetryDelay:     REQUEST_RETRY_DELAY,
	}
	tasksGroup := sync.WaitGroup{}

	// Counters for the progress
	var packagesScraped atomic.Uint32
	var distributionsFound atomic.Uint32
	var totalSizeBytes atomic.Uint64

	packagesCount := len(packages)

	logger := log.Default()
	logger.SetPrefix("\033[1A\033[K")

	// Continuously print the progress
	go func() {
		for {
			logger.Printf(
				"Scraped %d/%d packages, Distributions found: %d, Total size: %d GB\n",
				packagesScraped.Load(), packagesCount, distributionsFound.Load(), totalSizeBytes.Load()/1_000_000_000,
			)

			time.Sleep(LOG_INTERVAL)
		}
	}()

	f, err := os.Create("output.csv")
	if err != nil {
		panic(err)
	}

	writer := csv.NewWriter(f)

	// Write the header
	writer.Write([]string{"Package", "Distribution", "Size"})

	for _, pkg := range packages {
		tasksGroup.Add(1)

		packagesScraped.Add(1)
		go func(pkg Package) {
			distributions, err := pkg.getDistributions(requestManager)
			if err != nil {
				panic(err)
			}
			for _, distribution := range distributions {
				distributionsFound.Add(1)
				size, err := distribution.getSizeBytes(requestManager)
				totalSizeBytes.Add(uint64(size))
				if err != nil {
					panic(err)
				}
				writer.Write([]string{pkg.name, distribution.name, fmt.Sprintf("%d", size)})
			}
			tasksGroup.Done()

		}(pkg)
		time.Sleep(PACKAGE_ITERATION_DELAY)
	}
	tasksGroup.Wait()
	f.Close()
}
