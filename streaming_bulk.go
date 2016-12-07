package main

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"sync"

	"gopkg.in/olivere/elastic.v5"
)

// StreamingBulkService streams bulk requests directly to Elasticsearch,
// without copying data in memory.
type StreamingBulkService struct {
	esURL     string
	maxSize   int
	chunkSize int

	mu       sync.Mutex     // guards the following block
	req      *http.Request  // current HTTP request
	w        *io.PipeWriter // current writer to HTTP request
	debugOut io.Writer

	wg sync.WaitGroup
}

// NewStreamingBulkService returns a service to stream bulk requests.
func NewStreamingBulkService(esURL string) *StreamingBulkService {
	return &StreamingBulkService{
		esURL:   esURL,
		maxSize: 5 << 20, // 5 MB by default
	}
}

// MaxSize sets the maximum size of a chunk (in bytes) before closing the HTTP request.
func (s *StreamingBulkService) MaxSize(max int) *StreamingBulkService {
	s.maxSize = max
	return s
}

// DebugOut will write the bulk request data to the given writer, if specified.
func (s *StreamingBulkService) DebugOut(out io.Writer) *StreamingBulkService {
	s.debugOut = out
	return s
}

// Write writes a bulk request to the underlying stream.
// It returns the current chunk size as well as an error.
func (s *StreamingBulkService) Write(data []byte) (err error) {
	// Global lock for now
	s.mu.Lock()
	defer s.mu.Unlock()

	// Initialize a new HTTP request if we don't have one already
	if s.req == nil {
		pr, pw := io.Pipe()
		s.w = pw
		s.req, err = http.NewRequest("POST", s.esURL+"/_bulk", pr)
		if err != nil {
			return err
		}
	}

	if s.chunkSize > s.maxSize {
		// Commit the current HTTP request
		go func() {
			s.wg.Wait()
			_, err := s.w.Write(data)
			if err != nil {
				panic(err)
			}
			if s.debugOut != nil {
				s.debugOut.Write(data)
				s.debugOut.Write([]byte("== COMMIT ==\n"))
			}
			s.w.Close()
		}()

		// Post the HTTP request
		resp, err := http.DefaultClient.Do(s.req)
		if err != nil {
			return err
		}

		// Check for errors in bulk response
		var res elastic.BulkResponse
		err = json.NewDecoder(resp.Body).Decode(&res)
		if err != nil {
			return err
		}
		resp.Body.Close()
		if res.Errors {
			for _, item := range res.Failed() {
				log.Printf("%+v", item.Error)
			}
			panic("bulk response errors found")
		}

		// Make sure we create a new request (and pipe) on the next call to Write
		s.req = nil
		s.chunkSize = 0
	} else {
		// Write to existing pipe
		s.wg.Add(1)
		go func() {
			_, err := s.w.Write(data)
			if err != nil {
				panic(err)
			}
			if s.debugOut != nil {
				s.debugOut.Write(data)
			}
			s.wg.Done()
		}()
	}

	s.chunkSize += len(data)

	return
}

// Close commits outstanding bulk requests.
func (s *StreamingBulkService) Close() error {
	// Global lock for now
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.req == nil {
		return nil
	}

	go func() {
		s.wg.Wait()
		s.w.Close()
	}()

	resp, err := http.DefaultClient.Do(s.req)
	if err != nil {
		return err
	}
	var res elastic.BulkResponse
	err = json.NewDecoder(resp.Body).Decode(&res)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if res.Errors {
		for _, item := range res.Failed() {
			log.Printf("%+v", item.Error)
		}
		panic("bulk response errors found")
	}
	// Make sure we create a new request (and pipe) on the next call to Write
	s.req = nil
	s.chunkSize = 0

	// Flush to ensure the last commit is visible before returning from Close
	// This must be more granular (e.g. on the index only) or even removed from StreamingBulk
	req, err := http.NewRequest("POST", s.esURL+"/_flush", nil)
	if err != nil {
		return err
	}
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()

	return nil
}
