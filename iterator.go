package main

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"
	"strconv"
	"sync/atomic"
	"time"

	"gopkg.in/olivere/elastic.v5"
)

// Iterator defines a pull-style protocol for clients.
type Iterator interface {
	Next() ([]byte, error)
}

// iter is a simple implementation of Iterator.
type iter struct {
	index     string
	typ       string
	current   uint32
	numFields int
	numDocs   uint32
}

// NewIterator returns an iterator for Bulk requests.
func NewIterator(index, typ string, numFields, numDocs int) Iterator {
	return &iter{index: index, typ: typ, numFields: numFields, numDocs: uint32(numDocs)}
}

// Next returns the next bulk request to be sent to the stream.
func (it *iter) Next() ([]byte, error) {
	current := atomic.AddUint32(&it.current, 1)
	if current > it.numDocs {
		return nil, io.EOF
	}

	// Generate a random document with a number of fields
	doc := make(map[string]interface{})
	doc["@timestamp"] = time.Now()
	for i := 0; i < it.numFields; i++ {
		b := make([]byte, 32)
		_, err := rand.Read(b)
		if err != nil {
			return nil, err
		}
		doc[fmt.Sprintf("field_%d", i)] = fmt.Sprint(base64.URLEncoding.EncodeToString(b))
	}

	// Generate a bulk index request here
	req := elastic.NewBulkIndexRequest().
		Index(it.index).
		Type(it.typ).
		Id(strconv.Itoa(int(current))).
		Doc(doc)

	// Not very efficient, but that's not what we care about in this iterator
	lines, err := req.Source()
	if err != nil {
		return nil, err
	}
	buf := new(bytes.Buffer)
	for _, line := range lines {
		buf.WriteString(line)
		buf.WriteRune('\n')
	}
	return buf.Bytes(), nil
}
