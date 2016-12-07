package main

import (
	"io"
	"testing"

	"golang.org/x/net/context"
	"gopkg.in/olivere/elastic.v5"
)

const (
	ElasticURL   = "http://localhost:9200"
	ElasticIndex = "elastic-streaming-bulk-test"
)

func BenchmarkStreamingBulk(b *testing.B) {
	numFields := 1
	numDocs := 100
	maxSize := 1 * 1024

	for i := 0; i < b.N; i++ {

		// Drop the index to have a clean state
		client, err := elastic.NewClient(elastic.SetURL(ElasticURL), elastic.SetSniff(false))
		if err != nil {
			b.Fatal(err)
		}
		ctx := context.Background()
		exists, err := client.IndexExists(ElasticIndex).Do(ctx)
		if exists {
			_, err = client.DeleteIndex(ElasticIndex).Do(ctx)
			if err != nil {
				b.Fatal(err)
			}
		}

		// Create iterator
		it := NewIterator(ElasticIndex, "testcase", numFields, numDocs)
		svc := NewStreamingBulkService(ElasticURL).MaxSize(maxSize)

		for {
			// Read next Bulkable request
			data, err := it.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				b.Fatal(err)
			}

			// Send request to service
			err = svc.Write(data)
			if err != nil {
				b.Fatal(err)
			}
		}

		// Close the service and stop streaming
		err = svc.Close()
		if err != nil {
			b.Fatal(err)
		}

		// Check that the number of documents are inserted
		count, err := client.Count(ElasticIndex).Do(ctx)
		if err != nil {
			b.Fatal(err)
		}
		if count != int64(numDocs) {
			b.Errorf("expected %d documents, got %d\n", numDocs, count)
		}
	}
}
