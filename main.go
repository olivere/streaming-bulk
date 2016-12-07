package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"golang.org/x/net/context"
	"gopkg.in/olivere/elastic.v5"
)

func main() {
	var (
		url      = flag.String("url", "http://localhost:9200", "Elasticsearch URL")
		index    = flag.String("index", "", "Elasticsearch index name")
		typ      = flag.String("type", "", "Elasticsearch type name")
		sniff    = flag.Bool("sniff", false, "Sniff the cluster")
		fields   = flag.Int("fields", 1, "Number of fields the bulk documents should have")
		n        = flag.Int("n", 0, "Number of documents to bulk insert")
		maxSize  = flag.Int("max-size", 0, "Number of bytes to stream per chunk")
		debugOut = flag.String("out", "", "Filename to dump bulk requests")
	)
	flag.Parse()
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	if *url == "" {
		log.Fatal("missing url parameter")
	}
	if *index == "" {
		log.Fatal("missing index parameter")
	}
	if *typ == "" {
		log.Fatal("missing type parameter")
	}
	if *fields <= 0 {
		log.Fatal("fields must be a positive integer")
	}
	if *n <= 0 {
		log.Fatal("n must be a positive integer")
	}
	if *maxSize <= 0 {
		log.Fatal("max-size must be a positive integer")
	}

	// Drop the index to have a clean state
	client, err := elastic.NewClient(elastic.SetURL(*url), elastic.SetSniff(*sniff))
	if err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()
	exists, err := client.IndexExists(*index).Do(ctx)
	if exists {
		_, err = client.DeleteIndex(*index).Do(ctx)
		if err != nil {
			log.Fatal(err)
		}
	}

	// Create iterator
	it := NewIterator(*index, *typ, *fields, *n)

	svc := NewStreamingBulkService(*url).MaxSize(*maxSize)
	if *debugOut != "" {
		f, err := os.Create(*debugOut)
		if err != nil {
			panic(err)
		}
		defer f.Close()
		svc = svc.DebugOut(f)
	}

	var current int
	begin := time.Now()

	for {
		// Read next Bulkable request
		data, err := it.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		// Send request to service
		err = svc.Write(data)
		if err != nil {
			log.Fatal(err)
		}

		current++
		dur := time.Since(begin).Seconds()
		sec := int(dur)
		pps := int64(float64(current) / dur)
		fmt.Printf("%10d | %6d req/s | %02d:%02d\r",
			current,
			pps,
			sec/60, sec%60)
	}
	fmt.Println()

	// Close the service and stop streaming
	err = svc.Close()
	if err != nil {
		log.Fatal(err)
	}

	// Check that the number of documents are inserted
	count, err := client.Count(*index).Do(ctx)
	if err != nil {
		log.Fatal(err)
	}
	if count != int64(*n) {
		fmt.Printf("Expected %d documents in index, found %d\n", *n, count)
	}
}
