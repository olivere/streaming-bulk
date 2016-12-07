# Streaming bulk API for Elasticsearch

**This is just a proof of concept. Do not use in production.**

This repository implements a streaming bulk API for Go.
It streams documents directly to HTTP requests, committing them
after a chunk of bytes have been written.

Compile with `go build`.

Then run e.g. via:

```sh
$ ./streaming-bulk -url=http://localhost:9200 -index=warehouse -type=product -n=100000 -max-size=1024 -fields=10
```

## License

MIT
