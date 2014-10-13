`mETL`
====

mini ETL - a miniature ETL in Clojure

mETL accepts streaming connections that write a simple binary format:

    |bucket name length (4 byte integer)| bucket name ...|message length (4 byte integer)| message ...|

mETL saves events destined for one or more 'bucket' and flushes the metrics to another program's stdin.

Currently, the flush event occurs for each bucket when there are 1e6 events or the bucket size is roughly 128MB. When the events are first read there is a Unix timestamp, including milliseconds saved along with the event; duplicate events will only be saved once.


Starting the server
-------------------

mETL needs to be called with a sink.

Currently, we start on port 8888 with:

    $ lein run <sink program>

To start with the demo sink, `local.py`, that flushes the events, one per line, to a file with a directory structure named `data/<bucket name>/<ISO datetime partitioned by hour>/<uuid>.txt` call:

    $ lein run ./sinks/local.py


`TODO:`
-------

- config file for host, port, flush thresholds.
- builtin sink if flush exits with a non-zero status.
- better client
- http client for single events?
- emit metrics to statsite
- better repl server that closes all open connections
