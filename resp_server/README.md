# resp_server

A RESP2 server that makes B+-Forest usable from Redis clients. It bundles
the pipelined commands of all connections into batches and executes them
with the batch API of B+-Forest. It is a high-throughput ordered key-value
server that presumes deep client-side pipelines (`-P` of redis-benchmark
and memtier_benchmark); the latency of a single command is bounded by the
execution time of a batch.

## Starting

The default build (see the [top-level README](../README.md)) builds
`resp_server_<variant>` with every command:

```bash
cmake -S . -B build
cmake --build build --target resp_server_upmem
```

A build that leaves out query types with the `OPS` CMake variable answers
the commands of those types with an error.

B+-Forest is built only by bulk loading, so the server takes its initial
data at startup:

- `--init-nr N` (default 2^20): generate N pairs with key = i × stride and
  value = key (`--init-stride`, default 2)
- `--init-file PATH`: load an init file of `workload_gen` (or PIM-tree)

```console
$ build/resp_server/resp_server_upmem --port 6399 --init-nr 1000000
$ redis-cli -p 6399 GET 2
```

Other main options: `--bind` (default 127.0.0.1), `--batch-size` (the
maximum number of queries executed in a batch, default 2^20),
`--max-pipeline` (the maximum number of unexecuted commands per
connection), and the same set of B+-Forest tuning options as `host_app`
(`-a`, `--incremental`, and so on). `--help` lists them all.

## Commands

Keys and values are both decimal uint64 strings. Ranges include both
ends.

| Command | Batch API | Reply |
|---|---|---|
| `GET k` | batch_get | the value as a bulk string; nil on a miss |
| `SET k v` | batch_insert (upsert) | `+OK`; v = 0 is an error (see below) |
| `DEL k [k ...]` | batch_delete | `:<number deleted>` (a repeated argument counts once) |
| `EXISTS k [k ...]` | batch_get | `:<number existing>` |
| `BPF.PRED k` | batch_pred | `[key, value]`; nil if there is no predecessor |
| `BPF.RANGECOUNT b e v` | batch_range_count | `:<count>`: the number of pairs in [b,e] with value = v |
| `BPF.RANGEMAX b e` | batch_range_max | the maximum value in [b,e]; nil if empty |
| `PING` `ECHO` `COMMAND` `CONFIG` `DBSIZE` `QUIT` `SHUTDOWN` | — | for compatibility (so that redis-cli connects as is) |

## Semantics

- The commands of one connection give the same results, and the replies
  come in the same order, as if they were executed serially in the order
  sent.
- The order relative to the commands of other connections is not
  guaranteed (nor does Redis guarantee an order between clients), but all
  the replies and the final state are consistent with some serial
  execution order of all the commands.
- `SET` rejects the value 0, which collides with `NOT_FOUND_VALUE` and
  could not be told apart from a miss.
- `BPF.PRED k` returns the live pair with the largest key less than k. It
  never returns a deleted key; nil if there is no such pair.
- `DBSIZE` returns the total number of live pairs (deleted keys are not
  counted).
