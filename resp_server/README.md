# resp_server

BPForest を Redis クライアントから使えるようにする RESP2 サーバ。全接続からパイプラインされたコマンドをバッチに束ね、BPForest のバッチ API で実行する。深いクライアント側パイプライン (redis-benchmark / memtier_benchmark の `-P`) を前提とした高スループット順序付き KV サーバであり、単発コマンドのレイテンシはバッチ実行時間に律速される。

## 起動

ビルドは、この repo を `bp-forest/` として含む上位ディレクトリのビルドスクリプトで行うのが基本。単体でビルドする場合は、対応させる操作の `SUPPORT_*` フラグを付けて `resp_server_<target>` をビルドする:

```bash
SUP="-DSUPPORT_GET -DSUPPORT_PRED -DSUPPORT_INSERT -DSUPPORT_DELETE -DSUPPORT_RANGE_COUNT -DSUPPORT_RANGE_MAX"
cmake -Dtargets=upmem -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++ \
      -DCMAKE_C_FLAGS="$SUP" -DCMAKE_CXX_FLAGS="$SUP" -S . -B build
cmake --build build --target resp_server_upmem
```

BPForest はバルクロード構築のみのため、起動時に初期データを与える:

- `--init-nr N` (既定 2^20) — key = i × stride, value = key の N ペアを生成 (`--init-stride`, 既定 2)
- `--init-file PATH` — PIM-Tree init file をロード

```console
$ build/resp_server/resp_server_upmem --port 6399 --init-nr 1000000
$ redis-cli -p 6399 GET 2
```

他の主なオプション: `--bind` (既定 127.0.0.1)、`--batch-size` (1 バッチで実行する最大クエリ数、既定 2^20)、`--max-pipeline` (接続あたりの未実行コマンド数上限)、および `host_app` と同じ B+-Forest チューニング一式 (`-a`, `--incremental` など)。一覧は `--help`。

## コマンド

キー・値はともに 10 進 uint64 の文字列。範囲は両端を含む。

| コマンド | 対応するバッチ API | 応答 |
|---|---|---|
| `GET k` | batch_get | 値の bulk string、miss は nil |
| `SET k v` | batch_insert (upsert) | `+OK`。v = 0 はエラー (下記) |
| `DEL k [k ...]` | batch_delete | `:削除数` (重複引数は 1 回) |
| `EXISTS k [k ...]` | batch_get | `:存在数` |
| `BPF.PRED k` | batch_pred | `[key, value]`、predecessor なしは nil |
| `BPF.RANGECOUNT b e v` | batch_range_count | `:個数` — [b,e] 内で value = v のペア数 |
| `BPF.RANGEMAX b e` | batch_range_max | [b,e] 内の最大 value、空なら nil |
| `PING` `ECHO` `COMMAND` `CONFIG` `DBSIZE` `QUIT` `SHUTDOWN` | — | 互換用 (redis-cli がそのまま接続できる) |

`SUPPORT_*` フラグでビルドに含めなかった操作のコマンドはエラー応答になる。

## 意味論

- 同一接続のコマンド列は、送信順に直列実行したのと同じ結果と応答順を保証する。
- 別接続のコマンドとの相対順序は保証しない (Redis もクライアント間の順序は保証しない)。さらに、別接続の書き込みと同一バッチ・同一キーで衝突した場合に限り、`DEL` の応答値がどの直列実行順とも整合しないことがありうる (最終状態は決定的で一貫)。
- 値 0 は `NOT_FOUND_VALUE` と衝突し miss と区別できないため、`SET` が拒否する。
- `BPF.PRED k` は、生きているペアのうち key が k 未満で最大のものを返す。削除済みキーを返すことはなく、該当ペアがなければ nil。
