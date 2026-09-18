# ホスト側キャッシュ

hot partition の負荷が 1 つの KV ペアに集中していて hot split が失敗するとき、および
据えた直後の hot partition が goal を超えるとき、そのペアをホストに写し取り、そのキーへの point クエリの大部分をホストで捌く仕組み (get は全部、
insert, delete, pred はルーティングのスレッドあたり 1 件を除いて)。試験的な実装で、
`BPForestParameter::enable_hot_cache` (host_app の `--hot-cache`、既定 on) で切り替える。

## 意味

* キャッシュを使うのは get, insert, delete, pred のバッチ。range 系 (range_count,
  range_max, scan) は使わず、いつも通り DPU へ行く。
* 各バッチの終わりには、DPU 上の木はキャッシュと同じペアを持つ。したがって range クエリが
  古い値を見ることはない。
* DPU ごとに高々 1 ペアを持ち、そのペアのキーはその DPU の hot partition の範囲にある
  (「追い出し」で保つ)。したがってキャッシュの大きさは hot partition の数以下、すなわち DPU 数以下である。
* クエリがキャッシュに当たるのは、ルーティングの宛先がその DPU の hot partition で、
  キーがその DPU のキャッシュのキーと一致するときだけ。

## 採用

バッチはルーティング、再分割、DPU での実行の順に進み、再分割がパーティション表を変えた
ときはルーティングをやり直してから実行に進む。ペアをキャッシュに入れること (採用) は
再分割の中で行う。

探すのは、hot partition に割り振られたクエリ (キャッシュで捌いた分は含まない) のうち
半分以上が向かうキーで、探す場面は 2 つある: 増分再分割の hot split
(`incremental_repartition_worker_hot`) が失敗したときと、再分割で据えた hot partition
が hot split の goal (`hot_cnt_goal`) を超えるとき (下記「据えた hot partition での採用」)。
以下はまず前者について述べる。hot split (docs/rebalancing-algorithm.md §6.2) の失敗とは、
`split_hot_range_equal_load` が切れ目を 1 つも作れないこと、すなわち末尾のチャンクが
負荷の半分超を持つことである (piece の数が 2 未満で分割しないのは失敗ではない)。
負荷が途中のチャンクに集中していれば、そのチャンクの直後で切れ、その
チャンクを末尾に持つ最初の piece がその DPU に残る。この piece は据えた直後に
「据えた hot partition での採用」(下記) の対象になる。1 チャンク (`KVPairsChunkSize`
ペア以下) の hot partition は分割を試みないので、据えた直後の他にはこの仕組みの対象に
ならない。

探すのは get, insert, pred のバッチで失敗した場合だけである。delete のバッチではそのペアが
バッチの後に消えるので探さない。

`find_hot_cache_candidate` が [hot_key_finding.md](hot_key_finding.md) の乱択法で探す:
割り振られたクエリから 108 個を復元抽出して見込みのあるキーを高々 2 つ選び、それぞれを
高々 1337 個の復元抽出による打ち切り付きの逐次確率比検定にかけ、最初に通ったキーだけを
見る (そのキーのペアが無くても 2 つ目には進まない)。失敗確率 δ = 0.01、幅 w = 0.1 で、
半分以上を占めるキーがあれば確率 1 − δ 以上で見つけ、見つけたキーは確率 1 − δ 以上で 1/2 − w = 0.4 より多くを占める。あわせて、
検定で見た標本のうちそのキーに一致した割合に割り振られたクエリ数を掛けたものを、
そのキーの推定件数とする。

キャッシュに入れるペアは、get と pred のバッチでは hot 木から取る。hot split のために
hot 木はホストに直列化されている (`data_buf`) ので、追加の DPU 往復は要らない。
見つけたキーが hot 木に無ければ入れるペアが無いので採用しない (空のスロットの印に
`NOT_FOUND_VALUE` を使うので、「存在しない」ことはキャッシュできない)。insert のバッチでは、木を見ずに、そのバッチでそのキーを insert
する最後のクエリのペアを入れる。そのバッチの insert が DPU 上にも同じペアを作る。

その DPU のスロットに既にペアがあるときは、そのペアがこのバッチの直近のルーティングで
捌いた件数と、見つけたキーの推定件数を比べ、後者が多いときだけ置き換える。探索が見る
負荷にはキャッシュで捌いた分が含まれないので、比べずに置き換えると、次のバッチで
追い出したキーの負荷が戻り、採用し直しが毎バッチ繰り返される。

hot split が失敗して採用に至らなかったとき (delete のバッチ、半分以上を占めるキーが
無い、そのキーのペアが無い、既にあるペアの方が多い) は `hot_split_failed` を立てる。
立っている間はその hot partition は再分割の起動要因にならない (他の DPU が起動した
バッチでは goal を超えていれば再検査される)。hot partition を据え直す (新設、
全体再構築、hot split で piece を据える) と下りる。したがって、例えば消したキーへの
get の集中はこれを立て、そのキーを挿入し直しても hot partition を据え直すまで
自分からは再分割を起こさない。

採用したバッチのクエリは採用の前に振り分け済みなので、キャッシュが効くのは次のバッチ
からである (同じ再分割で他の hot partition が新設・分割されてルーティングをやり直す
ときだけ、そのバッチから効く)。

### 据えた hot partition での採用

再分割で据えた hot partition (cold から切り出したもの、hot split の 2 つ目以降の piece、
split 元に残した最初の piece) を DPU に据えてクエリを振り分け直した後、振り分けられた
クエリ数が `hot_cnt_goal` を超えるもの (1 チャンクより大きければ、次のバッチで hot split
の対象になるもの) について同じ探索をする (docs/rebalancing-algorithm.md の
`relieve_overloaded_hots`)。既にあるペアとの比較は hot split での採用と同じ。違いは次の
通り。

* 採用したキーの推定件数を引いたクエリ数がなお `hot_cnt_goal` を超えるときに `hot_split_failed` を
  立てる。採用しなかったとき (range クエリと delete のバッチを含む) は引かずに比べる。
* 1 つでも採用したらルーティングをもう一度やり直すので、キャッシュはそのバッチから効く。

## 各バッチでの扱い

ルーティング (`route_single_point_query`) でキャッシュに当たったクエリは、種類ごとに
次のように扱い、DPU へはそのうちルーティングのスレッドあたり高々 1 件だけを送る。
pred は key より小さい最大のキーのペアを返すクエリなので、キャッシュのペアでは
答えられない。同じキーの pred は同じ答えになることだけを使う。

| クエリ | ホストで | DPU へ送るもの |
|:-|:-|:-|
| get | 値をその場で書く | なし |
| insert | キャッシュの値をバッチ順で最後の 1 件のものにする | 各スレッドの最後の 1 件 |
| delete | 送らなかった分は `existed = 0` | 各スレッドの最初の 1 件。`existed` は DPU から受ける |
| pred | 送った 1 件の結果を写す | 各スレッドの最初の 1 件 |

スレッドあたり 1 件で足りるのは DPU 側の規則による: 同一バッチに同じキーの insert が
複数あれば最後の 1 件の値が残り (docs/parallel_batch_update.md)、同じキーの delete が
複数あれば先頭の 1 件だけが `existed = 1` を返す (docs/parallel_delete.md)。いずれも
ホストが送った順序で決まる。各スレッドはバッチ順に連続した範囲を担当し、DPU に送る
クエリ列はスレッド順に並ぶので、DPU が見る順序はバッチ順であり、バッチ全体で最後
(insert) または最初 (delete) の 1 件が効く。

## 追い出し

次の 3 つの場面でペアを捨てる。

* delete のバッチでそのキーが消えたとき (バッチの後)。
* 同じ DPU で別のペアを採用したとき。
* hot split でそのキーが最初の piece に無いとき、および全体再構築のとき。キーがその DPU の
  hot partition から外れると、そのキーへの書き込みはキャッシュを経ないので、古い値を
  残さないため。

## ログ (part-log)

* `nosplit hot <dpu> reason new_hot load <m>`: 据えた hot partition に
  `hot_split_failed` を立てた。`m` は振り分けられたクエリ数から採用したキーの推定件数を引いた値。
* `cache hot <dpu> key <k> nqrys <n> load <m>`: 採用。`n` は推定件数、`m` はその
  hot partition の負荷 (キャッシュで捌いた分は含まない)。
* `cache keep <dpu> key <k> served <h> over <k'> nqrys <n>`: 既にあるペア `k` (このバッチで
  `h` 件捌いた。DPU へ送った分も数える) を見つけたキー `k'` (推定 `n` 件) より優先して
  残した。
* `cache evict <dpu> key <k> reason replaced|deleted|moved`: 追い出し。`moved` は hot split
  か全体再構築でキーがその DPU の hot partition から外れたとき。
* `cache hits <n>`: そのバッチでキャッシュに当たったクエリ数 (DPU へ送った分も数える)。
  0 のときは出ない。

## 実装

* 状態は `hot_cache` で、DPU 番号で引く要素 (スロット) にその DPU のペアを置く。空の
  スロットは値を `NOT_FOUND_VALUE` にする (ユーザの値はこれを取らない)。追い出しは値を
  これに戻すだけ (`drop_hot_cache_pair`)。hot split では最初の piece を据える箇所で、
  全体再構築では冒頭で捨てる。
* ルーティングのスレッドごとの記録 `HotCacheHits`: スロットごとに捌いた件数、木へ送る
  クエリのバッチ内位置、同じスレッドで同じスロットに 2 件目以降として当たった pred の
  位置。`route_clear_impl` で消す。
* delete と pred はスレッドが最初に当たった時点で `route_point_query_to_tree`
  (キャッシュを見ないルーティング) で送る。insert は各スレッドが自分の範囲を終えてから
  最後の 1 件を送り、キャッシュの値は `route_queries` がスレッド順に上書きして
  バッチ順で最後のものにする。
* pred の写しは `postprocess_of_pred_impl` の末尾、delete の追い出しは
  `evict_deleted_hot_cache_pairs` (`batch_delete` の末尾)。
* テストは `bpforest/test/hot_cache.cpp` (`hot_cache_test_<target>`)。1 つのキーへの
  get の集中でキャッシュが入ること、その後の get, insert, pred, range_count, delete,
  再挿入、全体再構築後の読みが参照実装と一致することを、キャッシュの on/off 両方で
  確かめる。
