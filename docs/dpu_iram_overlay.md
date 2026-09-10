DPU プログラムの IRAM overlay
===

全タスクを組み込んだ DPU プログラムは IRAM (v1B: 3,968 命令 = 31,744
バイト) に収まらない。そこでタスクのコードを MRAM に置き、起動された
タスクが必要とするものだけを DPU 自身が実行時に IRAM へロードする。
CMake オプション `DPU_IRAM_OVERLAY=ON` で有効になる (既定 OFF: 全コードが
IRAM に収まるビルドでは従来どおり静的配置のまま)。

## 構成

同時に IRAM に載る必要のないタスク群を「スロット」にまとめ、全スロットを
IRAM 末尾の同一領域 (窓) に重ねて配置する (`dpu/inc/iram_overlay.h` の
`OVL_SLOT_*`):

| スロット | 中身 |
| --- | --- |
| `OVL_SLOT_INSERT` | task_insert (バッチの並べ替えと分担を含む)・TASK_MOVE_HOT の upsert |
| `OVL_SLOT_RESHARD` | rebalancing 系: 木の構築 (task_init 含む)・直列化・破棄 |
| `OVL_SLOT_QUERY` | その他のクエリ処理: get / pred / range_count / range_max |
| `OVL_SLOT_CHECK` | 木の構造検査 (`TASK_*_CHECK` を定義したビルドでのみ中身がある) |
| `OVL_SLOT_DELETE` | task_delete の前半: 結果配列づくりとキーの並べ替え |
| `OVL_SLOT_DELETE_TREE` | task_delete の後半: 木からペアを外す (分担と連結を含む) |

窓に載せず IRAM に置いたままにする部分 (以下**常駐**) に残るのは、main のタスク
振り分け、複数のスロットから呼ぶ関数 (ノードの確保と解放・二分探索・同期プリミティブ・
ノードの転送)、各タスクのフェーズ制御部 (`task_insert`・`task_delete`・`task_init`・
`task_move_hot` の本体)、ローダなど。別スロットの関数は呼べないので、共有するには
常駐に置くか各スロットに複製するかで、前者を採った。

木の構造検査 (`check_tree_structure`) を専用スロットに分けているのは、検査ルーチンが
3 KB 台と大きく、常駐に置くと全スロットの窓がそのぶん狭まるためである。検査は各タスクが
木を触り終えた後に、常駐のフェーズ制御部から `CHECK_trees` を呼んで行う。

常駐と全スロットは 1 つの ELF にリンクされるため、スロット内の関数から
常駐の関数・データを普通に (型検査付きで) 参照でき、ホストから見ても
バイナリは 1 つ・`dpu_load` も 1 回のまま。窓・ロードイメージ・ヒープの
位置はすべてリンカが決め、手動調整は無い。窓の実質容量は「IRAM 容量 −
常駐サイズ」で、超過はリンクエラーになる。

## タスクを書くときの規約

*   エントリ (常駐から呼ばれる関数) は、定義のシグネチャ部分を
    `OVERLAY_TASK[_STATIC](スロット, 名前, (引数...), (実引数...))` と
    書く。呼び出し側からは `<名前>` が通常の関数に見える (実体は
    「スロットをロードして本体 `<名前>_ovl` を呼ぶ」常駐ディスパッチ
    関数)。スロット内ヘルパには `OVERLAY_LOCAL(スロット)` を付ける。
*   エントリは全 NR_TASKLETS tasklet が同じ引数で呼ぶ。ロード前後の
    barrier に全 tasklet が到達する必要があるため。tasklet の絞り込み
    (`me() < TASK_*_NR_TASKLETS`) はエントリの内側で行う。
*   別スロットの関数は呼べない。必要なら常駐関数を経由し、フェーズごとに
    スロットを切り替える (例: 常駐の `task_move_hot`)。違反 (`<名前>_ovl`
    や別スロットの関数の直接呼び出し) は検出されず、実行時に「窓に載って
    いる別のコード」が黙って走る。

## 制約

*   dpu-lldb はスロット内コードをデバッグできない (窓に何が載っているか
    知らないため)。デバッグは overlay 無効ビルドか dpu_on_cpu ビルドで行う。
*   スロット切り替えごとにイメージの DMA (10〜20KB = DMA エンジンを
    5,000〜10,000 サイクル占有する) + barrier がかかる。同じスロットが
    連続する限りロードは省略される。
*   `SUPPORT_RANGE_MIN` (未修理タスク) は overlay 化していない。

## 仕組み

*   `dpu/overlay/overlay_additions.lds` — SDK の dpu.lds への追加定義。
    各スロットのセクションを同一 VMA (= 窓、常駐 `.text` の直後) に重ね、
    ロードイメージ (LMA) を MRAM 静的データの直後に 1024B (MRAM 行)
    整列で並べる。窓の位置と各イメージの LMA・サイズはシンボルでローダに
    公開し、ヒープの起点 `__sys_used_mram_end` はイメージの直後へ再代入で
    動かす。
*   `dpu/overlay/link_overlay.sh` — ドライバのリンク行から dpu.lds の
    パスを取り出し、「iram 領域の LENGTH を形式上広げる + 追加定義を
    連結する」の 2 変換で使用する lds を生成し、`-T` を差し替えて実行する
    (SDK の更新に自動追従)。
*   `dpu/overlay/apply_overlay_lma.py` — `dpu_load` は LMA を無視するため、
    overlay セグメントのアドレスを LMA へ書き換えて MRAM に配置させる。
*   `dpu/src/overlay.c` — 窓に載っているスロットを覚えておき、違う
    スロットが要求されたときだけ、全 tasklet の barrier の間に `ldmai`
    (MRAM → IRAM の DMA) でロードする。
