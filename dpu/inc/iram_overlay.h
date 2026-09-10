#pragma once
/*
 * IRAM overlay: タスクのコードを MRAM に置き、実行時に必要なスロット
 * だけを IRAM 末尾の「窓」へロードして呼び出す。docs/dpu_iram_overlay.md
 * 参照。IRAM_OVERLAY が未定義のビルドではすべて無効果。
 */

#define OVL_SLOT_INSERT 0      /* task_insert と upsert エンジン */
#define OVL_SLOT_RESHARD 1     /* rebalancing 系: 木の構築・直列化・破棄 */
#define OVL_SLOT_QUERY 2       /* その他のクエリ処理 */
#define OVL_SLOT_CHECK 3       /* 木の構造検査 (検査ビルドでのみ中身がある) */
#define OVL_SLOT_DELETE 4      /* task_delete の前半: 結果配列づくりとキーの並べ替え */
#define OVL_SLOT_DELETE_TREE 5 /* task_delete の後半: 木からペアを外す */

#ifdef IRAM_OVERLAY

#include <stdint.h>

#define OVL_STR_EXPANDED_(x) #x
#define OVL_STR_(x) OVL_STR_EXPANDED_(x)

/* エントリ (常駐から呼ばれる関数) 用。noinline は必須: 呼び出し元に
 * inline 展開されると本体が常駐 .text に取り込まれてしまう。 */
#define OVERLAY(slot) __attribute__((noinline, used, section("ovl" OVL_STR_(slot))))
/* スロット内ヘルパ用。inline 展開を許す (展開先も同じスロット)。 */
#define OVERLAY_LOCAL(slot) __attribute__((section("ovl" OVL_STR_(slot))))

/* スロットのイメージを窓へロードする (ロード済みなら何もしない)。
 * 全 NR_TASKLETS tasklet が同じ引数で呼ぶこと。 */
void ovl_load_slot(uint32_t idx_slot, const uint8_t* image_lma, uint32_t nbytes);

/*
 * エントリの定義は、シグネチャ部分を
 * OVERLAY_TASK[_STATIC](スロット, 名前, (引数...), (実引数...)) と書く。
 * 呼び出し側からは <名前> が通常の関数に見える: 実体は「スロットを
 * ロードして本体を呼ぶ」常駐ディスパッチ関数で、本体は <名前>_ovl として
 * スロットに配置される。overlay 無効ビルドでは素の関数定義になる。
 */
#define OVERLAY_TASK(slot, entry, params, args) OVERLAY_TASK_(slot, /* empty */, entry, params, args)
#define OVERLAY_TASK_STATIC(slot, entry, params, args) OVERLAY_TASK_(slot, static, entry, params, args)
#define OVERLAY_TASK_(slot, storage, entry, params, args)                                     \
    OVERLAY(slot) static void entry##_ovl params;                                             \
    extern uint8_t __ovl_load_ovl##slot[], __ovl_size_ovl##slot[];                            \
    storage void entry params                                                                 \
    {                                                                                         \
        ovl_load_slot(slot, __ovl_load_ovl##slot, (uint32_t)(uintptr_t)__ovl_size_ovl##slot); \
        entry##_ovl args;                                                                     \
    }                                                                                         \
    OVERLAY(slot) static void entry##_ovl params

#else /* IRAM_OVERLAY */

#define OVERLAY(slot)
#define OVERLAY_LOCAL(slot)
#define OVERLAY_TASK(slot, entry, params, args) void entry params
#define OVERLAY_TASK_STATIC(slot, entry, params, args) static void entry params

#endif /* IRAM_OVERLAY */
