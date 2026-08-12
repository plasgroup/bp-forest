/* IRAM overlay の常駐ローダ (docs/dpu_iram_overlay.md)。 */
#include "iram_overlay.h"

#ifdef IRAM_OVERLAY

#include <barrier.h>
#include <built_ins.h>
#include <defs.h>

#include <stdint.h>


BARRIER_INIT(ovl_barrier, NR_TASKLETS);

/* IRAM 窓のバイトアドレス (= 命令インデックス x 8)。常駐 .text の直後。 */
extern uint8_t __ovl_window_byte_addr[];

/* 窓に載っているスロット。バイナリのロード直後は何も載っていない。 */
static uint32_t ovl_idx_loaded_slot = UINT32_MAX;

/*
 * 前後の barrier で、窓の書き換え中はどの tasklet も窓内のコードを実行して
 * いないことを保証する。転送は全 tasklet で手分けする。
 *
 * ldmai (MRAM -> IRAM の DMA) は 1 命令あたり最大 2048 バイト。転送語数
 * (64-bit 単位、1 + imm + ra[31:24]) は即値を実行時に変えられないため
 * IRAM 側アドレスレジスタの上位バイトに乗せる。MRAM アドレスはリンク時
 * 0x08000000 起点、実行時 0 起点。
 */
void ovl_load_slot(const uint32_t idx_slot, const uint8_t* const image_lma, const uint32_t nbytes)
{
    if (ovl_idx_loaded_slot == idx_slot) {
        return;
    }
    barrier_wait(&ovl_barrier);

    const uint32_t mram_begin = (uint32_t)image_lma & 0x07ffffffu;
    const uint32_t iram_begin = (uint32_t)__ovl_window_byte_addr;
    for (uint32_t offset = 2048u * me(); offset < nbytes; offset += 2048u * NR_TASKLETS) {
        const uint32_t rest = nbytes - offset;
        const uint32_t chunk = (rest > 2048u ? 2048u : rest);
        const uint32_t nr_words = chunk / 8u;
        __builtin_ldmai_rri((iram_begin + offset) | ((nr_words - 1u) << 24), mram_begin + offset, "0");
    }

    if (me() == 0) {
        ovl_idx_loaded_slot = idx_slot;
    }
    barrier_wait(&ovl_barrier);
}

#endif /* IRAM_OVERLAY */
