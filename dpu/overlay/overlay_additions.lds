/*
 * IRAM overlay の追加定義。link_overlay.sh が SDK の dpu.lds (iram の
 * LENGTH を形式上広げたもの) の後ろに連結して使う。
 */
SECTIONS
{
  /* 各スロットの VMA = IRAM 窓 (常駐 .text の直後)、LMA (ロードイメージ)
   * は MRAM 静的データ (.mram) の直後に、DMA が MRAM 行をまたぎにくい
   * よう行サイズ (1024B) 整列で並ぶ。単一の OVERLAY 文はメンバの LMA を
   * 隙間なく連続配置してしまうため、スロットごとに OVERLAY 文を分けて
   * AT() で整列を指定する。 */
  OVERLAY ALIGN(ADDR(.text) + SIZEOF(.text), 8) : AT (ALIGN(ADDR(.mram) + SIZEOF(.mram), 1024)) {
    ovl0 { *(ovl0 ovl0.*) }
  }
  OVERLAY ADDR(ovl0) : AT (ALIGN(LOADADDR(ovl0) + SIZEOF(ovl0), 1024)) {
    ovl1 { *(ovl1 ovl1.*) }
  }
  OVERLAY ADDR(ovl0) : AT (ALIGN(LOADADDR(ovl1) + SIZEOF(ovl1), 1024)) {
    ovl2 { *(ovl2 ovl2.*) }
  }

  /* IRAM の実容量検査。iram 領域の LENGTH では検査できない
   * (link_overlay.sh 参照)。 */
  ASSERT(ADDR(.text) + SIZEOF(.text) <= 0x80000000 + DPU_IRAM_SIZE, "resident .text does not fit in IRAM")
  ASSERT(ADDR(ovl0) + SIZEOF(ovl0) <= 0x80000000 + DPU_IRAM_SIZE, "overlay ovl0 does not fit in IRAM")
  ASSERT(ADDR(ovl1) + SIZEOF(ovl1) <= 0x80000000 + DPU_IRAM_SIZE, "overlay ovl1 does not fit in IRAM")
  ASSERT(ADDR(ovl2) + SIZEOF(ovl2) <= 0x80000000 + DPU_IRAM_SIZE, "overlay ovl2 does not fit in IRAM")

  /* 窓の IRAM バイトアドレス。0x80000000 を引いた素の数値にしておくと
   * C 側でそのまま使える。ABSOLUTE がないと ovl0 所属シンボルになる。 */
  __ovl_window_byte_addr = ABSOLUTE(ADDR(ovl0) - 0x80000000);

  __ovl_load_ovl0 = LOADADDR(ovl0);
  __ovl_size_ovl0 = SIZEOF(ovl0);
  __ovl_load_ovl1 = LOADADDR(ovl1);
  __ovl_size_ovl1 = SIZEOF(ovl1);
  __ovl_load_ovl2 = LOADADDR(ovl2);
  __ovl_size_ovl2 = SIZEOF(ovl2);

  /*
   * MRAM ヒープの起点 __sys_used_mram_end (DPU_MRAM_HEAP_POINTER) を
   * SDK 定義 (.mram 末尾) から overlay イメージの直後へ再代入で動かし、
   * ヒープとイメージの衝突を防ぐ。トップレベルの代入だと ABS シンボルに
   * なりホストの dpu_get_symbol が拒否するため、イメージ末尾に置いた
   * セクションの中で定義する (8 バイトのダミーは空セクションの削除よけ)。
   */
  .mram.heap_origin LOADADDR(ovl2) + SIZEOF(ovl2) (NOLOAD) : {
    __sys_used_mram_end = .;
    . += 8;
  } > mram
}
