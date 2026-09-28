/*
 * Additional definitions for the IRAM overlay. link_overlay.sh appends them
 * to the SDK's dpu.lds (with the LENGTH of iram formally widened).
 */
SECTIONS
{
  /* The VMA of each slot is the IRAM window (right after the resident
   * .text). The LMAs (load images) follow the MRAM static data (.mram),
   * aligned to the row size (1024 B) so that the DMA crosses fewer MRAM
   * rows. A single OVERLAY statement would place the LMAs of its members
   * back to back with no gap, so each slot has its own OVERLAY statement,
   * with the alignment given by AT(). */
  OVERLAY ALIGN(ADDR(.text) + SIZEOF(.text), 8) : AT (ALIGN(ADDR(.mram) + SIZEOF(.mram), 1024)) {
    ovl0 { *(ovl0 ovl0.*) }
  }
  OVERLAY ADDR(ovl0) : AT (ALIGN(LOADADDR(ovl0) + SIZEOF(ovl0), 1024)) {
    ovl1 { *(ovl1 ovl1.*) }
  }
  OVERLAY ADDR(ovl0) : AT (ALIGN(LOADADDR(ovl1) + SIZEOF(ovl1), 1024)) {
    ovl2 { *(ovl2 ovl2.*) }
  }
  OVERLAY ADDR(ovl0) : AT (ALIGN(LOADADDR(ovl2) + SIZEOF(ovl2), 1024)) {
    ovl3 { *(ovl3 ovl3.*) }
  }
  OVERLAY ADDR(ovl0) : AT (ALIGN(LOADADDR(ovl3) + SIZEOF(ovl3), 1024)) {
    ovl4 { *(ovl4 ovl4.*) }
  }
  OVERLAY ADDR(ovl0) : AT (ALIGN(LOADADDR(ovl4) + SIZEOF(ovl4), 1024)) {
    ovl5 { *(ovl5 ovl5.*) }
  }

  /* Check against the real IRAM capacity. The LENGTH of the iram region
   * cannot be used for this (see link_overlay.sh). */
  ASSERT(ADDR(.text) + SIZEOF(.text) <= 0x80000000 + DPU_IRAM_SIZE, "resident .text does not fit in IRAM")
  ASSERT(ADDR(ovl0) + SIZEOF(ovl0) <= 0x80000000 + DPU_IRAM_SIZE, "overlay ovl0 does not fit in IRAM")
  ASSERT(ADDR(ovl1) + SIZEOF(ovl1) <= 0x80000000 + DPU_IRAM_SIZE, "overlay ovl1 does not fit in IRAM")
  ASSERT(ADDR(ovl2) + SIZEOF(ovl2) <= 0x80000000 + DPU_IRAM_SIZE, "overlay ovl2 does not fit in IRAM")
  ASSERT(ADDR(ovl3) + SIZEOF(ovl3) <= 0x80000000 + DPU_IRAM_SIZE, "overlay ovl3 does not fit in IRAM")
  ASSERT(ADDR(ovl4) + SIZEOF(ovl4) <= 0x80000000 + DPU_IRAM_SIZE, "overlay ovl4 does not fit in IRAM")
  ASSERT(ADDR(ovl5) + SIZEOF(ovl5) <= 0x80000000 + DPU_IRAM_SIZE, "overlay ovl5 does not fit in IRAM")

  /* IRAM byte address of the window. Subtracting 0x80000000 gives a plain
   * number that C code can use as is. Without ABSOLUTE, the symbol would
   * belong to ovl0. */
  __ovl_window_byte_addr = ABSOLUTE(ADDR(ovl0) - 0x80000000);

  __ovl_load_ovl0 = LOADADDR(ovl0);
  __ovl_size_ovl0 = SIZEOF(ovl0);
  __ovl_load_ovl1 = LOADADDR(ovl1);
  __ovl_size_ovl1 = SIZEOF(ovl1);
  __ovl_load_ovl2 = LOADADDR(ovl2);
  __ovl_size_ovl2 = SIZEOF(ovl2);
  __ovl_load_ovl3 = LOADADDR(ovl3);
  __ovl_size_ovl3 = SIZEOF(ovl3);
  __ovl_load_ovl4 = LOADADDR(ovl4);
  __ovl_size_ovl4 = SIZEOF(ovl4);
  __ovl_load_ovl5 = LOADADDR(ovl5);
  __ovl_size_ovl5 = SIZEOF(ovl5);

  /*
   * Move the MRAM heap origin __sys_used_mram_end (DPU_MRAM_HEAP_POINTER)
   * by reassignment from its SDK definition (end of .mram) to right after
   * the overlay images, so that the heap does not collide with the images.
   * A top-level assignment would make it an ABS symbol, which the host's
   * dpu_get_symbol rejects, so it is defined inside a section placed at the
   * end of the images (the 8-byte dummy keeps the empty section from being
   * discarded).
   */
  .mram.heap_origin LOADADDR(ovl5) + SIZEOF(ovl5) (NOLOAD) : {
    __sys_used_mram_end = .;
    . += 8;
  } > mram
}
