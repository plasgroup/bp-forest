IRAM overlay for the DPU program
===

A DPU program that includes every task does not fit in IRAM (v1B: 3,968
instructions = 31,744 bytes). The code of the tasks is therefore placed in
MRAM, and at run time the DPU itself loads into IRAM only the code that the
launched task needs. The CMake option `DPU_IRAM_OVERLAY` enables this and is
ON by default, because the default build compiles every query type into the
DPU program. Turn it OFF for a build whose code fits in IRAM (e.g., to debug
with dpu-lldb); all code is then placed statically in IRAM.

## Structure

Tasks that never need to be in IRAM at the same time are grouped into
"slots", and all slots are placed over the same region at the end of IRAM
(the window) (`OVL_SLOT_*` in `dpu/inc/iram_overlay.h`):

| Slot | Contents |
| --- | --- |
| `OVL_SLOT_INSERT` | task_insert (including sorting the batch and the handout) and the upsert of TASK_MOVE_HOT |
| `OVL_SLOT_RESHARD` | rebalancing: building trees (including task_init), serialization, and destruction |
| `OVL_SLOT_QUERY` | the other query processing: get / pred / range_count / range_max |
| `OVL_SLOT_CHECK` | tree structure checks (non-empty only in builds that define `TASK_*_CHECK`) |
| `OVL_SLOT_DELETE` | the first half of task_delete: building the result array and sorting the keys |
| `OVL_SLOT_DELETE_TREE` | the second half of task_delete: removing pairs from the trees (including the handout and linking) |

The part that is kept in IRAM rather than loaded into the window (hereafter
**resident**) consists of main's task dispatch, functions called from more
than one slot (node allocation and freeing, binary search, synchronization
primitives, node transfer), the phase control of each task (the bodies of
`task_insert`, `task_delete`, `task_init`, and `task_move_hot`), the loader,
and so on. A function in another slot cannot be called, so shared code must
either be resident or be duplicated in each slot; we chose the former.

The tree structure check (`check_tree_structure`) has its own slot because
the check routine is large (3 KB or more), and keeping it resident would
shrink the window for every slot by that amount. The check is done after
each task has finished touching the trees, by calling `CHECK_trees` from the
resident phase control.

The resident part and all slots are linked into one ELF, so functions in a
slot can refer to resident functions and data in the ordinary way (with type
checking), and from the host's point of view there is still one binary and
one `dpu_load`. The linker determines the positions of the window, the load
images, and the heap; nothing is adjusted by hand. The effective capacity of
the window is "IRAM capacity − resident size", and exceeding it is a link
error.

## Rules for writing tasks

*   For an entry (a function called from resident code), write the signature
    part of its definition as
    `OVERLAY_TASK[_STATIC](slot, name, (params...), (args...))`. To callers,
    `<name>` looks like an ordinary function (it is actually a resident
    dispatch function that "loads the slot and calls the body `<name>_ovl`").
    Mark helpers inside a slot with `OVERLAY_LOCAL(slot)`.
*   All NR_TASKLETS tasklets call an entry with the same arguments, because
    all tasklets must reach the barriers before and after the load. Narrowing
    down the tasklets (`me() < TASK_*_NR_TASKLETS`) is done inside the entry.
*   A function in another slot cannot be called. If needed, go through a
    resident function and switch slots for each phase (e.g., the resident
    `task_move_hot`). Violations (calling `<name>_ovl` or a function in
    another slot directly) are not detected; at run time, "other code that
    happens to be in the window" silently runs.

## Limitations

*   dpu-lldb cannot debug code in a slot (it does not know what is in the
    window). Debug with a build that has the overlay disabled or with a
    dpu_on_cpu build.
*   Each slot switch costs a DMA of the image (10–20 KB, which occupies the
    DMA engine for 5,000–10,000 cycles) plus barriers. The load is skipped
    as long as the same slot is requested consecutively.

## Mechanism

*   `dpu/overlay/overlay_additions.lds` — additional definitions appended to
    the SDK's dpu.lds. It places the sections of all slots at the same VMA
    (= the window, right after the resident `.text`), and lays out the load
    images (LMA) right after the MRAM static data, aligned to 1024 B (an
    MRAM row). It exposes the position of the window and the LMA and size of
    each image to the loader as symbols, and moves the heap origin
    `__sys_used_mram_end` to right after the images by reassignment.
*   `dpu/overlay/link_overlay.sh` — extracts the path of dpu.lds from the
    driver's link line, generates the linker script to use by two
    transformations ("formally widen the LENGTH of the iram region" and
    "append the additional definitions"), and runs the link with `-T`
    replaced (so it follows SDK updates automatically).
*   `dpu/overlay/apply_overlay_lma.py` — `dpu_load` ignores the LMA, so this
    rewrites the addresses of the overlay segments to their LMA so that they
    are placed in MRAM.
*   `dpu/src/overlay.c` — remembers which slot is in the window, and only
    when a different slot is requested, loads it with `ldmai` (MRAM → IRAM
    DMA) between barriers of all tasklets.
