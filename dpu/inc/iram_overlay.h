#pragma once
/*
 * IRAM overlay: the code of the tasks is placed in MRAM, and at run time
 * only the needed slot is loaded into the "window" at the end of IRAM and
 * called. See docs/dpu_iram_overlay.md. In a build without IRAM_OVERLAY
 * defined, everything here has no effect.
 */

#define OVL_SLOT_INSERT 0      /* task_insert and the upsert engine */
#define OVL_SLOT_RESHARD 1     /* rebalancing: building, serializing, and destroying trees */
#define OVL_SLOT_QUERY 2       /* the other query processing */
#define OVL_SLOT_CHECK 3       /* tree structure checks (non-empty only in check builds) */
#define OVL_SLOT_DELETE 4      /* first half of task_delete: building the result array and sorting the keys */
#define OVL_SLOT_DELETE_TREE 5 /* second half of task_delete: removing pairs from the trees */

#ifdef IRAM_OVERLAY

#include <stdint.h>

#define OVL_STR_EXPANDED_(x) #x
#define OVL_STR_(x) OVL_STR_EXPANDED_(x)

/* For entries (functions called from resident code). noinline is required:
 * if inlined into the caller, the body would end up in the resident .text. */
#define OVERLAY(slot) __attribute__((noinline, used, section("ovl" OVL_STR_(slot))))
/* For helpers inside a slot. Inlining is allowed (the caller is in the same slot). */
#define OVERLAY_LOCAL(slot) __attribute__((section("ovl" OVL_STR_(slot))))

/* Loads the image of the slot into the window (does nothing if already loaded).
 * All NR_TASKLETS tasklets must call it with the same arguments. */
void ovl_load_slot(uint32_t idx_slot, const uint8_t* image_lma, uint32_t nbytes);

/*
 * To define an entry, write the signature part as
 * OVERLAY_TASK[_STATIC](slot, name, (params...), (args...)).
 * To callers, <name> looks like an ordinary function: it is actually a
 * resident dispatch function that "loads the slot and calls the body", and
 * the body is placed in the slot as <name>_ovl. In a build with the overlay
 * disabled, this becomes a plain function definition.
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
