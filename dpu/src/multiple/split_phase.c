#include "split_phase.h"
#include "bplustree.h"
#include "common.h"
#include <assert.h>

extern KVPairPtr tree_transfer_buffer;
extern __mram split_info_t split_result[NR_SEATS_IN_DPU];

/* TODO: move to somewhere */
int traverse_and_count_elems(MBPTptr node)
{
    int elems = node->numKeys;
    if (!node->isLeaf) {
        for (int i = 0; i <= node->numKeys; i++) {
            elems += traverse_and_count_elems(node->ptrs.inl.children[i]);
        }
    }
    return elems;
}
/* END move to somewhere */

static void clear_split_result()
{
    memset(split_result, 0, sizeof(split_result));
}

static seat_id_t create_split_tree(KVPairPtr buffer, int start, int end)
{
    seat_id_t seat_id = Cabin_allocate_seat(INVALID_SEAT_ID);
    assert(seat_id != INVALID_SEAT_ID);
    init_BPTree(seat_id);
    BPTree_Deserialize(seat_id, buffer, start, end - start);
    return seat_id;
}

static void split_tree(KVPairPtr buffer, int n, __mram_ptr split_info_t* result)
{
    int num_trees = (n + NR_ELEMS_AFTER_SPLIT - 1) / NR_ELEMS_AFTER_SPLIT;
    assert(num_trees <= MAX_NUM_SPLIT);
    for (int i = 0; i < num_trees; i++) {
        int start = n * i / num_trees;
        int end = n * (i + 1) / num_trees;
        seat_id_t new_seat_id = create_split_tree(buffer, start, end);
        result->num_elems[i] = end - start;
        result->new_tree_index[i] = new_seat_id;
    }
    for (int i = 1; i < num_trees; i++) {
        int start = n * i / num_trees;
        result->split_key[i - 1] = buffer[start].key;
    }
    result->num_split = num_trees;
}

void split_phase()
{
    clear_split_result();
    for (seat_id_t seat_id = 0; seat_id < NR_SEATS_IN_DPU; seat_id++)
        if (Seat_is_used(seat_id)) {
            // TODO: check number of elements in advance
            int n = BPTree_Serialize(seat_id, tree_transfer_buffer);
            if (n > SPLIT_THRESHOLD) {
                Cabin_release_seat(seat_id);
                split_tree(tree_transfer_buffer, n, &split_result[seat_id]);
            }
        }
}