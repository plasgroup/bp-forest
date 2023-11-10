#include "split_phase.h"
#include "bplustree.h"
#include "common.h"
#include <assert.h>

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

void split_tree(seat_id_t seat_id, KVPairPtr tree_transfer_buffer, int num_trees, int num_elems)
{
    split_result[seat_id].num_split = num_trees;
    int num_elems_per_tree = num_elems / num_trees;
    for (int i = 0; i < num_trees; i++) {
        seat_id_t new_seat_id = Cabin_allocate_seat(-1);
        assert(new_seat_id != INVALID_SEAT_ID);
        init_BPTree(new_seat_id);
        int start_index = i * num_elems_per_tree;
        int transfer_num = (i == num_trees - 1) ? (num_elems_per_tree) : (num_elems - num_elems_per_tree * (num_trees - 1));
        BPTree_Deserialize(new_seat_id, tree_transfer_buffer, start_index, transfer_num);
        split_result[seat_id].num_elems[i] = transfer_num;
        if (i > 0) {
            split_result[seat_id].split_key[i - 1] = Seat_get_root(new_seat_id)->key[0];
        }
        split_result[seat_id].new_tree_index[i] = new_seat_id;
    }
    return;
}

int do_split_phase(seat_id_t seat_id, KVPairPtr tree_transfer_buffer)
{
    /* firstly, clear the former split_context */
    for (int j = 0; j < MAX_NUM_SPLIT; j++) {
        split_result[seat_id].new_tree_index[j] = 0;
        split_result[seat_id].num_elems[j] = 0;
        split_result[seat_id].split_key[j] = 0;
    }
    /* split if the size exceed the threshold */
    MBPTptr bpt = Seat_get_root(seat_id);
    if (traverse_and_count_elems(bpt) > SPLIT_THRESHOLD) {
        int num_trees = (traverse_and_count_elems(bpt) + SPLIT_THRESHOLD - 1) / SPLIT_THRESHOLD;
        assert(num_trees <= MAX_NUM_SPLIT);
        int num_elems = BPTree_Serialize(seat_id, tree_transfer_buffer);
        Cabin_release_seat(seat_id);
        split_tree(seat_id, tree_transfer_buffer, num_trees, num_elems);
        return 1;
    }
    return 0;
}
