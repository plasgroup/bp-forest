#include "split_phase.h"
#include "bplustree.h"
#include "common.h"

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

void split_tree(seat_id_t seat_id, KVPairPtr tree_transfer_buffer, int num_trees)
{
    split_result[seat_id].num_split = 0;
    MBPTptr bpt = Seat_get_root(seat_id);
    int num_elems_before = traverse_and_count_elems(bpt);
    int num_elems_per_tree = num_elems_before / num_trees;
    split_result[seat_id].num_elems_after = num_elems_before;
    // tree to be split
    MBPTptr cur;
    // new tree made by split
    MBPTptr n;
    for (int i = 0; i < num_trees - 1; i++) {
        int num_elems = 0;
        int j = 0;
        while (num_elems < num_elems_per_tree) {
            num_elems += traverse_and_count_elems(cur->ptrs.inl.children[cur->numKeys - j]);
            j++;  // divide 0~j-1, j~num_children-1
            if(j == cur->numKeys) return;
        }
        key_int64_t split_key;
        seat_id_t n_seat_id = Cabin_allocate_seat(-1);
        init_BPTree(n_seat_id);
        n = Seat_get_root(n_seat_id);
        // n is InternalNode
        int transfer_num = BPTree_Serialize_j_Last_Subtrees(cur, tree_transfer_buffer, j);
        BPTree_Deserialize(seat_id, tree_transfer_buffer, 0, transfer_num);
        // TODO: free nodes cur->children[numKeys - j ~ numKeys] recursively
        cur->numKeys = cur->numKeys - j;
        split_key = cur->key[cur->numKeys-1];
        split_result[seat_id].num_elems[i] = num_elems;
        split_result[seat_id].num_split++;
        split_result[seat_id].num_elems_after -= num_elems;
        split_result[seat_id].split_key[i] = split_key;
        split_result[seat_id].new_tree_index[i] = n_seat_id;
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
    if (traverse_and_count_elems(bpt) > 2 * SPLIT_THRESHOLD) {
        int num_trees = (traverse_and_count_elems(bpt) + SPLIT_THRESHOLD - 1) / SPLIT_THRESHOLD;
        split_tree(seat_id, tree_transfer_buffer, num_trees);
        return 1;
    }
    return 0;
}

