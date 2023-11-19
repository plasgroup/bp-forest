#include "bplustree.h"
#include "common.h"
#include "split_phase.h"
#include <assert.h>
#include <stdio.h>

extern __mram KVPair tree_transfer_buffer[MAX_NUM_NODES_IN_SEAT * MAX_CHILD];
extern __mram merge_info_t merge_info;


static seat_id_t merge_tree(seat_id_t seat_id, KVPairPtr buffer, int nkeys)
{
    seat_id = Cabin_allocate_seat(seat_id);
    printf("%d:", seat_id);
    assert(seat_id != INVALID_SEAT_ID);
    init_BPTree(seat_id);
    BPTree_Deserialize(seat_id, buffer, 0, nkeys);
    return seat_id;
}

void merge_phase()
{
    int index = 0;
    for (int i = 0; i < merge_info.num_merge; i++) {
        int nkeys = 0;
        for (int j = 0; j < merge_info.tree_nums[i]; j++) {
            seat_id_t merged_tree = merge_info.merge_list[index];
            nkeys += BPTree_Serialize_start_index(merged_tree, tree_transfer_buffer, nkeys);
            Cabin_release_seat(merged_tree);
            index++;
        }
        /* merge to the last tree in merge_list */
        merge_tree(merge_info.merge_list[index - 1], tree_transfer_buffer, nkeys);
    }
}