#include "bplustree.h"
#include "common.h"
#include "split_phase.h"
#include <assert.h>
#include <stdio.h>

extern __mram KVPair tree_transfer_buffer[MAX_NUM_NODES_IN_SEAT * MAX_CHILD];
extern __mram merge_info_t merge_info;

void merge_phase()
{
    for (seat_id_t i = 0; i < NR_SEATS_IN_DPU; i++)
        if (merge_info.merge_to[i] != INVALID_SEAT_ID) {
            seat_id_t dest = merge_info.merge_to[i];
            printf("%d -> %d\n", i, merge_info.merge_to[i]);
            int n = BPTree_Serialize(i, tree_transfer_buffer);
            BPTree_Deserialize(dest, tree_transfer_buffer, 0, n);
            Cabin_release_seat(i);
        }
}