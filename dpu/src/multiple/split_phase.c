#include "split_phase.h"
#include "bplustree.h"
#include "common.h"
#include <assert.h>
#include <stdio.h>

extern __mram KVPair tree_transfer_buffer[MAX_NUM_NODES_IN_SEAT * MAX_CHILD];
extern __mram split_info_t split_result[NR_SEATS_IN_DPU];


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
        printf("%d, ", new_seat_id);
        result->num_elems[i] = end - start;
        result->new_tree_index[i] = new_seat_id;
    }
    for (int i = 1; i < num_trees; i++) {
        int start = n * i / num_trees;
        result->split_key[i - 1] = buffer[start].key;
    }
    result->num_split = num_trees;
    printf("\n");
}

void split_phase()
{
    clear_split_result();
    for (seat_id_t seat_id = 0; seat_id < NR_SEATS_IN_DPU; seat_id++)
        if (Seat_is_used(seat_id)) {
            // TODO: check number of elements in advance
            int n = BPTree_Serialize(seat_id, tree_transfer_buffer);
            if (n > SPLIT_THRESHOLD) {
                printf("split: seat %d -> ", seat_id);
                Cabin_release_seat(seat_id);
                split_tree(tree_transfer_buffer, n, &split_result[seat_id]);
            }
        }
}
