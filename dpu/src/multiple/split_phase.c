#include "bplustree.h"
#include "common.h"
#include "split_phase.h"

// int traverse_and_count_elems(MBPTptr node)
// {
//     int elems = node->node.numKeys;
//     if (!node->node.isLeaf) {
//         for (int i = 0; i <= node->node.numKeys; i++) {
//             elems += count_elems_recursive(node->node.ptrs.inl.children[i]);
//         }
//     }
//     return elems;
// }
// int do_split_phase(MBPTptr bpt)
// {
//     /* firstly, clear the former split_context */
//     for (int i = 0; i < NR_SEATS_IN_DPU; i++) {
//         for (int j = 0; j < MAX_NUM_SPLIT; j++) {
//             split_result[i].new_tree_index[j] = 0;
//             split_result[i].num_elems[j] = 0;
//             split_result[i].split_key[j] = 0;
//         }
//     }
//     /* split if the size exceed the threshold */
//     if (traverse_and_count_elems(bpt) > SPLIT_THRESHOLD) {
//         split_tree(bpt);
//         return 1;
//     }
//     return 0;
// }
// // void split_tree(MBPTptr bpt)
// // {
// //     int num_trees = (traverse_and_count_elems(bpt) + SPLIT_THRESHOLD - 1) / SPLIT_THRESHOLD;
// //     int num_elems_per_tree = traverse_and_count_elems(bpt) / num_trees;
// //     MBPTptr cur, n;
// //     n = bpt;
// //     for (int i = 0; i < num_trees - 1; i++) {
// //         int num_elems = 0;
// //         cur = n;
// //         int j = 0;
// //         while (num_elems < num_elems_per_tree) {
// //             num_elems += traverse_and_count_elems(cur->ptrs.inl.children[j]);
// //             j++;  // divide 0~j-1, j~num_children-1
// //         }
// //         key_int64_t split_key;
// //         n = malloc_tree();
// //         int Mid = j;
// //         n->isLeaf = cur->isLeaf;
// //         n->numKeys = cur->numKeys - Mid;
// //         // n is InternalNode
// //         // TODO: key-value pairのリストに変換する
// //         for (int i = Mid; i < cur->numKeys; i++) {
// //             n->ptrs.inl.children[i - Mid] = cur->ptrs.inl.children[i];
// //             n->key[i - Mid] = cur->key[i];
// //             n->ptrs.inl.children[i - Mid]->parent = n;
// //             cur->numKeys = Mid - 1;
// //         }
// //         n->ptrs.inl.children[cur->numKeys - Mid] = cur->ptrs.inl.children[cur->numKeys];
// //         n->ptrs.inl.children[cur->numKeys - Mid]->parent = n;
// //         split_key = cur->key[Mid - 1];
// //         split_result[n->tree_index].num_elems[i] = num_elems;
// //         split_result[bpt->tree_index].num_elems[i] -= split_result[bpt->tree_index].num_elems[n->tree_index];
// //         split_result[bpt->tree_index].split_key[i] = split_key;
// //         split_result[bpt->tree_index].new_tree_index[i] = n->tree_index;
// //     }
// //     return;
// // }


