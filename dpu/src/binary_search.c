#include "binary_search.h"

#include <limits.h>


// // @return index of the appropriate tree if exists, UINT_MAX otherwise
// unsigned tree_responsible_for_get_request_with(key_int64_t key)
// {
//     // return (unsigned)(std::upper_bound(begin(lower_bounds) + 1, end(lower_bounds), key) - (begin(lower_bounds) + 1));
//     //
//     // implement the above line as branchless binary search

//     const unsigned num_comp = BITWIDTH_UINT32((uint32_t)(num_of_elems));
//     const unsigned first_idx = num_of_elems - (1u << (num_comp - 1u));
//     unsigned pos = (key < lower_bounds[first_idx] ? UINT_MAX : first_idx);

//     for (unsigned i = num_comp - 1u; i != 0u; i--) {
//         const unsigned step = 1u << (i - 1u);
//         pos = (key < lower_bounds[pos + step] ? pos : pos + step);
//     }

//     return pos;
// }
