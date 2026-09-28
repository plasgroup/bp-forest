# Estimate the maximum number of KV pairs that fit in the tree of one DPU.
#
# Reads the amount of MRAM available for the tree (MRAM_FOR_TREE) and the node size (SIZEOF_NODE)
# interactively, and binary-searches for the largest number of pairs whose node count fits within
# MAX_NR_NODES = MRAM_FOR_TREE / SIZEOF_NODE. An estimate for choosing -DMRAM_FOR_TREE / -DSIZEOF_NODE at build time.
#
# Assumptions of the estimate:
# * The result is a limit on the **total** of the cold tree and the hot tree. Both share the same node pool
#   (nodes_storage in dpu/inc/allocator.h).
# * Root nodes are resident in WRAM (cold_root / hot_root in dpu/src/bplustree.c) and do not consume the pool.
#   This calculation counts the roots too, so it errs on the safe (smaller) side by one node per tree.

print "MRAMForTree = "
MRAMForTree = gets.strip.to_i

print "SizeOfNode = "
SizeOfNode = gets.strip.to_i

MaxNrNodes = MRAMForTree / SizeOfNode

# Same as the definitions in dpu/inc/bplustree.h.
# Leaf: values[MaxNrPairs] + right (4B + 4B padding) + keys[MaxNrPairs] + left (4B + 4B padding)
MaxNrPairs = (SizeOfNode - 16) / 16
# Internal: children[MaxNrChildren] (4B each) + keys[MaxNrChildren - 1] (8B each), rounded down to an even number
# (with DEBUG_OCCUPANCY enabled, (SizeOfNode + 8 - 4) / 12 / 2 * 2, subtracting the 4B of numKeys)
MaxNrChildren = (SizeOfNode + 8) / 12 / 2 * 2

def nr_pairs_to_nr_nodes(nr_pairs)
  nr_nodes = 0

  nr_leaves = (nr_pairs + MaxNrPairs - 1) / MaxNrPairs
  nr_nodes += nr_leaves

  nr_nodes_in_prev_layer = nr_leaves
  while nr_nodes_in_prev_layer != 1
    nr_internal_nodes_in_a_layer = (nr_nodes_in_prev_layer + MaxNrChildren - 1) / MaxNrChildren
    nr_nodes += nr_internal_nodes_in_a_layer
    nr_nodes_in_prev_layer = nr_internal_nodes_in_a_layer
  end

  nr_nodes
end

left = 0
right = 1
while nr_pairs_to_nr_nodes(right) <= MaxNrNodes
  right *= 2
end

while right - left > 1
  mid = (right - left) / 2 + left
  if nr_pairs_to_nr_nodes(mid) <= MaxNrNodes
    left = mid
  else
    right = mid
  end
end

actual_nr_nodes = nr_pairs_to_nr_nodes(left)
print "#{left} pairs -> #{actual_nr_nodes} nodes -> #{actual_nr_nodes * SizeOfNode} bytes <= MRAMForTree (#{MRAMForTree} bytes)\n"
