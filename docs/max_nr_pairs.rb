print "MRAMForTree = "
MRAMForTree = gets.strip.to_i

print "SizeOfNode = "
SizeOfNode = gets.strip.to_i

MaxNrNodes = MRAMForTree / SizeOfNode

MaxNrPairs = (SizeOfNode - 16) / 16
MaxNrChildren = (SizeOfNode + 8) / 12

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
