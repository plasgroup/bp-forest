# DPU 1 台の木に入る KV ペア数の上限を見積もる。
#
# 木に使える MRAM の量 (MRAM_FOR_TREE) とノードサイズ (SIZEOF_NODE) を対話的に受け取り、
# ノード数が MAX_NR_NODES = MRAM_FOR_TREE / SIZEOF_NODE 以下に収まる最大のペア数を
# 二分探索する。ビルド時の -DMRAM_FOR_TREE / -DSIZEOF_NODE を決めるための見積もり用。
#
# 見積もりの前提:
# * ここで求まるのは cold 木と hot 木の**合計**に対する上限。両者は同じノードプール
#   (dpu/inc/allocator.h の nodes_storage) を共有する。
# * 根ノードは WRAM 常駐 (dpu/src/bplustree.c の cold_root / hot_root) でプールを消費しない。
#   この計算は根も数えるので、木 1 本あたり 1 ノード分だけ安全側 (小さめ) に出る。

print "MRAMForTree = "
MRAMForTree = gets.strip.to_i

print "SizeOfNode = "
SizeOfNode = gets.strip.to_i

MaxNrNodes = MRAMForTree / SizeOfNode

# dpu/inc/bplustree.h の定義と同じ。
# 葉: keys[MaxNrPairs] + values[MaxNrPairs] + right (4B) + left (4B) + パディング
MaxNrPairs = (SizeOfNode - 16) / 16
# 内部: keys[MaxNrChildren - 1] (8B each) + children[MaxNrChildren] (4B each) を偶数に切り下げ
# (DEBUG_OCCUPANCY 有効時は numKeys の 4B を引いた (SizeOfNode + 8 - 4) / 12 / 2 * 2)
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
