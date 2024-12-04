NrDPUs = 2560
# NrDPUs = 8

print "ZipfParam = "
ZipfParam = gets.chomp.to_f
print "ZipfParam = #{ZipfParam}\n"

print "BalancingParam = "
BalancingParam = gets.chomp.to_i
print "BalancingParam = #{BalancingParam}\n"

print "NrElemsPerDPU = "
NrElemsPerDPU = gets.chomp.to_i
print "NrElemsPerDPU = #{NrElemsPerDPU}\n"

p rel_prob_per_dpu = Array.new(NrElemsPerDPU * NrDPUs) {|i|
  (i+1) ** (-ZipfParam) 
}.each_slice(NrElemsPerDPU).map {|rel_prob_per_elem|
  rel_prob_per_elem.sum
}
sum_rel_prob = rel_prob_per_dpu.sum
hot_rel_prob = sum_rel_prob / NrDPUs

max_cold_rel_prob = hot_rel_prob * BalancingParam

p max_nr_hot_ranges = rel_prob_per_dpu.map {|rel_prob|
  [((rel_prob - max_cold_rel_prob) / hot_rel_prob).to_i, 0].max
}
print "MaxNrHotRanges = #{max_nr_hot_ranges.sum}\n"

p max_hot_range_sizes = max_nr_hot_ranges.map {|nr_hots|
  [nr_hots, BalancingParam].min.to_f / BalancingParam
}
print "MaxCommForRebalancing = #{max_hot_range_sizes.sum}\n"
