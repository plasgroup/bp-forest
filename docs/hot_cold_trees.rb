HOT_MAX_NR_ELEMS = 3

COLD_MAX_NR_QUERIES = 2
COLD_MAX_NR_ELEMS = 10

NR_DPUS = 10
NR_ELEMS = 70

$query_dist = [1] * 10 + [1/10r] * 60
raise unless $query_dist.length == NR_ELEMS

# ある要素から入れられる限りcold treeに入れるとして、次の要素はどれになるか
cold_span = Array.new(NR_ELEMS) { nil }
idx_begin_elem = 0
sum_nr_queries = 0
NR_ELEMS.times do |idx_elem|
    sum_nr_queries += $query_dist[idx_elem]
    if idx_elem - idx_begin_elem + 1 > COLD_MAX_NR_ELEMS
        cold_span[idx_begin_elem] = idx_elem
        sum_nr_queries -= $query_dist[idx_begin_elem]
        idx_begin_elem += 1
    end
    while sum_nr_queries > COLD_MAX_NR_QUERIES
        cold_span[idx_begin_elem] = idx_elem
        sum_nr_queries -= $query_dist[idx_begin_elem]
        idx_begin_elem += 1
    end
end
(idx_begin_elem...NR_ELEMS).each do |i|
    cold_span[i] = NR_ELEMS
end
p cold_span

# ある要素の一つ前から遡りつつ、入れられる限りcold treeに入れるとして、最初の要素はどれになるか
reverse_cold_span = Array.new(NR_ELEMS + 1) { nil }
idx_span_end = 0
NR_ELEMS.times do |idx_elem|
    while idx_span_end <= cold_span[idx_elem]
        reverse_cold_span[idx_span_end] = idx_elem
        idx_span_end += 1
    end
end
p reverse_cold_span

# nr_hot_queries[ie][id] = (先頭のie個の要素は、cold treeをid個作ると、hot treeに行くクエリをいくつまで減らせるか)
nr_hot_queries = Array.new(NR_ELEMS + 1) { Array.new(NR_DPUS) { nil } }

nr_hot_queries[0][0] = 0
NR_ELEMS.times do |nr_elems|
    NR_DPUS.times do |nr_cold_trees|
        here = nr_hot_queries[nr_elems][nr_cold_trees]
        next if here.nil?

        after_new_cold_tree = here
        if nr_cold_trees + 1 < NR_DPUS
            if nr_hot_queries[cold_span[nr_elems]][nr_cold_trees + 1].nil? \
                || nr_hot_queries[cold_span[nr_elems]][nr_cold_trees + 1] > after_new_cold_tree

                nr_hot_queries[cold_span[nr_elems]][nr_cold_trees + 1] = after_new_cold_tree
            end
        end

        after_new_hot_elem = here + $query_dist[nr_elems]
        if nr_hot_queries[nr_elems + 1][nr_cold_trees].nil? \
            || nr_hot_queries[nr_elems + 1][nr_cold_trees] > after_new_hot_elem

            nr_hot_queries[nr_elems + 1][nr_cold_trees] = after_new_hot_elem
        end
    end
end
pp nr_hot_queries


# hot_nr_elems = Array.new(NR_DPUS) { nil }
# cold_nr_elems = Array.new(NR_DPUS) { nil }

# def partition_cold_only(elem_range, dpu_range)
#     idx_elem = elem_range.min
#     if idx_elem.nil?
#         return dpu_range.map { nil }
#     end
#     return dpu_range.map {
#         next nil unless elem_range.include?(idx_elem)

#         query_cap = COLD_MAX_NR_QUERIES
#         idx_elem_in_dpu = 0
#         loop do
#             break [:END_OF_ELEMS, idx_elem_in_dpu] unless elem_range.include?(idx_elem)
#             break [:ELEM_LIMIT, idx_elem_in_dpu] if idx_elem_in_dpu >= COLD_MAX_NR_ELEMS
#             break [:QUERY_LIMIT, idx_elem_in_dpu] if query_cap < $query_dist[idx_elem]

#             query_cap -= $query_dist[idx_elem]
#             idx_elem = idx_elem.next
#             idx_elem_in_dpu += 1
#         end
#     }, !elem_range.include?(idx_elem)
# end

# def partition_cold_only_reverse(elem_range, dpu_range)
#     idx_elem = elem_range.max
#     if idx_elem.nil?
#         return dpu_range.map { nil }
#     end
#     return dpu_range.reverse_each.map {
#         next nil unless elem_range.include?(idx_elem)

#         query_cap = COLD_MAX_NR_QUERIES
#         idx_elem_in_dpu = 0
#         loop do
#             break [:END_OF_ELEMS, idx_elem_in_dpu] unless elem_range.include?(idx_elem)
#             break [:ELEM_LIMIT, idx_elem_in_dpu] if idx_elem_in_dpu >= COLD_MAX_NR_ELEMS
#             break [:QUERY_LIMIT, idx_elem_in_dpu] if query_cap < $query_dist[idx_elem]

#             query_cap -= $query_dist[idx_elem]
#             idx_elem = idx_elem.pred
#             idx_elem_in_dpu += 1
#         end
#     }.reverse, !elem_range.include?(idx_elem)
# end

# p partition_cold_only_reverse(0...NR_ELEMS, 0...NR_DPUS)

# # idx_elem = 0
# # NR_DPUS.times do |idx_dpu|
# #     query_cap = COLD_MAX_NR_QUERIES
# #     idx_elem_in_dpu = 0
# #     loop do
# #         if idx_elem >= NR_ELEMS || idx_elem_in_dpu >= COLD_MAX_NR_ELEMS
# #             cold_nr_elems[idx_dpu] = [:ELEM_LIMIT, idx_elem_in_dpu]
# #             break
# #         end
# #         if query_cap < $query_dist[idx_elem]
# #             cold_nr_elems[idx_dpu] = [:QUERY_LIMIT, idx_elem_in_dpu]
# #             break
# #         end
# #         query_cap -= $query_dist[idx_elem]
# #         idx_elem += 1
# #         idx_elem_in_dpu += 1
# #     end
# # end

# # p cold_nr_elems
