#ifndef __MIGRATION_HPP__
#define __MIGRATION_HPP__

#include <iostream>
#include <map>
#include <stdio.h>
#include <stdlib.h>
#include <utility>
#include <vector>

#include "common.h"
#include "host_data_structures.hpp"

class HostTree;
class BatchCtx;

class Migration
{
public:
    class MigrationPlanIterator : public std::iterator<std::input_iterator_tag, std::pair<seat_addr_t, seat_addr_t>>
    {

        friend Migration;

        typedef std::pair<seat_addr_t, seat_addr_t> item_t;

    private:
        Migration* migration;
        seat_addr_t p;

        bool inc_p()
        {
            p.seat++;
            if (p.seat == NR_SEATS_IN_DPU) {
                p.dpu++;
                p.seat = 0;
                if (p.dpu == NR_DPUS) {
                    p = seat_addr_t(-1, INVALID_SEAT_ID);
                    return false;
                }
            }
            return true;
        }

        void locate_next()
        {
            while (migration->plan[p.dpu][p.seat].dpu == -1)
                if (!inc_p())
                    return;
        }

        void advance()
        {
            inc_p();
            locate_next();
        }

        MigrationPlanIterator(Migration* m, bool begin) : migration(m)
        {
            if (begin) {
                p = seat_addr_t(0, 0);
                locate_next();
            } else
                p = seat_addr_t(-1, INVALID_SEAT_ID);
        }

    public:
        MigrationPlanIterator& operator++()
        {
            advance();
            return *this;
        }

        MigrationPlanIterator operator++(int)
        {
            MigrationPlanIterator pre = *this;
            advance();
            return pre;
        }

        item_t operator*()
        {
            return {migration->plan[p.dpu][p.seat], p};
        }

        bool operator==(const MigrationPlanIterator& it)
        {
            return it.migration == migration && it.p == p;
        }

        bool operator!=(const MigrationPlanIterator& it)
        {
            return !(*this == it);
        }
    };

private:
    seat_addr_t plan[NR_DPUS][NR_SEATS_IN_DPU];
    seat_set_t used_seats[NR_DPUS];
    seat_set_t freeing_seats[NR_DPUS];
    int nr_used_seats[NR_DPUS];
    int nr_freeing_seats[NR_DPUS];

public:
    Migration(HostTree* tree);
    int get_num_queries_for_source(BatchCtx& batch_ctx, uint32_t dpu, seat_id_t seat_id);
    seat_addr_t get_source(uint32_t dpu, seat_id_t seat_id);
    void migration_plan_query_balancing(BatchCtx& batch_ctx, int num_migration);
    void migration_plan_memory_balancing(void);
    void migration_plan_for_merge(HostTree* host_tree, merge_info_t* merge_list);
    void normalize(void);
    void execute(void);
    void print_plan(void);

    MigrationPlanIterator begin()
    {
        return MigrationPlanIterator(this, true);
    }

    MigrationPlanIterator end()
    {
        return MigrationPlanIterator(this, false);
    }

private:
    seat_id_t find_available_seat(uint32_t dpu);
    void do_migrate_subtree(uint32_t from_dpu, seat_id_t from, uint32_t to_dpu, seat_id_t to);
    void migrate_subtree(uint32_t from_dpu, seat_id_t from, uint32_t to_dpu, seat_id_t to);
    bool migrate_subtree_to_balance_load(uint32_t from_dpu, uint32_t to_dpu, int diff, int nkeys_for_trees[NR_DPUS][NR_SEATS_IN_DPU]);
    void migrate_subtrees(uint32_t from_dpu, uint32_t to_dpu, int n);
    bool plan_merge(seat_addr_t left, seat_addr_t right, merge_info_t* merge_list);
};

#endif /* __MIGRATION_HPP__ */