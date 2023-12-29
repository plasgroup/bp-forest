#ifndef __MIGRATION_HPP__
#define __MIGRATION_HPP__

#include "common.h"
#ifndef NO_DPU_EXECUTION
extern "C" {
#include <dpu.h>
#include <dpu_log.h>
}
#endif /* NO_DPU_EXECUTION */
#include <iostream>
#include <map>
#include <stdio.h>
#include <stdlib.h>
#include <utility>
#include <vector>

class HostTree;
class BatchCtx;

class Migration
{
public:
    using Position = std::pair<dpu_id_t, seat_id_t>;

    class MigrationPlanIterator : public std::iterator<std::input_iterator_tag, std::pair<Position, Position>>
    {

        friend Migration;

        typedef std::pair<Position, Position> item_t;

    private:
        Migration* migration;
        Position p;

        bool inc_p()
        {
            p.second++;
            if (p.second == NR_SEATS_IN_DPU) {
                p.first++;
                p.second = 0;
                if (p.first == NR_DPUS) {
                    p = {-1, INVALID_SEAT_ID};
                    return false;
                }
            }
            return true;
        }

        void locate_next()
        {
            while (migration->plan[p.first][p.second].first == -1)
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
                p = {0, 0};
                locate_next();
            } else
                p = {-1, INVALID_SEAT_ID};
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
            return {migration->plan[p.first][p.second], p};
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
    Position plan[NR_DPUS][NR_SEATS_IN_DPU];
    seat_set_t used_seats[NR_DPUS];
    seat_set_t freeing_seats[NR_DPUS];
    int nr_used_seats[NR_DPUS];
    int nr_freeing_seats[NR_DPUS];

public:
    Migration(HostTree* tree);
    int get_num_queries_for_source(BatchCtx& batch_ctx, dpu_id_t dpu, seat_id_t seat_id);
    Position get_source(dpu_id_t dpu, seat_id_t seat_id);
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
    seat_id_t find_available_seat(dpu_id_t dpu);
    void do_migrate_subtree(dpu_id_t from_dpu, seat_id_t from, dpu_id_t to_dpu, seat_id_t to);
    void migrate_subtree(dpu_id_t from_dpu, seat_id_t from, dpu_id_t to_dpu, seat_id_t to);
    bool migrate_subtree_to_balance_load(dpu_id_t from_dpu, dpu_id_t to_dpu, int diff, int nkeys_for_trees[NR_DPUS][NR_SEATS_IN_DPU]);
    void migrate_subtrees(dpu_id_t from_dpu, dpu_id_t to_dpu, int n);
    bool plan_merge(Position left, Position right, merge_info_t* merge_list);
};

#endif /* __MIGRATION_HPP__ */