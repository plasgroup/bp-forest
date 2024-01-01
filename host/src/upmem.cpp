#include <stdlib.h>
#include <sys/time.h>
#include <assert.h>
#include "common.h"
#include "host_data_structures.hpp"
#include "node_defs.hpp"
#include "utils.hpp"

#ifndef HOST_ONLY
extern "C" {
#include <dpu.h>
#include <dpu_log.h>
}
#else /* HOST_ONLY */
#include <bitset>
#include <map>

#define EMU_MAX_DPUS 3000
typedef std::bitset<EMU_MAX_DPUS> dpu_set_t;
typedef uint32_t dpu_id_t;

#endif /* HOST_ONLY */

static dpu_set_t dpu_set;

/* buffer */
dpu_requests_t* dpu_requests;
dpu_results_t* dpu_results;
merge_info_t merge_info[NR_DPUS];
split_info_t split_result[NR_DPUS][NR_SEATS_IN_DPU];
static BPTreeNode tree_migration_buffer[MAX_NUM_NODES_IN_SEAT];

uint32_t upmem_get_nr_dpus();

#define MEASURE_XFER_BYTES
#ifdef MEASURE_XFER_BYTES
typedef struct xfer_summary {
    xfer_summary() :
        total_bytes(0),
        effective_bytes(0),
        count(0) {}
    uint64_t total_bytes;
    uint64_t effective_bytes;
    uint64_t count;
} xfer_summary_t;
std::map<std::string, xfer_summary_t> xfer_stat;

void 
accumulate_xfer_bytes(const char* symbol,
                      uint64_t xfer_bytes, uint64_t effective_bytes)
{
    std::string key = std::string(symbol);
    if (xfer_stat.find(key) == xfer_stat.end())
        xfer_stat.insert(std::make_pair(key, xfer_summary_t()));
    xfer_stat[key].total_bytes += xfer_bytes;
    xfer_stat[key].effective_bytes += effective_bytes;
    xfer_stat[key].count++;
}

void print_xfer_bytes()
{
    printf("==== XFER STATISTICS (MB) ====\n");
    printf("symbol                    count xfer-bytes    average  effective effeciency(%%) \n");
    for(auto x: xfer_stat) {
        const char* symbol = x.first.c_str();
        uint64_t total_bytes = x.second.total_bytes;
        uint64_t count = x.second.count;
        uint64_t effective_bytes = x.second.effective_bytes;
#define MB(x) (((float) (x)) / 1000 / 1000)
        printf("%-25s %5lu %10.3f %10.3f %10.3f",
               symbol, count,
               MB(total_bytes),
               MB(total_bytes / count),
               MB(effective_bytes));
        if (total_bytes > 0)
            printf(" %5.3f\n", ((float) effective_bytes) / total_bytes);
        else
            printf("  0.000\n");
#undef MB
    }
}
#else /* MEASURE_XFER_BYTES */
#define accumulate_xfer_bytes(s,b)
#endif /* MEASURE_XFER_BYTES */

#ifdef HOST_ONLY

struct Emulation {
    void* get_addr_of_symbol(const char* symbol)
    {
#define MRAM_SYMBOL(S) do {          \
        if (strcmp(symbol, #S) == 0) \
            return &mram.S;          \
} while (0)

        MRAM_SYMBOL(task_no);
        MRAM_SYMBOL(end_idx);
        MRAM_SYMBOL(request_buffer);
        MRAM_SYMBOL(merge_info);
        MRAM_SYMBOL(result);
        MRAM_SYMBOL(split_result);
        MRAM_SYMBOL(num_kvpairs_in_seat);
        MRAM_SYMBOL(tree_transfer_num);
        MRAM_SYMBOL(tree_transfer_buffer);

#undef MRAM_SYMBOL

        abort();
        return NULL;
    }

    struct MRAM {
        uint64_t task_no;
        int end_idx[NR_SEATS_IN_DPU];
        each_request_t request_buffer[MAX_REQ_NUM_IN_A_DPU];
        merge_info_t merge_info;
        each_result_t result[MAX_REQ_NUM_IN_A_DPU];
        split_info_t split_result[NR_SEATS_IN_DPU];
        int num_kvpairs_in_seat[NR_SEATS_IN_DPU];
        uint64_t tree_transfer_num;
        KVPair tree_transfer_buffer[MAX_NUM_NODES_IN_SEAT * MAX_CHILD];
    } mram;

    std::map<key_int64_t, value_ptr_t> subtree[NR_SEATS_IN_DPU];
    bool in_use[NR_SEATS_IN_DPU];

    void execute()
    {
        switch (TASK_GET_ID(mram.task_no)) {
        case TASK_INIT:
            task_init(TASK_GET_OPERAND(mram.task_no));
            break;
        case TASK_INSERT:
            task_insert();
#ifdef DEBUG_ON
            task_get();
#endif /* DEBUG_ON */
            break;
        case TASK_GET:
            task_get();
            break;
        case TASK_FROM:
            task_from(TASK_GET_OPERAND(mram.task_no));
            break;
        case TASK_TO:
            task_to(TASK_GET_OPERAND(mram.task_no));
            break;
        default:
            abort();
        }
    }

private:
    /* counterpert of Cabin_allocate_seat */
    seat_id_t allocate_seat(seat_id_t seat_id)
    {
        if (seat_id == INVALID_SEAT_ID) {
            for (seat_id_t i = 0; i < NR_SEATS_IN_DPU; i++)
                if (!in_use[i]) {
                    in_use[i] = true;
                    return i;
                }
            abort();
            return INVALID_SEAT_ID;
        } else {
            assert(!in_use[seat_id]);
            in_use[seat_id] = true;
            return seat_id;
        }
    }

    /* counterpart of Cabin_release_seat */
    void release_seat(seat_id_t seat_id)
    {
        assert(in_use[seat_id]);
        in_use[seat_id] = false;
        mram.num_kvpairs_in_seat[seat_id] = 0;
    }

    int serialize(seat_id_t seat_id, KVPair buf[])
    {
        assert(in_use[seat_id]);
        int n = 0;
        for (auto x: subtree[seat_id]) {
            buf[n].key = x.first;
            buf[n].value = x.second;
            n++;
        }
        return n;
    }

    void deserialize(seat_id_t seat_id, KVPair buf[], int start, int n)
    {
        assert(in_use[seat_id]);
        assert(subtree[seat_id].size() == 0);
        for (int i = start; i < start + n; i++) {
            key_int64_t key = buf[i].key;
            value_ptr_t val = buf[i].value;
            assert(subtree[seat_id].find(key) == subtree[seat_id].end());
            subtree[seat_id].insert(std::make_pair(key, val));
        }
    }

    void split_tree(KVPair buf[], int n, split_info_t result[])
    {
        int num_trees = (n + NR_ELEMS_AFTER_SPLIT - 1) / NR_ELEMS_AFTER_SPLIT;
        assert(num_trees <= MAX_NUM_SPLIT);
        for (int i = 0; i < num_trees; i++) {
            int start = n * i / num_trees;
            int end = n * (i + 1) / num_trees;
            seat_id_t seat_id = allocate_seat(INVALID_SEAT_ID);
            deserialize(seat_id, buf, start, end - start);
            result->num_elems[i] = end - start;
            result->new_tree_index[i] = seat_id;
        }
        for (int i = 0; i < num_trees; i++) {
            int end = n * (i + 1) / num_trees - 1;
            result->split_key[i] = buf[end].key;
        }
        result->num_split = num_trees;
    }

    void split()
    {
        for (int i = 0; i < NR_SEATS_IN_DPU; i++) {
            assert(subtree[i].size() == mram.num_kvpairs_in_seat[i]);
            assert(in_use[i] || mram.num_kvpairs_in_seat[i] == 0);
            if (in_use[i]) {
                int n = mram.num_kvpairs_in_seat[i];
                if (n > SPLIT_THRESHOLD) {
                    serialize(i, mram.tree_transfer_buffer);
                    release_seat(i);
                    split_tree(mram.tree_transfer_buffer, n, &mram.split_result[i]);
                }
            }
        }
    }

    void task_init(int nr_init_trees)
    {
        for (int i = 0; i < NR_SEATS_IN_DPU; i++) {
            mram.num_kvpairs_in_seat[i] = 0;
            in_use[i] = false;
        }
        for (int i = 0; i < nr_init_trees; i++)
            allocate_seat(i);
    }

    void task_insert()
    {
        /* sanity check */
        assert(mram.end_idx[0] >= 0);
        for (int i = 1; i < NR_SEATS_IN_DPU; i++)
            assert(mram.end_idx[i - 1] <= mram.end_idx[i]);

        for (int i = 0, j = 0; i < NR_SEATS_IN_DPU; i++)
            for (; j < mram.end_idx[i]; j++) {
                key_int64_t key = mram.request_buffer[j].key;
                value_ptr_t val = mram.request_buffer[j].write_val_ptr;
                subtree[i].insert(std::make_pair(key, val));
            }
    }

    void task_get()
    {
        /* sanity check */
        assert(mram.end_idx[0] >= 0);
        for (int i = 1; i < NR_SEATS_IN_DPU; i++)
            assert(mram.end_idx[i - 1] <= mram.end_idx[i]);

        for (int i = 0, j = 0; i < NR_SEATS_IN_DPU; i++)
            for (; j < mram.end_idx[i]; j++) {
                key_int64_t key = mram.request_buffer[j].key;
                auto it = subtree[i].lower_bound(key);
                if (it != subtree[i].end() && it->first == key)
                    mram.result[j].get_result = subtree[i].at(key);
                else
                    mram.result[j].get_result = 0;
            }
    }

    void task_from(seat_id_t seat_id)
    {
        int n = 0;
        for (auto x: subtree[seat_id]) {
            mram.tree_transfer_buffer[n].key = x.first;
            mram.tree_transfer_buffer[n].value = x.second;
            n++;
        }
        mram.tree_transfer_num = n;
        subtree[seat_id].clear();
    }

    void task_to(seat_id_t seat_id)
    {
        assert(subtree[seat_id].size() == 0);
        for (int i = 0; i < mram.tree_transfer_num; i++) {
            key_int64_t key = mram.tree_transfer_buffer[i].key;
            value_ptr_t val = mram.tree_transfer_buffer[i].value;
            subtree[seat_id].insert(std::make_pair(key, val));
        }
    }

} emu[EMU_MAX_DPUS];
#endif /* HOST_ONLY */

static const char* task_name(uint64_t task)
{
    switch (TASK_GET_ID(task)) {
    case TASK_INIT:   return "INIT";
    case TASK_GET:    return "GET";
    case TASK_INSERT: return "INSERT";
    case TASK_DELETE: return "DELETE";
    case TASK_FROM:   return "FROM";
    case TASK_TO:     return "TO";
    case TASK_MERGE:   return "MERGE";
    default: return "unknown-task";
    }
}

//
// Low level DPU ACCESS
//

#ifdef HOST_ONLY
static uint32_t
nr_dpus_in_set(dpu_set_t set)
{
    return set.count();
}

static void
select_dpu(dpu_set_t* dst, uint32_t index)
{
    dpu_set_t mask;
    mask[index] = true;
    *dst = dpu_set & mask;
}

static void
xfer_foreach(dpu_set_t set, const char* symbol, size_t size,
                const void* array, size_t elmsize, bool to_dpu)
{
    uintptr_t addr = (uintptr_t) array;
    for (int i = 0; i < EMU_MAX_DPUS; i++)
        if (set[i]) {
            void* mram_addr = emu[i].get_addr_of_symbol(symbol);
            if (to_dpu)
                memcpy(mram_addr, (void*) addr, size);
            else
                memcpy((void*) addr, mram_addr, size);
            addr += elmsize;
        }
}

static void
broadcast(dpu_set_t set, const char* symbol, const void* addr, size_t size)
{
    for (int i = 0; i < EMU_MAX_DPUS; i++)
        if (set[i]) {
            void* mram_addr = emu[i].get_addr_of_symbol(symbol);
            memcpy(mram_addr, addr, size);
        }
}

static void
execute(dpu_set_t set)
{
    for (int i = 0; i < EMU_MAX_DPUS; i++)
        if (set[i])
            emu[i].execute();
}

#else /* HOST_ONLY */
static uint32_t
nr_dpus_in_set(dpu_set_t set)
{
    uint32_t nr_dpus;
    DPU_ASSERT(dpu_get_nr_dpus(dpu_set, &nr_dpus));
    return nr_dpus;
}

static void
select_dpu(dpu_set_t* dst, uint32_t index)
{
    uint32_t each_dpu;
    DPU_FOREACH(dpu_set, *dst, each_dpu)
        if (index == each_dpu)
            return;
    abort();
}

static void
xfer_foreach(dpu_set_t set, const char* symbol, size_t size,
                const void* array, size_t elmsize, bool to_dpu)
{
    dpu_set_t dpu;
    dpu_xfer_t dir = to_dpu ? DPU_XFER_TO_DPU : DPU_XFER_FROM_DPU;

    uintptr_t addr = (uintptr_t) array;
    DPU_FOREACH(dpu_set, dpu) {
        DPU_ASSERT(dpu_prepare_xfer(dpu, (void*) addr));
        addr += elmsize;
    }
    DPU_ASSERT(dpu_push_xfer(
        dpu_set, dir, symbol, 0, size, DPU_XFER_DEFAULT));
}

static void
broadcast(dpu_set_t set, const char* symbol, const void* addr, size_t size)
{
    DPU_ASSERT(dpu_broadcast_to(
        dpu_set, symbol, 0, addr, size, DPU_XFER_DEFAULT));
}

static void
execute(dpu_set_t dpu_set)
{
    dpu_set_t dpu;
    DPU_ASSERT(dpu_launch(dpu_set, DPU_SYNCHRONOUS));
    dpu_sync(dpu_set);
#ifdef PRINT_DEBUG
    DPU_FOREACH(dpu_set, dpu)
    {
        DPU_ASSERT(dpu_log_read(dpu, stdout));
    }
#endif /* PRINT_DEBUG */
}
#endif /* HOST_ONLY */

static void
xfer_single(dpu_set_t set, const char* symbol, size_t size,
                const void* addr, bool to_dpu)
{
    assert(nr_dpus_in_set(set) == 1);
    xfer_foreach(set, symbol, size, addr, 0, to_dpu);
}

#define SEND_FOREACH(set,sym,size,ary) \
    xfer_foreach(set, sym, size, ary, sizeof((ary)[0]), true)
#define RECV_FOREACH(set,sym,size,ary) \
    xfer_foreach(set, sym, size, ary, sizeof((ary)[0]), false)
#define SEND_SINGLE(set,sym,size,addr) \
    xfer_single(set, sym, size, addr, true)
#define RECV_SINGLE(set,sym,size,addr) \
    xfer_single(set, sym, size, addr, false)

//
//  UPMEM module interface
//

void upmem_init(const char* binary, bool is_simulator)
{
    int nr_dpus_allocated;

    dpu_requests = (dpu_requests_t*)malloc((NR_DPUS) * sizeof(dpu_requests_t));
    dpu_results = (dpu_results_t*)malloc((NR_DPUS) * sizeof(dpu_results_t));

#ifdef HOST_ONLY
    for (int i = 0; i < NR_DPUS; i++)
        dpu_set[i] = true;
#else /* HOST_ONLY */
    if (is_simulator) {
        DPU_ASSERT(dpu_alloc(NR_DPUS, "backend=simulator", &dpu_set));
    } else {
        DPU_ASSERT(dpu_alloc(NR_DPUS, NULL, &dpu_set));
    }
    DPU_ASSERT(dpu_load(dpu_set, binary, NULL));
#endif /* HOST_ONLY */

#ifdef PRINT_DEBUG
    printf("Allocated %d DPU(s)\n", upmem_get_nr_dpus());
#endif /* PRINT_DEBUG */
}

void upmem_release()
{
#ifdef HOST_ONLY
    dpu_set ^= dpu_set;
#else /* HOST_ONLY */
    DPU_ASSERT(dpu_free(dpu_set));
#endif /* HOST_ONLY */
}

uint32_t upmem_get_nr_dpus()
{
    return nr_dpus_in_set(dpu_set);
}

void upmem_send_task(const uint64_t task, BatchCtx& batch_ctx,
                     float* send_time, float* exec_time)
{
    struct timeval start, end;

#ifdef PRINT_DEBUG
    printf("send task [%s]\n", task_name(task));
#endif /* PRINT_DEBUG */

    gettimeofday(&start, NULL);

    /* send task ID */
    broadcast(dpu_set, "task_no", &task, sizeof(uint64_t));

    /* send data */
    switch (task) {
    case TASK_GET:
    case TASK_INSERT:
        SEND_FOREACH(dpu_set, "end_idx",
                     sizeof(int) * NR_SEATS_IN_DPU,
                     batch_ctx.key_index);
        SEND_FOREACH(dpu_set, "request_buffer",
                     sizeof(each_request_t) * batch_ctx.send_size,
                     dpu_requests);
        break;
    case TASK_MERGE:
        SEND_FOREACH(dpu_set, "merge_info",
                     sizeof(merge_info_t), merge_info);
        break;
    }

    gettimeofday(&end, NULL);
    if (send_time != NULL)
        *send_time = time_diff(&start, &end);

#ifdef PRINT_DEBUG
    printf("send task [%s] done; %0.5f sec\n",
           task_name(task), time_diff(&start, &end));
    printf("execute task [%s]\n", task_name(task));
#endif /* PRINT_DEBUG */

    gettimeofday(&start, NULL);

    /* launch DPU */
    execute(dpu_set);

    gettimeofday(&end, NULL);
    if (exec_time != NULL)
        *exec_time = time_diff(&start, &end);

#ifdef PRINT_DEBUG
    printf("execute task [%s] done; %0.5f sec\n",
           task_name(task), time_diff(&start, &end));
#endif /* PRINT_DEBUG */
}

void upmem_receive_results(BatchCtx& batch_ctx, float* receive_time)
{
    struct timeval start, end;

    gettimeofday(&start, NULL);

#ifdef PRINT_DEBUG
    printf("send_size: %ld / buffer_size: %ld\n",
           sizeof(each_result_t) * batch_ctx.send_size,
           sizeof(each_result_t) * MAX_REQ_NUM_IN_A_DPU);
#endif /* PRINT_DEBUG */

    RECV_FOREACH(dpu_set, "result",
                 sizeof(each_result_t) * batch_ctx.send_size, dpu_results);


    gettimeofday(&end, NULL);
    if (receive_time != NULL)
        *receive_time = time_diff(&start, &end);
}

void upmem_recieve_split_info(float* receive_time)
{
    struct timeval start, end;

    gettimeofday(&start, NULL);

    RECV_FOREACH(dpu_set, "split_result",
                 sizeof(split_info_t) * NR_SEATS_IN_DPU, split_result);

    gettimeofday(&end, NULL);
    if (receive_time != NULL)
        *receive_time = time_diff(&start, &end);
}

void upmem_recieve_num_kvpairs(HostTree* host_tree, float* receive_time)
{
    struct timeval start, end;

    gettimeofday(&start, NULL);

    RECV_FOREACH(dpu_set, "num_kvpairs_in_seat",
                 sizeof(int) * NR_SEATS_IN_DPU, host_tree->num_kvpairs);

    gettimeofday(&end, NULL);
    if (receive_time != NULL)
        *receive_time = time_diff(&start, &end);
}

void upmem_send_nodes_from_dpu_to_dpu(uint32_t from_DPU, seat_id_t from_tree,
                                      uint32_t to_DPU, seat_id_t to_tree)
{
    dpu_set_t dpu;
    uint64_t n;
    uint64_t task;

    select_dpu(&dpu, from_DPU);
    task = TASK_WITH_OPERAND(TASK_FROM, from_tree);
    SEND_SINGLE(dpu, "task_no", sizeof(uint64_t), &task);
    execute(dpu);
    RECV_SINGLE(dpu, "tree_transfer_num", sizeof(uint64_t), &n);
    RECV_SINGLE(dpu, "tree_transfer_buffer",
                n * sizeof(BPTreeNode), &tree_migration_buffer);
    
    select_dpu(&dpu, to_DPU);
    task = TASK_WITH_OPERAND(TASK_TO, to_tree);
    SEND_SINGLE(dpu, "task_no", sizeof(uint64_t), &task);
    SEND_SINGLE(dpu, "tree_transfer_num", sizeof(uint64_t), &n);
    SEND_SINGLE(dpu, "tree_transfer_buffer",
                n * sizeof(BPTreeNode), &tree_migration_buffer);
    execute(dpu);
}

