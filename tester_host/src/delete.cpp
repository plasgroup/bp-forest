#include "assert.hpp"
#include "common.h"
#include "extendable_buffer.hpp"
#include "log_buffer.hpp"
#include "pimtree_query.hpp"
#include "workload_buffer.hpp"

extern "C" {
#include <dpu.h>
#include <dpu_types.h>
}

#include <cmdline.h>

#include <cstdint>
#include <fstream>
#include <memory>
#include <string>


struct DPUHandler {
    dpu_set_t all_dpu;
    struct dpu_program_t* dpu_program;
    struct dpu_symbol_t comm_buffer;

    DPUHandler()
    {
        DPU_ASSERT(dpu_alloc(DPU_ALLOCATE_ALL,
#ifdef UPMEM_SIMULATOR
            "backend=simulator,chipId=0x42",
#else
            "",
#endif
            &all_dpu));

#ifdef UPMEM_TRACE
        DPU_ASSERT(dpu_load(all_dpu, DPU_BINARY_PATH, &dpu_program));
#else
        extern dpu_incbin_t dpu_binary;
        DPU_ASSERT(dpu_load_from_incbin(all_dpu, &dpu_binary, &dpu_program));
#endif

        DPU_ASSERT(dpu_get_symbol(dpu_program, DPU_MRAM_HEAP_POINTER_NAME, &comm_buffer));
    }

    ~DPUHandler()
    {
        DPU_ASSERT(dpu_free(all_dpu));
    }
};

struct CMDOpt {
    CMDOpt(int argc, char* argv[])
    {
        cmdline::parser a;
        a.add<std::string>("dump-params", 0, "file path to output parameters");
        a.add<std::string>("init_file", 'i', "file path to PIM-Tree init file", true);
        a.add<std::string>("workload_file", 'w', "file path to PIM-Tree workload file", true);
        a.add<uint16_t>("batch_size", 'b', "num of queries per batch", true);
        a.add<unsigned>("num_batches", 'n', "maximum num of batches", true);
        a.parse_check(argc, argv);

        dump_param_file = a.get<std::string>("dump-params");
        init_file = a.get<std::string>("init_file");
        workload_file = a.get<std::string>("workload_file");
        batch_size = a.get<uint16_t>("batch_size");
        nr_batches = a.get<unsigned>("num_batches");
    }

    std::string dump_param_file;
    std::string init_file;
    std::string workload_file;
    uint16_t batch_size;
    unsigned nr_batches;
};

void dump_param(const CMDOpt& opt, const DPUHandler& dpu_hdr)
{
    dpu_set_t all_dpu = dpu_hdr.all_dpu, first_dpu, tmp_dpu;
    DPU_FOREACH(all_dpu, tmp_dpu)
    {
        first_dpu = tmp_dpu;
        break;
    }

    uint64_t dpu_param_dump_size;
    DPU_ASSERT(dpu_copy_from(first_dpu, "ParamDumpSize", 0, &dpu_param_dump_size, sizeof(uint64_t)));
    std::unique_ptr<char[]> dpu_param_dump{new char[dpu_param_dump_size]};
    DPU_ASSERT(dpu_copy_from(first_dpu, "ParamDump", 0, &dpu_param_dump[0], dpu_param_dump_size));

    std::ofstream dump_param_file(opt.dump_param_file, std::ios_base::app);
    if (!dump_param_file) {
        std::cerr << "cannot open file: " << opt.dump_param_file << std::endl;
        std::quick_exit(1);
    }
    dump_param_file << dpu_param_dump.get()
                    << "init_file: " << opt.init_file
                    << "\nworkload_file: " << opt.workload_file
                    << "\nbatch_size: " << opt.batch_size
                    << "\nnr_batches: " << opt.nr_batches << std::endl;
}

struct InitHeader {
    uint32_t task_no = TASK_INIT;
    uint32_t nr_pairs;
};
struct ConstructHotHeader {
    uint32_t task_no = TASK_MOVE_HOT;
    uint32_t nr_pairs;
};
struct NopHeader {
    uint32_t task_no = TASK_NONE;
    uint32_t pad = 0;
};
struct DeleteHeader {
    uint32_t task_no = TASK_DELETE;
    uint16_t nr_cold_qrys;
    uint16_t nr_hot_qrys = 0;
};


int main(int argc, char* argv[])
{
    CMDOpt opt{argc, argv};
    DPUHandler dpu_hdr;

    dump_param(opt, dpu_hdr);

    {
        const pimtree_queries init_qrys = make_pimtree_queries(opt.init_file);
        ExtendableBuffer<KVPair> init_pairs{init_qrys.length};
        for (size_t i = 0; i < init_qrys.length; i++) {
            ASSERT(init_qrys.ops[i].type == insert_t);
            init_pairs[i] = {key_int64_to_uint64(init_qrys.ops[i].tsk.i.key), key_int64_to_uint64(init_qrys.ops[i].tsk.i.value)};
        }

        const InitHeader init_header{TASK_INIT, static_cast<uint32_t>(init_qrys.length)};
        DPU_ASSERT(dpu_broadcast_to_symbol(dpu_hdr.all_dpu, dpu_hdr.comm_buffer, 0, &init_header, 8, DPU_XFER_DEFAULT));
        DPU_ASSERT(dpu_broadcast_to_symbol(dpu_hdr.all_dpu, dpu_hdr.comm_buffer, 8, &init_pairs[0], init_qrys.length * sizeof(KVPair), DPU_XFER_DEFAULT));
        DPU_ASSERT(dpu_launch(dpu_hdr.all_dpu, DPU_SYNCHRONOUS));

        const ConstructHotHeader hot_header{TASK_MOVE_HOT, static_cast<uint32_t>(init_qrys.length)};
        DPU_ASSERT(dpu_broadcast_to_symbol(dpu_hdr.all_dpu, dpu_hdr.comm_buffer, 0, &hot_header, 8, DPU_XFER_DEFAULT));
        DPU_ASSERT(dpu_broadcast_to_symbol(dpu_hdr.all_dpu, dpu_hdr.comm_buffer, 8, &init_pairs[0], init_qrys.length * sizeof(KVPair), DPU_XFER_DEFAULT));
        DPU_ASSERT(dpu_launch(dpu_hdr.all_dpu, DPU_SYNCHRONOUS));
    }

    {
        const pimtree_queries workload_qrys = make_pimtree_queries(opt.workload_file);
        std::vector<key_uint64_t> workload_keys;
        workload_keys.reserve(workload_qrys.length);
        for (size_t i = 0; i < workload_qrys.length; i++) {
            workload_keys.push_back(key_int64_to_uint64(workload_qrys.ops[i].tsk.r.key));
        }
        WorkloadBuffer buf{std::move(workload_keys)};

        for (unsigned idx_batch = 0; idx_batch < opt.nr_batches; idx_batch++) {
            const auto [qrys, nr_qrys] = buf.take(opt.batch_size);
            if (nr_qrys == 0) {
                break;
            }

            const DeleteHeader insert_header{TASK_DELETE, static_cast<uint16_t>(nr_qrys), static_cast<uint16_t>(nr_qrys)};

            DPU_ASSERT(dpu_broadcast_to_symbol(dpu_hdr.all_dpu, dpu_hdr.comm_buffer, 0, &insert_header, 8, DPU_XFER_DEFAULT));
            DPU_ASSERT(dpu_broadcast_to_symbol(dpu_hdr.all_dpu, dpu_hdr.comm_buffer, 8, qrys, nr_qrys * sizeof(key_uint64_t), DPU_XFER_DEFAULT));
            DPU_ASSERT(dpu_broadcast_to_symbol(dpu_hdr.all_dpu, dpu_hdr.comm_buffer, static_cast<uint32_t>(8 + nr_qrys * sizeof(key_uint64_t)), qrys, nr_qrys * sizeof(key_uint64_t), DPU_XFER_DEFAULT));
            DPU_ASSERT(dpu_launch(dpu_hdr.all_dpu, DPU_SYNCHRONOUS));

#ifdef PRINT_DEBUG
            {
                LogStream stream;

                dpu_set_t dpu;
                DPU_FOREACH(dpu_hdr.all_dpu, dpu)
                {
                    DPU_ASSERT(dpu_log_read(dpu, stream.get()));
                }

                std::cout << std::move(stream).close()->get() << std::flush;
            }
#endif

            std::cout << "batch#" << idx_batch << " done" << std::endl;
        }
    }
}
