#pragma once

#include "pimtree_query.hpp"
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <cassert>
#include <inttypes.h>


inline pimtree_queries make_pimtree_queries(std::string filepath) {
    int fd = open(filepath.c_str(), O_RDONLY, (mode_t)0600);

    if (fd == -1) {
        perror("Error opening file for writing");
        exit(EXIT_FAILURE);
    }

    struct stat fileInfo;

    if (fstat(fd, &fileInfo) == -1) {
        perror("Error getting the file size");
        exit(EXIT_FAILURE);
    }

    if (fileInfo.st_size == 0) {
        fprintf(stderr, "Error: File is empty, nothing to do\n");
        exit(EXIT_FAILURE);
    }

    printf("File size is %ji\n", (intmax_t)fileInfo.st_size);

    const size_t st_size = static_cast<size_t>(fileInfo.st_size);
    void* map = mmap(0, st_size, PROT_READ, MAP_SHARED, fd, 0);

    if (map == MAP_FAILED) {
        close(fd);
        perror("Error mmapping the file");
        exit(EXIT_FAILURE);
    }

    assert(st_size % sizeof(operation) == 0);

    size_t n = st_size / sizeof(operation);

    return {(operation*)map, n};
}

inline void show_pimtree_queries(pimtree_queries queries) {
    for (size_t i = 0;i < queries.length;i++) {
        operation op = queries.ops[i];
        int operation_id = op.type;
        switch (op.type) {
            case empty_t: {
                fprintf(stdout, "[%zu] empty(%d)\n", i, operation_id);
                break;
            }
            case get_t: {
                fprintf(stdout, "[%zu] get(%d) %" PRId64 "\n", i, operation_id, op.tsk.g.key);
                break;
            }
            case update_t: {
                fprintf(stdout, "[%zu] update(%d) %" PRId64 " %" PRId64 "\n", i, operation_id, op.tsk.u.key, op.tsk.u.value);
                break;
            }
            case predecessor_t: {
                fprintf(stdout, "[%zu] predecessor(%d) %" PRId64 "\n", i, operation_id, op.tsk.p.key);
                break;
            }
            case scan_t: {
                fprintf(stdout, "[%zu] scan(%d) %" PRId64 " %" PRId64 "\n", i, operation_id, op.tsk.s.lkey, op.tsk.s.rkey);
                break;
            }
            case insert_t: {
                fprintf(stdout, "[%zu] insert(%d) %" PRId64 " %" PRId64 "\n", i, operation_id, op.tsk.i.key, op.tsk.i.value);
                break;
            }
            case remove_t: {
                fprintf(stdout, "[%zu] remove(%d) %" PRId64 "\n", i, operation_id, op.tsk.r.key);
                break;
            }
        }
    }
}
