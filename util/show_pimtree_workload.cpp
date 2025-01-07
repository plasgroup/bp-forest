#include "host/inc/pimtree_query.hpp"
#include "host/inc/pimtree_query.ipp"

int main(int argc, char* argv[])
{
    if (argc != 2) {
        fprintf(stderr, "Usage: %s <workload_file>\n", argv[0]);
        return 1;
    }
    pimtree_queries queries = make_pimtree_queries(argv[1]);
    show_pimtree_queries(queries);
    return 0;
}
