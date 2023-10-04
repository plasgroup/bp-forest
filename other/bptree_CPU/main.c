#include <stdio.h>
#include "bplustree.h"
#include "../../common/common.h"
#include <time.h>
clock_t clock(void);

#define INSERT_TIMES (2700)
#define GET_TIMES (2700)

int main() {
    clock_t start_clock, end_clock;
    clock_t nb_cycles_get, nb_cycles_insert;
    init_BPTree();
    char* value = "abc";
    void* getval;
    start_clock = clock();
    for(int i = 1; i < INSERT_TIMES; i+= 1){
        BPTreeInsert(i,value);
    }
    end_clock = clock();
    nb_cycles_insert = end_clock - start_clock;

    start_clock = clock();
    for(int i = 1; i < GET_TIMES; i+= 1){
        getval = BPTreeGet(i);
        printf("%s\n", (char*)getval);
    }
    end_clock = clock();
    nb_cycles_get = end_clock - start_clock;

    // BPTreePrintRoot();
    // BPTreePrintLeaves();
    printf("total num of nodes = %d\n", BPTree_GetNumOfNodes());
    printf("nb_cycles_insert = %ld\n", nb_cycles_insert);
    printf("cycles/insert = %f\n", (double)nb_cycles_insert/INSERT_TIMES);
    printf("insertion time[ms] = %f\n", (double)1000*nb_cycles_insert / CLOCKS_PER_SEC);
    printf("nb_cycles_get = %ld\n", nb_cycles_get);
    printf("cycles/get = %f\n", (double)nb_cycles_get/GET_TIMES);
    printf("get time[ms] = %f\n", (double)1000*nb_cycles_get / CLOCKS_PER_SEC);
    return 0;
}