
/* move this section to config.h */

#define NR_SEATS_IN_DPU 20
#define MAX_NODES_IN_SEAT (10 * 1000)

/* move this section to config.h end */


typedef int seat_id_t;

#define INVALID_SEAT_ID (-1)

typedef char bitmap_word_t;
#deinfe LOG_BITS_IN_BMPWD     5
#define BITS_IN_BMPWD (1 << LOG_BITS_IN_BMPWD)

struct Seat {
  int in_use;
  __mram Node* storage;
  char bitmap[(MAX_NODES_IN_SEAT + BITS_IN_BMPWD - 1) >> LOG_BITS_IN_BMPWD];
  int next_alloc;
};

#define ROOT_NODE_INDEX 0

extern struct Seat cabin[NR_SEATS_IN_DPU];


