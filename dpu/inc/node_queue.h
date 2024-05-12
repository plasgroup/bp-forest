#pragma once

#include "node_ptr.h"

#include <attributes.h>

#include <stdbool.h>
#include <stdio.h>


#ifndef QUEUE_SIZE
#error define QUEUE_SIZE
#else

typedef struct {  // queue for showing all nodes by BFS
    int tail;
    int head;
    NodePtr ptrs[QUEUE_SIZE];
} Queue;

__mram_noinit Queue queue;

inline void initQueue()
{
    queue.head = 0;
    queue.tail = 0;
    // printf("queue is initialized\n");
}

inline void enqueue(NodePtr input)
{
    if ((queue.tail + 1) % (int)QUEUE_SIZE == queue.head) {
        printf("queue is full\n");
        return;
    }
    queue.ptrs[queue.tail] = input;
    queue.tail = (queue.tail + 1) % (int)QUEUE_SIZE;
    // printf("%p is enqueued\n",input);
}

inline bool isQueueEmpty()
{
    return queue.tail == queue.head;
}

inline NodePtr dequeue()
{
    if (isQueueEmpty()) {
        printf("queue is empty\n");
        return NODE_NULLPTR;
    }
    NodePtr ret = queue.ptrs[queue.head];
    queue.head = (queue.head + 1) % (int)QUEUE_SIZE;
    // printf("%p is dequeued\n",ret);
    return ret;
}

#endif /* !QUEUE_SIZE */
