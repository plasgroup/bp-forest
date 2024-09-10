#include "rbtree.h"

#include "allocator.h"
#include "bit_ops_macro.h"
#include "workload_types.h"

#include <attributes.h>
#include <mram.h>

#include <stdbool.h>
#include <stdio.h>


#define AssignChild(NODE_HEADER, IS_CHILD_RIGHT, NEW_CHILD)          \
    ((IS_CHILD_RIGHT) ? (NodePtr)((NODE_HEADER).right = (NEW_CHILD)) \
                      : (NodePtr)((NODE_HEADER).left = (NEW_CHILD)))
#define GetChild(NODE_HEADER, IS_CHILD_RIGHT) \
    ((IS_CHILD_RIGHT) ? ((NODE_HEADER).right) \
                      : ((NODE_HEADER).left))


#define MAX_HEIGHT (FLOOR_LOG2_UINT32((uint32_t)(MAX_NUM_NODES_IN_DPU)) * 2)

NodePtr root;

__host uint32_t num_kvpairs;


void init_Tree(void)
{
    Allocator_reset();
    root = NODE_NULLPTR;
}

NodePtr next_node(NodePtr node)
{
    NodeHeader node_header;
    mram_read(&Deref(node).header, &node_header, sizeof(NodeHeader));

    if (node_header.right == NODE_NULLPTR) {
        for (;;) {
            const NodePtr parent = node_header.parent;
            if (parent == NODE_NULLPTR) {
                return NODE_NULLPTR;
            }

            NodeHeader parent_header;
            mram_read(&Deref(parent).header, &parent_header, sizeof(NodeHeader));
            if (parent_header.right != node) {
                return parent;
            }

            node = parent;
            node_header = parent_header;
        }
    } else {
        node = node_header.right;

        for (;;) {
            mram_read(&Deref(node).header, &node_header, sizeof(NodeHeader));
            if (node_header.left == NODE_NULLPTR) {
                return node;
            } else {
                node = node_header.left;
            }
        }
    }
}

void TreeInsert(key_uint64_t key, __dma_aligned value_uint64_t value)
{
    if (root == NODE_NULLPTR) {
        __dma_aligned Node new_child_data;
        new_child_data.header.left = NODE_NULLPTR;
        new_child_data.header.right = NODE_NULLPTR;
        new_child_data.header.parent = NODE_NULLPTR;
        new_child_data.header.color = Black;
        new_child_data.key = key;
        new_child_data.value = value;

        const NodePtr new_child = Allocate_node();
        mram_write(&new_child_data, &Deref(new_child), sizeof(Node));

        root = new_child;
        num_kvpairs = 1;
        return;
    }

    NodePtr node = root;
    __dma_aligned NodeHeaderAndKey node_data;
    bool is_child_right;
    for (;;) {
        mram_read(&Deref(node), &node_data, sizeof(NodeHeaderAndKey));
        if (key < node_data.key) {
            if (node_data.header.left != NODE_NULLPTR) {
                node = node_data.header.left;
            } else {
                is_child_right = false;
                break;
            }
        } else if (node_data.key < key) {
            if (node_data.header.right != NODE_NULLPTR) {
                node = node_data.header.right;
            } else {
                is_child_right = true;
                break;
            }
        } else {
            mram_write(&value, &Deref(node).value, sizeof(value_uint64_t));
            return;
        }
    }

    const NodePtr new_child = Allocate_node();
    __dma_aligned Node new_child_data;
    new_child_data.key = key;
    new_child_data.value = value;

    num_kvpairs++;

    if (node_data.header.color == Black) {
        AssignChild(node_data.header, is_child_right, new_child);
        mram_write(&node_data.header, &Deref(node).header, sizeof(NodeHeader));

        new_child_data.header.left = NODE_NULLPTR;
        new_child_data.header.right = NODE_NULLPTR;
        new_child_data.header.parent = node;
        new_child_data.header.color = Red;
        mram_write(&new_child_data, &Deref(new_child), sizeof(Node));

        return;
    }

    NodePtr child;
    __dma_aligned NodeHeader child_header;
    __dma_aligned NodeHeader grandchildren_header[2];
    {
        const NodePtr black_node = node_data.header.parent;
        __dma_aligned NodeHeader black_node_header;
        mram_read(&Deref(black_node).header, &black_node_header, sizeof(NodeHeader));

        const bool is_node_right = (black_node_header.left != node);

        const NodePtr sibling = GetChild(black_node_header, !is_node_right);
        if (sibling == NODE_NULLPTR) {
            const NodePtr grandgrandparent = black_node_header.parent;
            NodePtr new_black_node;

            if (is_child_right == is_node_right) {
                AssignChild(node_data.header, is_child_right, new_child);
                AssignChild(node_data.header, !is_child_right, black_node);
                node_data.header.parent = black_node_header.parent;
                node_data.header.color = black_node_header.color;
                new_black_node = node;

                black_node_header.left = NODE_NULLPTR;
                black_node_header.right = NODE_NULLPTR;
                black_node_header.parent = node;
                black_node_header.color = Red;

                new_child_data.header = black_node_header;

            } else {
                AssignChild(new_child_data.header, !is_child_right, node);
                AssignChild(new_child_data.header, is_child_right, black_node);
                new_child_data.header.parent = black_node_header.parent;
                new_child_data.header.color = black_node_header.color;
                new_black_node = new_child;

                black_node_header.left = NODE_NULLPTR;
                black_node_header.right = NODE_NULLPTR;
                black_node_header.parent = new_child;
                black_node_header.color = Red;

                node_data.header = black_node_header;
            }

            if (grandgrandparent == NODE_NULLPTR) {
                root = new_black_node;
            } else {
                __dma_aligned NodeHeader grandgrandparent_header;
                mram_read(&Deref(grandgrandparent).header, &grandgrandparent_header, sizeof(NodeHeader));
                AssignChild(grandgrandparent_header, grandgrandparent_header.left != black_node, new_black_node);
                mram_write(&grandgrandparent_header, &Deref(grandgrandparent).header, sizeof(NodeHeader));
            }

            mram_write(&node_data.header, &Deref(node).header, sizeof(NodeHeader));
            mram_write(&black_node_header, &Deref(black_node).header, sizeof(NodeHeader));
            mram_write(&new_child_data, &Deref(new_child), sizeof(Node));
            return;

        } else {
            __dma_aligned NodeHeader sibling_header;
            sibling_header.left = NODE_NULLPTR;
            sibling_header.right = NODE_NULLPTR;
            sibling_header.parent = black_node;
            sibling_header.color = Black;

            AssignChild(node_data.header, is_child_right, new_child);
            node_data.header.color = Black;

            new_child_data.header.left = NODE_NULLPTR;
            new_child_data.header.right = NODE_NULLPTR;
            new_child_data.header.parent = node;
            new_child_data.header.color = Red;

            mram_write(&new_child_data, &Deref(new_child), sizeof(Node));

            child = black_node;
            child_header = black_node_header;
            grandchildren_header[is_node_right] = node_data.header;
            grandchildren_header[!is_node_right] = sibling_header;
        }
    }

    for (;;) {
        // at this point,
        //     child_header: not modified
        //     grandchildren_header: both modified but not written back

        const NodePtr node = child_header.parent;
        if (node == NODE_NULLPTR) {
            mram_write(&grandchildren_header[0], &Deref(child_header.left).header, sizeof(NodeHeader));
            mram_write(&grandchildren_header[1], &Deref(child_header.right).header, sizeof(NodeHeader));
            return;
        }

        __dma_aligned NodeHeader node_header;
        mram_read(&Deref(node).header, &node_header, sizeof(NodeHeader));

        if (node_header.color == Black) {
            child_header.color = Red;
            mram_write(&child_header, &Deref(child).header, sizeof(NodeHeader));
            mram_write(&grandchildren_header[0], &Deref(child_header.left).header, sizeof(NodeHeader));
            mram_write(&grandchildren_header[1], &Deref(child_header.right).header, sizeof(NodeHeader));
            return;
        }

        const NodePtr black_node = node_header.parent;
        __dma_aligned NodeHeader black_node_header;
        mram_read(&Deref(black_node).header, &black_node_header, sizeof(NodeHeader));

        const bool is_node_right = (black_node_header.left != node);

        const NodePtr sibling = GetChild(black_node_header, !is_node_right);
        __dma_aligned NodeHeader sibling_header;
        mram_read(&Deref(sibling).header, &sibling_header, sizeof(NodeHeader));
        if (sibling_header.color == Black) {
            const bool is_child_right = (node_header.left != child);
            const NodePtr grandgrandparent = black_node_header.parent;
            NodePtr new_black_node;

            if (is_child_right == is_node_right) {
                node_header.parent = black_node_header.parent;
                node_header.color = black_node_header.color;
                new_black_node = node;

                const NodePtr moved = AssignChild(black_node_header, is_node_right, GetChild(node_header, !is_child_right));
                AssignChild(node_header, !is_child_right, black_node);

                NodeHeader moved_header;
                mram_read(&Deref(moved).header, &moved_header, sizeof(NodeHeader));
                moved_header.parent = black_node;
                mram_write(&moved_header, &Deref(moved).header, sizeof(NodeHeader));

                black_node_header.parent = node;
                black_node_header.color = Red;

                child_header.color = Red;

            } else {
                AssignChild(node_header, is_child_right, GetChild(child_header, !is_child_right));
                grandchildren_header[!is_child_right].parent = node;
                AssignChild(black_node_header, is_node_right, GetChild(child_header, !is_node_right));
                grandchildren_header[!is_node_right].parent = black_node;

                AssignChild(child_header, !is_child_right, node);
                AssignChild(child_header, !is_node_right, black_node);
                child_header.parent = black_node_header.parent;
                child_header.color = black_node_header.color;
                new_black_node = child;

                black_node_header.parent = child;
                black_node_header.color = Red;
                node_header.parent = child;
            }

            if (grandgrandparent == NODE_NULLPTR) {
                root = new_black_node;
            } else {
                __dma_aligned NodeHeader grandgrandparent_header;
                mram_read(&Deref(grandgrandparent).header, &grandgrandparent_header, sizeof(NodeHeader));
                AssignChild(grandgrandparent_header, grandgrandparent_header.left != black_node, new_black_node);
                mram_write(&grandgrandparent_header, &Deref(grandgrandparent).header, sizeof(NodeHeader));
            }

            mram_write(&black_node_header, &Deref(black_node).header, sizeof(NodeHeader));
            mram_write(&node_header, &Deref(node).header, sizeof(NodeHeader));
            mram_write(&child_header, &Deref(child).header, sizeof(NodeHeader));
            mram_write(&grandchildren_header[0], &Deref(child_header.left).header, sizeof(NodeHeader));
            mram_write(&grandchildren_header[1], &Deref(child_header.right).header, sizeof(NodeHeader));
            return;

        } else {
            mram_write(&grandchildren_header[0], &Deref(child_header.left).header, sizeof(NodeHeader));
            mram_write(&grandchildren_header[1], &Deref(child_header.right).header, sizeof(NodeHeader));

            sibling_header.color = Black;
            node_header.color = Black;
            child_header.color = Red;

            mram_write(&child_header, &Deref(child).header, sizeof(NodeHeader));

            child = black_node;
            child_header = black_node_header;
            grandchildren_header[is_node_right] = node_header;
            grandchildren_header[!is_node_right] = sibling_header;
        }
    }
}

value_uint64_t TreeGet(key_uint64_t key)
{
    if (root == NODE_NULLPTR) {
        return 0;
    }

    NodePtr node = root;
    __dma_aligned Node node_data;
    for (;;) {
        mram_read(&Deref(node), &node_data, sizeof(Node));
        if (key < node_data.key) {
            if (node_data.header.left != NODE_NULLPTR) {
                node = node_data.header.left;
            } else {
                return 0;
            }
        } else if (node_data.key < key) {
            if (node_data.header.right != NODE_NULLPTR) {
                node = node_data.header.right;
            } else {
                return 0;
            }
        } else {
            return node_data.value;
        }
    }
}


#ifdef DEBUG_ON
#define QUEUE_SIZE (MAX_NUM_NODES_IN_DPU / 2)
#include "node_queue.h"

void showNode(NodePtr cur, int nodeNo)
{  // show single node
    printf("[Node No. %d]  @0x%x\n", nodeNo, cur);
    printf("0. color: %s\n", (Deref(cur).header.color == Black ? "Black" : "Red"));
    printf("1. parent: 0x%x\n", Deref(cur).header.parent);
    printf("2. key: %lx\n", Deref(cur).key);
    printf("3. value: %lx\n", Deref(cur).value);
    printf("4. children: [0x%x 0x%x]\n", Deref(cur).header.left, Deref(cur).header.right);
    printf("\n");
}

void TreePrintKeys()
{
    NodePtr node = root;
    if (root != NODE_NULLPTR) {
        while (Deref(node).header.left != NODE_NULLPTR) {
            node = Deref(node).header.left;
        }
        for (; node != NODE_NULLPTR; node = next_node(node)) {
            printf("%lx ", Deref(node).key);
        }
    }
    printf("\n");
}

bool TreeCheckStructure()
{
    if (root == NODE_NULLPTR) {
        return true;
    }

    bool success = true;
    initQueue();
    enqueue(root);
    for (int nodeNo = 0; !isQueueEmpty(); nodeNo++) {
        const NodePtr cur = dequeue();

        if (Deref(cur).header.color == Red) {
            if (Deref(cur).header.parent == NODE_NULLPTR) {
                success = false;
                printf("Node[%d]: 0x%x is a red node but doesn't have a parent.\n", nodeNo, cur);
            } else if (Deref(Deref(cur).header.parent).header.color == Red) {
                success = false;
                printf("Node[%d]: 0x%x and its parent 0x%x are both red nodes.\n", nodeNo, cur, Deref(cur).header.parent);
            }
        }

        const NodePtr left_child = Deref(cur).header.left, right_child = Deref(cur).header.right;
        if (left_child != NODE_NULLPTR) {
            if (Deref(left_child).header.parent != cur) {
                success = false;
                printf("Node[%d]: 0x%x->left_child->parent == 0x%x != 0x%x\n", nodeNo, cur, Deref(left_child).header.parent, cur);
            }
            if (Deref(left_child).key >= Deref(cur).key) {
                success = false;
                printf("Node[%d]: 0x%x->left_child->key >= 0x%x->key\n", nodeNo, cur, cur);
            }
            enqueue(left_child);
        }
        if (right_child != NODE_NULLPTR) {
            if (Deref(right_child).header.parent != cur) {
                success = false;
                printf("Node[%d]: 0x%x->right_child->parent == %u != %u\n", nodeNo, cur, Deref(right_child).header.parent, cur);
            }
            if (Deref(right_child).key <= Deref(cur).key) {
                success = false;
                printf("Node[%d]: 0x%x->right_child->key <= 0x%x->key\n", nodeNo, cur, cur);
            }
            enqueue(right_child);
        }
    }
    return success;
}

void TreePrintRoot()
{
    if (root == NODE_NULLPTR) {
        printf("rootNode is null\n");
    } else {
        printf("rootNode\n");
        showNode(root, 0);
    }
}

void TreePrintAll()
{  // show all node (BFS)
    if (root == NODE_NULLPTR) {
        return;
    }

    int nodeNo = 0;
    initQueue();
    enqueue(root);
    while (!isQueueEmpty()) {
        NodePtr cur = dequeue();
        showNode(cur, nodeNo);
        nodeNo++;
        if (Deref(cur).header.left != NODE_NULLPTR) {
            enqueue(Deref(cur).header.left);
        }
        if (Deref(cur).header.right != NODE_NULLPTR) {
            enqueue(Deref(cur).header.right);
        }
    }
}

#endif /* DEBUG_ON */
