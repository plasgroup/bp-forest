#pragma once

/* To add a new index structure:
 * 1. Create dpu/inc/new_tree.h  (define Node and TreeWorkspace)
 * 2. Create dpu/src/new_tree.c  (implement the 8 task_* functions from tree.h)
 * 3. Change the #include below to "new_tree.h"
 * 4. Update dpu/CMakeLists.txt to compile the new source file
 */
#include "bplustree.h"
