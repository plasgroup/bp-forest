#pragma once

#include "bitmap_fwd.h"
#include "tree_impl.h"


typedef union {
    TreeWorkspace tree;
    BitmapInitWorkspace bitmap;
} Workspace;

extern Workspace workspace;
