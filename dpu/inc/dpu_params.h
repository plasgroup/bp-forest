#pragma once


#ifndef MRAM_FOR_TREE
#define MRAM_FOR_TREE (27u * 1024u * 1024u)
#endif


#ifdef USE_RBTREE
#define SIZEOF_NODE (24u)
#endif

#ifndef SIZEOF_NODE
#define SIZEOF_NODE (256u)
#endif
