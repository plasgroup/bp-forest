Initialization of Trees
===

## Target layout

### Basic policy

* Consider the layers starting from the one closest to the leaves
* In each layer, fill nodes as full as possible, from the left

### Contents of the leaf layer

* There are $`n`$ key-value pairs
* A leaf node may hold at least $`m_l`$ and at most $`M_l`$ key-value pairs

Then,

* Create $`L := \left\lceil n \over M_l \right\rceil`$ leaf nodes in total
* Writing $`n = M_l q + r`$ ($`q, r \in \mathbb{N}, r < M_l`$),
  * If $`L = 1`$, put $`n`$ key-value pairs into the only node
  * If $`L > 1 \land 0 < r < m_l \iff L > 1 \land n - (L - 1) M_l < m_l`$,
    * put $`M_l`$ key-value pairs into each of the $`L - 2`$ leftmost nodes
    * put $`M_l + r - m_l`$ key-value pairs into the second node from the right
    * put $`m_l`$ key-value pairs into the rightmost node
  * If $`L > 1 \land (r = 0 \lor r \ge m_l) \iff L > 1 \land n - (L - 1) M_l \ge m_l`$,
    * put $`M_l`$ key-value pairs into each of the $`L - 1`$ leftmost nodes
    * put $`n - (L - 1) M_l`$ key-value pairs into the rightmost node

### Contents of an internal layer

* The layer below has $`n`$ nodes
* An internal node may have at least $`m_I`$ and at most $`M_I`$ children

Then,

* Create $`I := \left\lceil n \over M_I \right\rceil`$ internal nodes in total
* Writing $`n = M_I q + r`$ ($`q, r \in \mathbb{N}, r < M_I`$),
  * **Layout "root"**: if $`I = 1`$, give the only node $`n`$ children
  * **Layout "tail-away"**: if $`I > 1 \land 0 < r < m_I \iff I > 1 \land n - (I - 1) M_I < m_I`$,
    * give $`M_I`$ children to each of the $`I - 2`$ leftmost nodes
    * give $`M_I + r - m_I`$ children to the second node from the right
    * give $`m_I`$ children to the rightmost node
  * **Layout "tidy"**: if $`I > 1 \land (r = 0 \lor r \ge m_I) \iff I > 1 \land n - (I - 1) M_I \ge m_I`$,
    * give $`M_I`$ children to each of the $`I - 1`$ leftmost nodes
    * give $`n - (I - 1) M_I`$ children to the rightmost node

## Calculations

### From a child index to a parent index

A layer of $`N_P`$ parents sits above a layer of $`N_C`$ children. How many parents $`i_P`$ satisfy the condition "all of their children are among the $`i_C`$ leftmost children of the child layer"? ($`0 \le i_C \le N_C`$)

* For layout "root",
  * if $`i_C = N_C`$, $`i_P = 1 (= N_P)`$
  * if $`0 \le i_C < N_C`$, $`i_P = 0`$
* For layout "tail-away",
  * if $`i_C = N_C`$, $`i_P = N_P`$
  * if $`N_C - m_I \le i_C < N_C`$, $`i_P = N_P - 1`$
  * if $`0 \le i_C < N_C - m_I`$, $`i_P = \left\lfloor i_C \over M_I \right\rfloor`$
* For layout "tidy",
  * if $`i_C = N_C`$, $`i_P = N_P`$
  * if $`0 \le i_C < N_C`$, $`i_P = \left\lfloor i_C \over M_I \right\rfloor`$

This can be computed as follows.

* If $`i_C + m_I < N_C`$, $`i_P = \left\lfloor i_C \over M_I \right\rfloor`$
* Otherwise, if $`i_C < N_C`$, $`i_P = N_P - 1`$
* Otherwise, $`i_P = N_P`$

### From a parent index to a child index

A layer of $`N_P`$ parents sits above a layer of $`N_C`$ children. What is the total number of children $`i_C`$ of the $`i_P`$ leftmost parents?

* For layout "root",
  * if $`i_P = 1 (= N_P)`$, $`i_C = N_C`$
  * if $`i_P = 0`$, $`i_C = 0`$
* For layout "tail-away",
  * if $`i_P = N_P`$, $`i_C = N_C`$
  * if $`i_P = N_P - 1`$, $`i_C = N_C - m_I`$
  * if $`0 \le i_P < N_P - 1`$, $`i_C = M_I i_P`$
* For layout "tidy",
  * if $`i_P = N_P`$, $`i_C = N_C`$
  * if $`0 \le i_P < N_P`$, $`i_C = M_I i_P`$

This can be computed as follows.

* If $`N_P > i_P + \begin{cases} 1 & (\text{tail-away}) \\ 0 & (\text{other layout}) \end{cases}`$, $`i_C = M_I i_P`$
* Otherwise, if $`i_P = N_P`$, $`i_C = N_C`$
* Otherwise, $`i_C = N_C - m_I`$

### From a leaf index to a pair index

$`N_V`$ key-value pairs are stored in $`N_L`$ leaf nodes. What is the total number $`i_V`$ of key-value pairs in the $`i_L`$ leftmost leaves?

Same as [From a parent index to a child index](#from-a-parent-index-to-a-child-index)
(read $`M_I, m_I`$ as the leaf parameters $`M_l, m_l`$, and $`N_C`$ as the pair count $`N_V`$).

* If $`N_L > i_L + \begin{cases} 1 & (\text{tail-away}) \\ 0 & (\text{other layout}) \end{cases}`$, $`i_V = M_l i_L`$
* Otherwise, if $`i_L = N_L`$, $`i_V = N_V`$
* Otherwise, $`i_V = N_V - m_l`$
