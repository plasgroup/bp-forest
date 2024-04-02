Serialization
===

## 実現すること

* input:
  * a B+-tree: $`T = \{ k_i \mapsto v_i \}_{i \in [0, N)}, i < j \implies k_i < k_j`$
  * a `delimiter`($`d`$) key
* output:
  * key-value pairs whose keys < `delimiter`: $`L = [ (k_i, v_i) ]_{i \in [0, N')}`$
  * a B+-tree whose elements with keys >= `delimiter`: $`\{ k_i \mapsto v_i \}_{i \in [N', N)}`$
* post-condition: $`i \in [0, N') \iff k_i < d`$

## アルゴリズム

0.  (変数宣言)
    1.  `L = []`
1.  copy&deletion: 木の左端から順にkey, valueをコピーしながら、空になったノードは削除する
    1.  `leaf <- T.leftmost_leaf`
    2.  loop:
        1.  if `leaf.max_key < d`:  // 全てコピーする
            1.  `L <- L ++ leaf.kv_pairs`
            2.  `deleted <- leaf`
            3.  `leaf <- leaf.right_leaf`
            4.  loop:  // 空になったノードを削除する。これは親に伝播しうる。
                1.  if `deleted.is_root`:  // 木全体が空になった
                    1.  `deleted <- EmptyRoot`
                    2.  return
                2.  else (`!deleted.is_root`):
                    1.  `parent <- deleted.parent`
                    2.  `is_last_child <- (parent.rightmost_child == node)`
                    3.  `free(deleted)`
                    4.  if `is_last_child`: `deleted <- parent`  // 最後の子供が消えた
                    5.  else (`!is_last_child`): break
        1.  else (`leaf.max_key >= d`):  // 途中までコピーする
            1.  `former <- filter((k, v) -> { k < d }, leaf.kv_pairs)`
            2.  `latter <- filter((k, v) -> { k >= d }, leaf.kv_pairs)`
            3.  `L <- L ++ former`
            4.  `leaf.kv_pairs <- latter`
            5.  break
    *   木が空になってearly returnするか、そうでなければ `leaf` が空でないリーフのうち左端になっている
2.  hole-filling: 消えたノードを子供として指している内部ノードに手を入れ、それをなくす
    1.  `node <- leaf`
    2.  while `!node.is_root`:
        1.  `parent <- node.parent`
        2.  `idx <- find(node)_in(parent.children)`
        3.  `parent.keys <- parent.keys[idx:]`
        4.  `parent.children <- parent.children[idx:]`
        5.  `node <- parent`
3.  regularization: 中身が少なすぎるノードをなくす
    1.  while `T.root.keys == []`:  // 子が1つだけのルートノードをなくす
        1.  `child <- T.root.children[0]`
        2.  `free(T.root)`
        3.  `T.root <- child`
    3.  if `!T.root.is_leaf`:
        1.  `parent <- T.root`
        3.  loop:  // 中身が少なすぎる非ルートなノードをなくす
            1.  `node <- parent.children[0]`
            2.  if `node.is_leaf`:
                1.  if `node.keys.size < MIN_NUM_KEYS`:
                    1.  `sibling <- parent.children[1]`
                    2.  if `node.keys.size + sibling.keys.size >= 2 * MIN_NUM_KEYS`:  // 兄弟から子供を移す
                        1.  `n_move <- MIN_NUM_KEYS - node.keys.size`
                        2.  foreach `grandchild` in `sibling.children[:n_move]`:
                            1.  `grandchild.parent <- node`
                        3.  `node.keys <- node.keys ++ [parent.keys[0]] ++ sibling.keys[:(n_move - 1)]`
                        4.  `parent.keys[0] <- sibling.keys[n_move - 1]`
                        5.  `sibling.keys <- sibling.keys[n_move:]`
                        6.  `node.children <- node.children ++ sibling.children[:n_move]`
                        7.  `sibling.children <- sibling.children[n_move:]`
                    3.  else (`node.keys.size + sibling.keys.size < 2 * MIN_NUM_KEYS`):  // 兄弟とmerge
                        1.  foreach `grandchild` in `node.children`:
                            1.  `grandchild.parent <- sibling`
                        2.  `sibling <- merge(node, parent.keys[0], sibling)`
                        3.  `free(node)`
                        4.  `parent.keys <- parent.keys[1:]`
                        5.  `parent.children <- parent.children[1:]`
                        6.  if `parent.keys == []`:
                            1.  `free(parent)`
                            2.  `T.root <- sibling`
                2.  break
            3.  else (`!node.is_leaf`):
                1.  if `node.keys.size < MIN_NUM_KEYS + 1`:  // 次の周回でmergeが起きてもいいように、(最小+1)のサイズにする
                    1.  `sibling <- parent.children[1]`
                    2.  if `node.keys.size + sibling.keys.size >= 2 * MIN_NUM_KEYS + 1`:  // 兄弟から子供を移す
                        1.  `n_move <- (MIN_NUM_KEYS + 1) - node.keys.size`
                        2.  foreach `grandchild` in `sibling.children[:n_move]`:
                            1.  `grandchild.parent <- node`
                        3.  `node.keys <- node.keys ++ [parent.keys[0]] ++ sibling.keys[:(n_move - 1)]`
                        4.  `parent.keys[0] <- sibling.keys[n_move - 1]`
                        5.  `sibling.keys <- sibling.keys[n_move:]`
                        6.  `node.children <- node.children ++ sibling.children[:n_move]`
                        7.  `sibling.children <- sibling.children[n_move:]`
                        8.  `parent <- node`
                    3.  else (`node.keys.size + sibling.keys.size < 2 * MIN_NUM_KEYS + 1`):  // 兄弟とmerge
                        1.  foreach `grandchild` in `node.children`:
                            1.  `grandchild.parent <- sibling`
                        2.  `sibling <- merge(node, parent.keys[0], sibling)`
                        3.  `free(node)`
                        4.  `parent.keys <- parent.keys[1:]`
                        5.  `parent.children <- parent.children[1:]`
                        6.  if `parent.keys == []`:
                            1.  `free(parent)`
                            2.  `T.root <- sibling`
                        7.  `parent <- sibling`
