Initialization of Trees
===

## 実現するレイアウト

### 基本方針

* 葉に近い層から考える
* 各層で、左側から順にできるかぎり中身を入れる

### 葉の層の中身

* key-valueペアが $`n`$ 個ある
* 1つの葉ノードには最低 $`m_l`$ 個、最大 $`M_l`$ 個のkey-valueペアを入れてよい

このとき、

* 全部で $`L := \left\lceil n \over M_l \right\rceil`$ 個の葉ノードを作る
* $`n = M_l q + r`$ ($`q, r \in \mathbb{N}, r < M_l`$) として、
  * $`L = 1 (\implies q = 0)`$ のとき、その唯一のノードに $`n`$ 個のkey-valueペアを入れる
  * $`L > 1 \land 0 < r < m_l \iff n - (L - 1) M_l < m_l`$ のとき、
    * 左側から $`L - 2`$ 個のノードに $`M_l`$ 個ずつkey-valueペアを入れる
    * 右側から2番目のノードに $`M_l + r - m_l`$ 個のkey-valueペアを入れる
    * 最も右側のノードに $`m_l`$ 個のkey-valueペアを入れる
  * $`L > 1 \land (r = 0 \lor r \ge m_l) \iff L > 1 \land n - (L - 1) M_l \ge m_l`$ のとき、
    * 左側から $`L - 1`$ 個のノードに $`M_l`$ 個ずつkey-valueペアを入れる
    * 最も右側のノードに $`r`$ 個のkey-valueペアを入れる

### 内部の層の中身

* 1つ下の層に $`n`$ ノードある
* 1つの内部ノードには最低 $`m_I`$ 個、最大 $`M_I`$ 個の子供がいてよい

このとき、

* 全部で $`I := \left\lceil n \over M_I \right\rceil`$ 個の内部ノードを作る
* $`n = M_I q + r`$ ($`q, r \in \mathbb{N}, r < M_I`$) として、
  * **レイアウト「root」**: $`I = 1`$ のとき、その唯一のノードに $`n`$ 個の子供を持たせる
  * **レイアウト「tail-away」**: $`I > 1 \land 0 < r < m_I \iff I > 1 \land n - (I - 1) M_I < m_I`$ のとき、
    * 左側から $`I - 2`$ 個のノードに $`M_I`$ 個ずつ子供を持たせる
    * 右側から2番目のノードに $`M_I + r - m_I`$ 個の子供を持たせる
    * 最も右側のノードに $`m_I`$ 個の子供を持たせる
  * **レイアウト「tidy」**: $`I > 1 \land (r = 0 \lor r \ge m_I) \iff I > 1 \land n - (I - 1) M_I \ge m_I`$ のとき、
    * 左側から $`I - 1`$ 個のノードに $`M_I`$ 個ずつ子供を持たせる
    * 最も右側のノードに $`r`$ 個の子供を持たせる

## いろんな計算

### 子のインデックスから親のインデックス

$`N_C`$ 個の子からなる層の上に $`N_P`$ 個の親からなる層がある。「全ての子が、子の層の中で左側から $`i_C`$ 個までに位置している」という条件を満たす親の個数 $`i_P`$ は？ ($`0 \le i_C \le N_C`$)

* レイアウト「root」のとき、
  * $`i_C = N_C`$ のとき、 $`i_P = 1 (= N_P)`$
  * $`0 \le i_C < N_C`$ のとき、 $`i_P = 0`$
* レイアウト「tail-away」のとき、
  * $`i_C = N_C`$ のとき、 $`i_P = N_P`$
  * $`N_C - m_I \le i_C < N_C`$ のとき、 $`i_P = N_P - 1`$
  * $`0 \le i_C < N_C - m_I`$ のとき、 $`i_P = \left\lfloor i_C \over M_I \right\rfloor`$
* レイアウト「tidy」のとき、
  * $`i_C = N_C`$ のとき、 $`i_P = N_P`$
  * $`0 \le i_C < N_C`$ のとき、 $`i_P = \left\lfloor i_C \over M_I \right\rfloor`$

これは次のように計算できる。

* $`i_C + m_I < N_C`$ なら、 $`i_P = \left\lfloor i_C \over M_I \right\rfloor`$
* そうでなく $`i_C < N_C`$ なら、 $`i_P = N_P - 1`$
* そうでないなら、 $`i_P = N_P`$

### 親のインデックスから子のインデックス

$`N_C`$ 個の子からなる層の上に $`N_P`$ 個の親からなる層がある。左側から $`i_P`$ 個の親についての、子の数の合計 $`i_C`$ は？

* レイアウト「root」のとき、
  * $`i_P = 1 (= N_P)`$ のとき、 $`i_C = N_C`$
  * $`i_P = 0`$ のとき、 $`i_C = 0`$
* レイアウト「tail-away」のとき、
  * $`i_P = N_P`$ のとき、 $`i_C = N_C`$
  * $`i_P = N_P - 1`$ のとき、 $`i_C = N_C - m_I`$
  * $`0 \le i_P < N_P - 1`$ のとき、 $`i_C = M_I i_P`$
* レイアウト「tidy」のとき、
  * $`i_P = N_P`$ のとき、 $`i_C = N_C`$
  * $`0 \le i_P < N_P`$ のとき、 $`i_C = M_I i_P`$

これは次のように計算できる。

* $`N_P > i_P + \begin{cases} 1 & (\text{tail-away}) \\ 0 & (\text{other layout}) \end{cases}`$ のとき、 $`i_C = M_I i_P`$
* そうでなく $`i_P = N_P`$ なら、 $`i_C = N_C`$
* そうでないなら、 $`i_C = N_C - m_I`$

### 葉のインデックスからペアのインデックス

$`N_V`$ 個のkey-valueペアが $`N_L`$ 個の葉ノードに入っている。左側から $`i_L`$ 個の葉に入っていkey-valueペアの総数 $`i_V`$ は？

[親のインデックスから子のインデックス](#親のインデックスから子のインデックス)と同様。

* $`N_L > i_L + \begin{cases} 1 & (\text{tail-away}) \\ 0 & (\text{other layout}) \end{cases}`$ のとき、 $`i_V = M_L i_L`$
* そうでなく $`i_L = N_L`$ なら、 $`i_V = N_C`$
* そうでないなら、 $`i_V = N_C - m_L`$
