# 配列中の準過半数要素を求める乱択アルゴリズム（l = 2 版）

## 0. 問題と保証

- 入力：ランダムアクセス可能な長さ $n$ の配列 $a$、失敗確率 $\delta\in(0,1)$、幅 $w\in(0,1/2)$。
- 記法：値 $v$ の出現割合を $p_v=(\text{配列中の } v \text{ の個数})/n$ とする。
- 出力：配列内の値、または None。
- 保証：確率 $1-\delta$ 以上で次の両方が成り立つ。
  - (i) 出力が値 $v$ なら $p_v>1/2-w$。
  - (ii) $p_{x^*}\ge1/2$ を満たす値 $x^*$ が存在するなら、出力は None ではない。
- コスト：サンプル数は最悪でも $k+2m$（下記定数）、空間 $O(1)$。$n$ に依存しない。

## 1. 必要な確率論の知識

### 1.1 復元抽出と i.i.d. ベルヌーイ試行（sampling with replacement）

一様復元抽出において「サンプルが値 $v$ に等しい」の指示変数は Bernoulli$(p_v)$ で、異なるサンプル間で独立同分布。

### 1.2 Hoeffding の不等式（Hoeffding's inequality）

$X_1,\dots,X_m$ を i.i.d. Bernoulli$(p)$、$S=\sum X_i$ とすると、任意の $t>0$ で
$$P(S\le m(p-t))\le e^{-2mt^2},\qquad P(S\ge m(p+t))\le e^{-2mt^2}.$$

### 1.3 Ville の不等式（Ville's inequality; maximal inequality for nonnegative supermartingales）

$(M_t)_{t\ge0}$ が非負の優マルチンゲールで $M_0=1$ なら、任意の $c>0$ で
$$P\Big(\sup_{t\ge0}M_t\ge c\Big)\le\frac1c.$$
特に $Z_1,Z_2,\dots$ が i.i.d.、非負、$E[Z_i]\le1$ なら $M_t=\prod_{i\le t}Z_i$ はこの条件を満たす。
（尤度比に適用すると SPRT に対する Wald の誤り確率上界になる。本稿で使うのは Ville の不等式のみ。）

### 1.4 Misra–Gries アルゴリズム（Misra–Gries algorithm, frequent items）

長さ $k$ のストリームに $l$ 個のカウンタで Misra–Gries を走らせると、出現回数が $k/(l+1)$ を超える要素はすべて終了時に保持されている。空間 $O(l)$。
（空間を $O(1)$ にするためだけに使う。正しさの証明には不要で、辞書で厳密に数えてもよい。）

## 2. アルゴリズム

### 定数（$\delta,w$ から決まる）

$$k=\Big\lceil18\ln\tfrac4\delta\Big\rceil,\qquad B=\ln\tfrac8\delta,\qquad m=\Big\lceil\tfrac{2}{w^2}\ln\tfrac8\delta\Big\rceil,\qquad \alpha=\ln\tfrac1{1-2w},\qquad \beta=\ln(1+2w).$$

### 手順

1. 候補選択：$k$ 個を復元抽出し、出現回数が $k/3$ を超える値の集合を $C$ とする。回数の合計は $k$ なので $|C|\le2$。（Misra–Gries をカウンタ 2 個で走らせれば、$C$ を含む高々 2 個の集合が $O(1)$ 空間で得られ、以下の議論はそのまま成り立つ。）
2. 検証：$C$ の各要素 $x$ について任意の順に TEST$(x)$ を実行し、最初に「採択」が出た時点でその $x$ を出力して終了。
3. すべて棄却なら（$C$ が空の場合を含む）None を出力。

### TEST$(x)$（打ち切り付き SPRT）

$L\leftarrow0$、$h\leftarrow0$ とし、$t=1,\dots,m$ について：

- 1 個復元抽出する。$x$ に等しければ $L\leftarrow L+\alpha$、$h\leftarrow h+1$。等しくなければ $L\leftarrow L-\beta$。
- $L\ge B$ なら「採択」、$L\le-B$ なら「棄却」を返して終了。

$m$ 回終えても決着しなければ、$h\ge m(1/2-w/2)$ なら「採択」、そうでなければ「棄却」を返す。

### サンプル数の目安

- 上限：$k+2m$（決定的）。
- 該当値 $x^*$ があり $p_{x^*}\approx1/2$ の典型入力：約 $k+B/(2w^2)$。
- 最悪入力（割合が $\beta/(\alpha+\beta)\approx1/2-w/2$ の競合値がある）の期待値：約 $k+m+B/(2w^2)$。
- 例：$\delta=0.01,\ w=0.1$ で $k=108,\ B\approx6.7,\ m=1337$。典型 $\approx440$、最悪入力期待値 $\approx1{,}800$、上限 $2{,}782$。
- 例：$\delta=10^{-6},\ w=0.1$ で $k=274,\ B\approx15.9,\ m=3{,}179$。典型 $\approx1{,}000$、上限 $6{,}632$。

## 3. 失敗確率が $\delta$ 以下であることの証明

失敗を次の 2 事象の和とする。

- $A$：出力が値 $v$ で $p_v\le1/2-w$（偽採択）。
- $B$：$p_{x^*}\ge1/2$ の値 $x^*$ が存在するのに出力が None（見逃し）。

$A,B$ は出力が異なるので排反であり、$P(A)+P(B)\le\delta$ を示せばよい。

$f_0$ を Bernoulli$(1/2)$、$f_1$ を Bernoulli$(1/2-w)$ の確率関数とする。$\alpha=\ln\frac{f_0(1)}{f_1(1)}$、$-\beta=\ln\frac{f_0(0)}{f_1(0)}$ なので、TEST 内の $L$ は $t$ 個目までの対数尤度比 $L_t=\sum_{i\le t}\ln\frac{f_0(X_i)}{f_1(X_i)}$ に等しい。

### 補題 1（候補選択）

$p_{x^*}\ge1/2$ なら $P(x^*\notin C)\le e^{-k/18}\le\delta/4$。

証明：$k$ 個中の $x^*$ の回数 $c^*$ は 1.1 より Bin$(k,p_{x^*})$。$x^*\notin C\iff c^*\le k/3=k(p_{x^*}-t)$、$t=p_{x^*}-1/3\ge1/6$。Hoeffding より $P(c^*\le k/3)\le e^{-2k/36}=e^{-k/18}$。$k\ge18\ln(4/\delta)$ よりこれは $\delta/4$ 以下。$\square$

### 補題 2（偽採択）

$p_x\le1/2-w$ を満たす固定した $x$ について、$P(\text{TEST}(x)=\text{採択})\le e^{-B}+e^{-mw^2/2}\le\delta/4$。

証明：採択は (i) ある $t\le m$ で $L_t\ge B$、または (ii) 打ち切り時に $h\ge m(1/2-w/2)$、のいずれかで起きる。

(i)：$M_t=e^{L_t}=\prod_{i\le t}Z_i$、$Z_i=f_0(X_i)/f_1(X_i)$ とおく。$X_i\sim$ Bernoulli$(p_x)$ のとき
$$E[Z_i]=\frac{p_x}{1-2w}+\frac{1-p_x}{1+2w}=:g(p_x)$$
で、$g$ は $p$ の一次関数、傾き $\frac1{1-2w}-\frac1{1+2w}>0$、$g(1/2-w)=1$。よって $p_x\le1/2-w$ なら $E[Z_i]\le1$ となり、1.3 より $M_t$ は $M_0=1$ の非負優マルチンゲール。Ville の不等式で $c=e^B$ として $P(\exists t:L_t\ge B)=P(\sup_tM_t\ge e^B)\le e^{-B}=\delta/8$。

(ii)：$h\sim$ Bin$(m,p_x)$。$m(1/2-w/2)=m(p_x+t)$、$t=1/2-w/2-p_x\ge w/2$。Hoeffding より $P(h\ge m(1/2-w/2))\le e^{-2m(w/2)^2}=e^{-mw^2/2}\le\delta/8$（$m\ge\frac2{w^2}\ln\frac8\delta$ より）。

両者を足して $\delta/4$。$\square$

### 補題 3（偽棄却）

$p_x\ge1/2$ を満たす固定した $x$ について、$P(\text{TEST}(x)=\text{棄却})\le e^{-B}+e^{-mw^2/2}\le\delta/4$。

証明：棄却は (i) ある $t\le m$ で $L_t\le-B$、または (ii) 打ち切り時に $h < m(1/2-w/2)$、のいずれか。

(i)：$N_t=e^{-L_t}=\prod_{i\le t}\tilde Z_i$、$\tilde Z_i=f_1(X_i)/f_0(X_i)$ とおく。
$$E[\tilde Z_i]=p_x(1-2w)+(1-p_x)(1+2w)=:\tilde g(p_x)$$
は傾き $-4w<0$ の一次関数で $\tilde g(1/2)=1$。よって $p_x\ge1/2$ なら $E[\tilde Z_i]\le1$ で、$N_t$ は非負優マルチンゲール。Ville の不等式より $P(\exists t:L_t\le-B)=P(\sup_tN_t\ge e^B)\le e^{-B}=\delta/8$。

(ii)：$h\sim$ Bin$(m,p_x)$。$m(1/2-w/2)=m(p_x-t)$、$t=p_x-1/2+w/2\ge w/2$。Hoeffding より $P(h < m(1/2-w/2))\le e^{-mw^2/2}\le\delta/8$。$\square$

### 定理

$P(A)+P(B)\le\delta$。

証明：手順 1 のサンプルと各 TEST のサンプルは互いに独立なので、$C$ を条件付けても各 TEST の分布は補題 2, 3 の通りである。

$P(A)$：事象 $A$ が起きるには、ある $v\in C$ で $p_v\le1/2-w$ かつ TEST$(v)$ が採択される必要がある。$C$ で条件付けて補題 2 を $C$ の該当要素それぞれに適用し、$|C|\le2$ を使うと
$$P(A)\le E\Big[\sum_{v\in C,\ p_v\le1/2-w}P(\text{TEST}(v)=\text{採択}\mid C)\Big]\le2\cdot\frac\delta4=\frac\delta2.$$
（2 つ目の TEST は 1 つ目が棄却されたときにしか実行されないが、これは確率を減らす方向にしか働かない。）

$P(B)$：$x^*$ が存在するとする。出力が None になるのは、$x^*\notin C$ か、または $x^*\in C$ かつ TEST$(x^*)$ が棄却されたときに限る（$x^*$ より前の候補が採択されれば出力はその候補であり None ではない）。補題 1 と補題 3 より
$$P(B)\le P(x^*\notin C)+P(\text{TEST}(x^*)=\text{棄却})\le\frac\delta4+\frac\delta4=\frac\delta2.$$

よって $P(\text{失敗})=P(A)+P(B)\le\delta$。$\square$

### 証明で使った事実の対応

- 1.1：$c^*$ と $h$ が二項分布であること、および手順 1 と各 TEST の独立性。
- 1.2：補題 1、補題 2 (ii)、補題 3 (ii)。
- 1.3：補題 2 (i)、補題 3 (i)。$E[Z_i]\le1$ の確認は一次関数の計算のみ。
- 1.4：空間 $O(1)$ の実装のみ。$k/3$ を超える回数の値がすべて保持されることが、補題 1 で必要な「$c^*>k/3\Rightarrow x^*\in C$」と一致する。

