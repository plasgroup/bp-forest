# 一部の値が取り除かれた配列に対する最頻値の $\varepsilon$ 近似を求める逐次乱択アルゴリズム

## 0. 問題と保証

- 元の配列を $\bar a$、その長さを $\bar n$ とする。$\bar a$ からある値 $v_0$ の出現をすべて取り除いた配列が $a$（長さ $n$）である。
- 入力：$n$、$a$（ランダムアクセス可能）、$v_0$、取り除かれた個数 $r\ge0$、精度 $\varepsilon\in(0,1)$、失敗確率 $\delta\in(0,1)$。$\bar n=n+r$ は入力から計算する。何も取り除かれていない場合は $r=0$ とし、$v_0$ は存在しないものとして扱う（§6 参照）。
- 記法：値 $v$ の $\bar a$ における出現割合を $\bar p_v$、$a$ における出現割合を $p_v$ とする。$v\ne v_0$ について $\bar p_v=\frac{n}{\bar n}p_v$、また $\bar p_{v_0}=r/\bar n$ は正確に分かる。$\bar p_{(1)}=\max_v\bar p_v$、$p_{(1)}=\max_{v\ne v_0}p_v$ とし、それぞれを達成する値のひとつを $\bar v^*$、$v^*$ とする。
- 出力：$\bar a$ 内の値。
- 保証：確率 $1-\delta$ 以上で、出力 $v$ が $\bar p_v\ge\bar p_{(1)}-\varepsilon$ を満たす。特に、$\bar a$ の上位 2 値の割合の差が $\varepsilon$ 以上なら、出力は真の最頻値である。
- コスト：サンプル数は $n$ に依存しない。最悪値は $k+m$（§3）、割合の差が大きい入力では早期に停止する。空間 $O(1/\varepsilon)$。

厳密な最頻値を $n$ に依存しないコストで求めることは不可能である（上位 2 値の差が $1/n$ まで小さくなりうるため）。$\varepsilon$ による緩和はこのための最小限の妥協である。

## 1. 必要な確率論の知識

### 1.1 復元抽出と i.i.d. ベルヌーイ試行（sampling with replacement）

一様復元抽出において「サンプルが値 $v$ に等しい」の指示変数は Bernoulli$(p_v)$ で、異なるサンプル間で独立同分布。

### 1.2 Hoeffding の不等式（Hoeffding's inequality）

$X_1,\dots,X_t$ を i.i.d. Bernoulli$(p)$、$\hat p=\frac1t\sum X_i$ とすると、任意の $\rho>0$ で
$$P(\hat p\le p-\rho)\le e^{-2t\rho^2},\qquad P(\hat p\ge p+\rho)\le e^{-2t\rho^2}.$$

### 1.3 乗法型 Chernoff 上界・下側（multiplicative Chernoff bound, lower tail）

$X$ を独立なベルヌーイ変数の和、$\mu=E[X]$ とすると、任意の $\eta\in(0,1)$ で
$$P\big(X\le(1-\eta)\mu\big)\le e^{-\eta^2\mu/2}.$$

### 1.4 時刻についての和集合上界（union bound, Basel problem）

事象列 $E_1,E_2,\dots$ が $P(E_t)\le c/t^2$ を満たすなら
$$P\Big(\bigcup_{t\ge1}E_t\Big)\le c\sum_{t\ge1}\frac1{t^2}=c\cdot\frac{\pi^2}6<1.645\,c.$$
（これで作る「すべての時刻で同時に成り立つ信頼区間」は confidence sequence / anytime-valid confidence bound と呼ばれる。）

### 1.5 Misra–Gries アルゴリズム（Misra–Gries algorithm, frequent items）

長さ $k$ のストリームに $l$ 個のカウンタで Misra–Gries を走らせると、出現回数が $k/(l+1)$ を超える要素はすべて終了時に保持されている。空間 $O(l)$。

## 2. 帰着の考え方

2 つの事実を使う。

1. $v_0$ 以外の値の順序は $a$ と $\bar a$ で一致し、$\bar a$ での加法誤差 $\varepsilon$ は $a$ での加法誤差
   $$\varepsilon'=\varepsilon\cdot\frac{\bar n}{n}$$
   に対応する。$\varepsilon'\ge\varepsilon$ なので、$a$ 上では要求精度が緩くなる。
2. $\bar p_{v_0}=r/\bar n$ は既知なので、$v_0$ を「推定誤差 0 の候補」として検証段の候補集合に加えればよい。$v_0$ と他の候補との比較も、他の候補どうしの比較と同じ停止規則で扱える。

候補選択は $a$ に対して精度 $\varepsilon'$ で行い、検証は候補集合 $C\cup\{v_0\}$ 上で、推定値を $\bar a$ の単位に換算して行う。

## 3. アルゴリズム

### 定数

$\delta_1=\delta_2=\delta_3=\delta/3$、$\varepsilon'=\varepsilon\bar n/n$ とし、
$$
k=\Big\lceil\frac{8}{\varepsilon'}\ln\frac1{\delta_1}\Big\rceil,\qquad
l=\Big\lceil\frac{2}{\varepsilon'}\Big\rceil .
$$
第 1 段で得られる候補集合 $C$ の要素数を $L=|C|$（$L\le l$）とし、
$$
m=\Big\lceil\frac{2}{\varepsilon'^2}\ln\frac{2L}{\delta_3}\Big\rceil,\qquad
\rho_t=\sqrt{\frac{\ln(4Lt^2/\delta_2)}{2t}}\quad(t\ge1).
$$
$\rho_t$ は $t$ について単調減少である（$4L/\delta_2\ge12>e^2$ より）。さらに $\bar a$ の単位に換算した推定値と半径を
$$
\hat q_v(t)=\frac{n}{\bar n}\hat p_v(t),\quad \mathrm{rad}(v)=\bar\rho_t:=\frac{n}{\bar n}\rho_t\qquad(v\in C),\qquad
\hat q_{v_0}(t)=\frac r{\bar n},\quad \mathrm{rad}(v_0)=0
$$
とする。ここで $\hat p_v(t)$ は第 2 段の $t$ 個のサンプル中で $v$ が出現した割合である。

### 手順

1. $r+\varepsilon\bar n\ge n$ なら $v_0$ を出力して終了。（このとき $a$ のどの値も $\bar p_v\le n/\bar n\le r/\bar n+\varepsilon$。）
   以下では $n>r+\varepsilon\bar n$、したがって $\varepsilon'<1$ である。
2. 第 1 段（候補選択）：$a$ から $k$ 個を復元抽出し、Misra–Gries を $l$ 個のカウンタで走らせて、保持された値の集合を $C$ とする。（辞書で厳密に数え、出現回数が $k/(l+1)$ を超える値を $C$ としてもよい。）$C=\emptyset$ なら $v_0$ を出力して終了。
3. 第 2 段（逐次検証）：$a$ から新たに 1 個ずつ復元抽出し、$v\in C$ について $\hat p_v(t)$ を維持する。各 $t$ で $\hat v_t=\arg\max_{v\in C\cup\{v_0\}}\hat q_v(t)$（同点は任意）とし：
   - 早期停止：すべての $u\in C\cup\{v_0\}$、$u\ne\hat v_t$ について
     $$\hat q_{\hat v_t}(t)-\mathrm{rad}(\hat v_t)\ \ge\ \hat q_u(t)+\mathrm{rad}(u)-\varepsilon$$
     が成り立てば $\hat v_t$ を出力して終了。
   - 打ち切り：$t=m$ に達したら $\hat v_m$ を出力して終了。

### 実装上の注意

- 1 サンプルあたりの更新は $O(1)$。停止条件の判定は $O(L)$ なので、毎回でなく $L$ 回ごと、あるいは $t$ が幾何級数的な時刻でのみ判定してもよい（停止が高々その間隔分だけ遅れる）。
- 空間は $C$ の計数分で $O(L)=O(1/\varepsilon')\le O(1/\varepsilon)$。
- 手順 1, 2 の判定はサンプリングを要しない。

## 4. 失敗確率が $\delta$ 以下であることの証明

次の 3 事象を定義する。

- $F$：$v^*\in C$。
- $G$：すべての $v\in C$ とすべての $t\ge1$ で $|\hat p_v(t)-p_v|\le\rho_t$。
- $H$：すべての $v\in C$ で $|\hat p_v(m)-p_v|\le\varepsilon'/2$。

$\bar a$ の単位では、$G$ の下で $|\hat q_v(t)-\bar p_v|\le\bar\rho_t$、$H$ の下で $|\hat q_v(m)-\bar p_v|\le\varepsilon/2$（$v\in C$）が成り立ち、$v_0$ については $\hat q_{v_0}=\bar p_{v_0}$ が常に成り立つ。

### 補題 1

$p_{(1)}\ge\varepsilon'$ なら $P(F^c)\le e^{-k\varepsilon'/8}\le\delta_1$。

証明：第 1 段での $v^*$ の出現回数 $c^*$ は 1.1 より Bin$(k,p_{(1)})$、$\mu=kp_{(1)}\ge k\varepsilon'$。1.5 より $c^*>k/(l+1)$ なら $v^*\in C$ であり、$l+1>2/\varepsilon'$ から $k/(l+1)<k\varepsilon'/2\le\mu/2$。よって
$$P(F^c)\le P(c^*\le\mu/2)\le e^{-\mu/8}\le e^{-k\varepsilon'/8}$$
（1.3 を $\eta=1/2$ で適用）。$k\ge\frac8{\varepsilon'}\ln\frac1{\delta_1}$ よりこれは $\delta_1$ 以下。$\square$

### 補題 2

$P(G^c)\le\delta_2$。

証明：第 2 段のサンプルは第 1 段と独立なので、$C$ を固定して考えてよい。固定した $v\in C$、$t$ について、1.2（両側）より
$$P(|\hat p_v(t)-p_v|>\rho_t)\le2e^{-2t\rho_t^2}=\frac{\delta_2}{2Lt^2}.$$
1.4 より $t$ について足すと $<\delta_2/L$、さらに $v\in C$ について足して $\le\delta_2$。$\square$

### 補題 3

$P(H^c)\le\delta_3$。

証明：固定した $v\in C$ について 1.2 より $P(|\hat p_v(m)-p_v|>\varepsilon'/2)\le2e^{-m\varepsilon'^2/2}$。$L$ 個について足して $2Le^{-m\varepsilon'^2/2}\le\delta_3$（$m\ge\frac2{\varepsilon'^2}\ln\frac{2L}{\delta_3}$ より）。$\square$

### 補題 4（手順 1, 2 の出力）

手順 1 または手順 2 で $v_0$ が出力されたとき、$p_{(1)}\ge\varepsilon'$ なら $F$ の下で、そうでなければ無条件に、$\bar p_{v_0}\ge\bar p_{(1)}-\varepsilon$。

証明：手順 1 では $a$ のすべての値 $v$ で $\bar p_v\le n/\bar n\le r/\bar n+\varepsilon$。手順 2 で $C=\emptyset$ のとき、$p_{(1)}\ge\varepsilon'$ なら $F$ に反するので $p_{(1)}<\varepsilon'$、したがって $a$ のすべての値で $\bar p_v<\varepsilon$。$\bar p_{(1)}=r/\bar n$ なら自明、そうでなければ $\bar p_{(1)}<\varepsilon\le\bar p_{v_0}+\varepsilon$。$\square$

### 補題 5（$\bar a$ の最頻値が候補集合に入る）

$\bar v^*=v_0$ なら常に、$\bar v^*\ne v_0$ かつ $p_{(1)}\ge\varepsilon'$ なら $F$ の下で、$\bar v^*\in C\cup\{v_0\}$。$\bar v^*\ne v_0$ かつ $p_{(1)}<\varepsilon'$ なら $\bar p_{(1)}<\varepsilon$ であり、任意の出力が保証を満たす。

証明：$\bar v^*\ne v_0$ なら $\bar v^*$ は $a$ の最頻値でもあるので $v^*$ と取れる。$p_{(1)}<\varepsilon'$ なら $\bar p_{(1)}=\frac n{\bar n}p_{(1)}<\varepsilon$。$\square$

### 補題 6（早期停止の正しさ）

$\bar v^*\in C\cup\{v_0\}$ かつ $G$ の下で、早期停止により出力された $\hat v$ は $\bar p_{\hat v}\ge\bar p_{(1)}-\varepsilon$ を満たす。

証明：停止時刻を $t$ とする。任意の $u\in C\cup\{v_0\}$ について、$G$（$u\in C$）または等式（$u=v_0$）と停止条件より
$$\bar p_u\le\hat q_u(t)+\mathrm{rad}(u)\le\hat q_{\hat v}(t)-\mathrm{rad}(\hat v)+\varepsilon\le\bar p_{\hat v}+\varepsilon$$
（$u=\hat v$ のときは自明）。$u=\bar v^*$ と取る。$\square$

### 補題 7（打ち切りの正しさ）

$\bar v^*\in C\cup\{v_0\}$ かつ $H$ の下で、打ち切りにより出力された $\hat v_m$ は $\bar p_{\hat v_m}\ge\bar p_{(1)}-\varepsilon$ を満たす。

証明：$H$ の下で $v\in C\cup\{v_0\}$ のすべてについて $|\hat q_v(m)-\bar p_v|\le\varepsilon/2$。よって
$$\bar p_{\hat v_m}\ge\hat q_{\hat v_m}(m)-\frac\varepsilon2\ge\hat q_{\bar v^*}(m)-\frac\varepsilon2\ge\bar p_{(1)}-\varepsilon.\ \square$$

### 定理

出力が $\bar p_v\ge\bar p_{(1)}-\varepsilon$ を満たさない確率は $\delta$ 以下。

証明：手順 1, 2 で終了する場合は補題 4。手順 3 に進んだ場合、補題 5 より、$\bar v^*\in C\cup\{v_0\}$ が成り立つ（$F$ を要するのは $\bar v^*\ne v_0$ かつ $p_{(1)}\ge\varepsilon'$ の場合のみ）か、任意の出力が正しいかのいずれかであり、前者では補題 6, 7 より $G\cap H$ の下で出力は正しい。したがって失敗事象は $F^c$（$p_{(1)}\ge\varepsilon'$ の場合のみ関与）、$G^c$、$H^c$ の和に含まれ、補題 1–3 より確率は $\delta_1+\delta_2+\delta_3=\delta$ 以下。$\square$

## 5. サンプル数

### 最悪値

$k+m$。$\varepsilon'=\varepsilon\bar n/n$ なので、$r$ が大きいほど $k,l$ は $n/\bar n$ 倍、$m$ は $(n/\bar n)^2$ 倍に減る。

### 早期停止の保証（適応性）

候補集合 $C\cup\{v_0\}$ 内での $\bar a$ の単位の上位 2 値の差を $\bar\Delta$ とおく（要素数 1 なら停止条件は空で直ちに停止する）。$\gamma=\frac{\bar n}{n}\cdot\frac{\max(\varepsilon,\bar\Delta)}4$ とすると、$\bar v^*\in C\cup\{v_0\}$ かつ $G$ の下で、$\rho_t\le\gamma$（すなわち $\bar\rho_t\le\max(\varepsilon,\bar\Delta)/4$）を満たす最初の時刻 $T_0$ までに早期停止する。

証明：$\bar\rho=\bar\rho_t$ と書く。すべての候補の半径は $\bar\rho$ 以下である。
$4\bar\rho\le\varepsilon$ の場合、任意の $u\ne\hat v_t$ について
$$\hat q_{\hat v_t}(t)-\mathrm{rad}(\hat v_t)\ge\hat q_{\bar v^*}(t)-\bar\rho\ge\bar p_{(1)}-2\bar\rho\ge\bar p_u+2\bar\rho-\varepsilon\ge\hat q_u(t)+\mathrm{rad}(u)-\varepsilon$$
（3 つ目の不等式は $\bar p_u\le\bar p_{(1)}$ と $4\bar\rho\le\varepsilon$）。
$\varepsilon<\bar\Delta$ かつ $4\bar\rho\le\bar\Delta$ の場合、$2\bar\rho<\bar\Delta$ より $\hat q_{\bar v^*}(t)\ge\bar p_{(1)}-\bar\rho>\bar p_u+\bar\rho\ge\hat q_u(t)$（$u\ne\bar v^*$）なので $\hat v_t=\bar v^*$。このとき $u\ne\bar v^*$ について
$$\hat q_{\bar v^*}(t)-\mathrm{rad}(\bar v^*)\ge\bar p_{(1)}-2\bar\rho\ge\bar p_u+\bar\Delta-2\bar\rho\ge\bar p_u+2\bar\rho-\varepsilon\ge\hat q_u(t)+\mathrm{rad}(u)-\varepsilon$$
（3 つ目の不等式は $4\bar\rho\le\bar\Delta<\bar\Delta+\varepsilon$）。$\square$

$T_0$ の明示的な上界は
$$T_0\le\max\Big\{1,\ \Big\lceil\frac1{\gamma^2}\ln\frac{4L}{\delta_2\gamma^4}\Big\rceil\Big\}$$
である（この $t$ で $\ln(4Lt^2/\delta_2)\le2\gamma^2t$ となることは、$A=\ln(4L/\delta_2)$、$\Gamma=\ln(1/\gamma^2)$ とおくと $A+2\Gamma\ge2\ln(A+2\Gamma)$ に帰着し、$x\ge2\ln x$ から従う。$A+2\Gamma\le0$ なら $\rho_1\le\gamma$ で $T_0=1$）。

$v_0$ の半径は 0 なので、$v_0$ が明確な最頻値である場合はこの評価より早く止まる（$v_0$ を含む比較では半径が片側にしか現れない）。

### 数値例（$\varepsilon=0.1$、$\delta=0.01$）

| | $\varepsilon'$ | $k$ | $l$ | $m$（$L=l$） | 最悪値 $k+m$ |
|---|---|---|---|---|---|
| $r=0$ | $0.1$ | $457$ | $20$ | $1{,}879$ | $2{,}336$ |
| $r=0.3\bar n$ | $0.143$ | $320$ | $14$ | $886$ | $1{,}206$ |

$r=0$ での早期停止の上界 $T_0$：$\bar\Delta\le0.1$ で約 $7{,}500$（打ち切り $m$ が先）、$\bar\Delta=0.5$ で約 $1{,}200$、$\bar\Delta=0.8$ で約 $410$。

- 上の $T_0$ は保証としての上界で、実際の停止はこれより早いことが多い。ただし保証の上で早期停止が打ち切りに勝つのは、差 $\bar\Delta$ が $\varepsilon$ の数倍以上ある場合に限られる。$\bar\Delta\le\varepsilon$ では打ち切りが先に来て、コストは固定サンプル法と同じ。
- $\rho_t$ の定数は、$\ln t^2$ の代わりに $\ln\ln t$ 型の項を使う信頼区間（law of the iterated logarithm 型の confidence sequence）や、分散を使う empirical Bernstein 型の不等式で改善できる。本書は証明の単純さを優先している。

## 6. 補足

- $r=0$（何も取り除かれていない）場合：$\varepsilon'=\varepsilon$ で、手順 1 は発動せず、候補集合は $C$ のみとする。手順 2 で $C=\emptyset$ なら $a$ の任意の要素を出力する（補題 4 と同様に、このとき $p_{(1)}<\varepsilon$ か $F^c$ のどちらかである）。
- 第 1 段のコスト $k$ が $1/\varepsilon'$ のオーダーで済むのは、$v^*$ の出現回数について「期待値の半分以上」という乗法的な条件だけを要求しているためである（1.3）。$1/\varepsilon'^2$ が必要になるのは第 2 段の加法的な精度の判定だけである。
- 仮想的に戻す方法：$[0,\bar n)$ から一様に添字を引き、$n$ 未満なら $a$ のその要素、$n$ 以上なら $v_0$ を返せば $\bar a$ からの一様復元抽出になり、$r=0$ の場合のアルゴリズムを無変更で使える。ただし既知の $\bar p_{v_0}$ を推定し直すことになり、$m$ も $(\bar n/n)^2$ 倍かかる。実装を変えたくない場合の代替としてのみ意味がある。
- 出力が満たすのは $\bar p_v\ge\bar p_{(1)}-\varepsilon$ であり、真の最頻値との一致は $\bar\Delta\ge\varepsilon$ のときのみ保証される。厳密な最頻値が必要なら、本アルゴリズムで候補を $C\cup\{v_0\}$ に絞ったあと、$a$ を全走査して $C$ の各値を正確に数え、$r$ と比較すればよい（$C$ が小さいので計数は SIMD 化しやすい）。
