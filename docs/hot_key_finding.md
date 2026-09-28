# A sequential randomized algorithm for an $\varepsilon$-approximate mode of an array with some values removed

## 0. Problem and guarantee

- Let $\bar a$ be the original array and $\bar n$ its length. The array $a$ (length $n$) is $\bar a$ with all occurrences of some value $v_0$ removed.
- Input: $n$, $a$ (random access), $v_0$, the number removed $r\ge0$, accuracy $\varepsilon\in(0,1)$, failure probability $\delta\in(0,1)$. $\bar n=n+r$ is computed from the input. If nothing was removed, set $r=0$ and treat $v_0$ as nonexistent (see §6).
- Notation: let $\bar p_v$ be the fraction of occurrences of value $v$ in $\bar a$, and $p_v$ its fraction in $a$. For $v\ne v_0$, $\bar p_v=\frac{n}{\bar n}p_v$, and $\bar p_{v_0}=r/\bar n$ is known exactly. Let $\bar p_{(1)}=\max_v\bar p_v$ and $p_{(1)}=\max_{v\ne v_0}p_v$, and let $\bar v^*$ and $v^*$ be values attaining them.
- Output: a value in $\bar a$.
- Guarantee: with probability at least $1-\delta$, the output $v$ satisfies $\bar p_v\ge\bar p_{(1)}-\varepsilon$. In particular, if the fractions of the top 2 values of $\bar a$ differ by at least $\varepsilon$, the output is the true mode.
- Cost: the number of samples does not depend on $n$. The worst case is $k+m$ (§3), and on inputs with a large gap in fractions it stops early. Space $O(1/\varepsilon)$.

Finding the exact mode at a cost independent of $n$ is impossible (because the gap between the top 2 values can be as small as $1/n$). Relaxing by $\varepsilon$ is the minimal compromise for this.

## 1. Required probability background

### 1.1 Sampling with replacement and i.i.d. Bernoulli trials

Under uniform sampling with replacement, the indicator of "the sample equals value $v$" is Bernoulli$(p_v)$, independent and identically distributed across samples.

### 1.2 Hoeffding's inequality

Let $X_1,\dots,X_t$ be i.i.d. Bernoulli$(p)$ and $\hat p=\frac1t\sum X_i$. Then for any $\rho>0$,
$$P(\hat p\le p-\rho)\le e^{-2t\rho^2},\qquad P(\hat p\ge p+\rho)\le e^{-2t\rho^2}.$$

### 1.3 Multiplicative Chernoff bound, lower tail

Let $X$ be a sum of independent Bernoulli variables and $\mu=E[X]$. Then for any $\eta\in(0,1)$,
$$P\big(X\le(1-\eta)\mu\big)\le e^{-\eta^2\mu/2}.$$

### 1.4 Union bound over time (Basel problem)

If a sequence of events $E_1,E_2,\dots$ satisfies $P(E_t)\le c/t^2$, then
$$P\Big(\bigcup_{t\ge1}E_t\Big)\le c\sum_{t\ge1}\frac1{t^2}=c\cdot\frac{\pi^2}6<1.645\,c.$$
(A "confidence interval that holds simultaneously at all times" built this way is called a confidence sequence / anytime-valid confidence bound.)

### 1.5 Misra–Gries algorithm (frequent items)

Running Misra–Gries with $l$ counters on a stream of length $k$, every element that occurs more than $k/(l+1)$ times is retained at the end. Space $O(l)$.

## 2. Idea of the reduction

Two facts are used.

1. The order of values other than $v_0$ is the same in $a$ and $\bar a$, and an additive error $\varepsilon$ in $\bar a$ corresponds to an additive error
   $$\varepsilon'=\varepsilon\cdot\frac{\bar n}{n}$$
   in $a$. Since $\varepsilon'\ge\varepsilon$, the required accuracy is looser on $a$.
2. $\bar p_{v_0}=r/\bar n$ is known, so it suffices to add $v_0$ to the candidate set of the verification stage as a "candidate with zero estimation error". Comparisons between $v_0$ and other candidates are handled by the same stopping rule as comparisons among the other candidates.

Candidate selection is done on $a$ with accuracy $\varepsilon'$, and verification is done on the candidate set $C\cup\{v_0\}$ with the estimates converted to the units of $\bar a$.

## 3. Algorithm

### Constants

Let $\delta_1=\delta_2=\delta_3=\delta/3$, $\varepsilon'=\varepsilon\bar n/n$, and
$$
k=\Big\lceil\frac{8}{\varepsilon'}\ln\frac1{\delta_1}\Big\rceil,\qquad
l=\Big\lceil\frac{2}{\varepsilon'}\Big\rceil .
$$
Let $L=|C|$ ($L\le l$) be the size of the candidate set $C$ obtained in stage 1, and
$$
m=\Big\lceil\frac{2}{\varepsilon'^2}\ln\frac{2L}{\delta_3}\Big\rceil,\qquad
\rho_t=\sqrt{\frac{\ln(4Lt^2/\delta_2)}{2t}}\quad(t\ge1).
$$
$\rho_t$ is monotonically decreasing in $t$ (since $4L/\delta_2\ge12>e^2$). Further, define the estimates and radii converted to the units of $\bar a$ as
$$
\hat q_v(t)=\frac{n}{\bar n}\hat p_v(t),\quad \mathrm{rad}(v)=\bar\rho_t:=\frac{n}{\bar n}\rho_t\qquad(v\in C),\qquad
\hat q_{v_0}(t)=\frac r{\bar n},\quad \mathrm{rad}(v_0)=0
$$
where $\hat p_v(t)$ is the fraction of the $t$ samples of stage 2 in which $v$ occurred.

### Procedure

1. If $r+\varepsilon\bar n\ge n$, output $v_0$ and stop. (In this case every value of $a$ has $\bar p_v\le n/\bar n\le r/\bar n+\varepsilon$.)
   From here on, $n>r+\varepsilon\bar n$, and hence $\varepsilon'<1$.
2. Stage 1 (candidate selection): draw $k$ samples with replacement from $a$, run Misra–Gries with $l$ counters, and let $C$ be the set of retained values. (Alternatively, count exactly with a dictionary and let $C$ be the values occurring more than $k/(l+1)$ times.) If $C=\emptyset$, output $v_0$ and stop.
3. Stage 2 (sequential verification): draw fresh samples from $a$ with replacement, one at a time, and maintain $\hat p_v(t)$ for $v\in C$. At each $t$, let $\hat v_t=\arg\max_{v\in C\cup\{v_0\}}\hat q_v(t)$ (ties broken arbitrarily), and:
   - Early stop: if for all $u\in C\cup\{v_0\}$ with $u\ne\hat v_t$
     $$\hat q_{\hat v_t}(t)-\mathrm{rad}(\hat v_t)\ \ge\ \hat q_u(t)+\mathrm{rad}(u)-\varepsilon$$
     holds, output $\hat v_t$ and stop.
   - Truncation: on reaching $t=m$, output $\hat v_m$ and stop.

### Implementation notes

- The update per sample is $O(1)$. Checking the stopping condition is $O(L)$, so it may be checked every $L$ samples instead of every time, or only at geometrically spaced $t$ (stopping is delayed by at most that interval).
- Space is $O(L)=O(1/\varepsilon')\le O(1/\varepsilon)$ for the counts of $C$.
- The checks in steps 1 and 2 require no sampling.

## 4. Proof that the failure probability is at most $\delta$

Define the following 3 events.

- $F$: $v^*\in C$.
- $G$: for all $v\in C$ and all $t\ge1$, $|\hat p_v(t)-p_v|\le\rho_t$.
- $H$: for all $v\in C$, $|\hat p_v(m)-p_v|\le\varepsilon'/2$.

In the units of $\bar a$, under $G$ we have $|\hat q_v(t)-\bar p_v|\le\bar\rho_t$, and under $H$ we have $|\hat q_v(m)-\bar p_v|\le\varepsilon/2$ ($v\in C$); for $v_0$, $\hat q_{v_0}=\bar p_{v_0}$ always holds.

### Lemma 1

If $p_{(1)}\ge\varepsilon'$, then $P(F^c)\le e^{-k\varepsilon'/8}\le\delta_1$.

Proof: by 1.1, the number of occurrences $c^*$ of $v^*$ in stage 1 is Bin$(k,p_{(1)})$, with $\mu=kp_{(1)}\ge k\varepsilon'$. By 1.5, $c^*>k/(l+1)$ implies $v^*\in C$, and $l+1>2/\varepsilon'$ gives $k/(l+1)<k\varepsilon'/2\le\mu/2$. Hence
$$P(F^c)\le P(c^*\le\mu/2)\le e^{-\mu/8}\le e^{-k\varepsilon'/8}$$
(applying 1.3 with $\eta=1/2$). Since $k\ge\frac8{\varepsilon'}\ln\frac1{\delta_1}$, this is at most $\delta_1$. $\square$

### Lemma 2

$P(G^c)\le\delta_2$.

Proof: the samples of stage 2 are independent of stage 1, so we may treat $C$ as fixed. For fixed $v\in C$ and $t$, by 1.2 (two-sided),
$$P(|\hat p_v(t)-p_v|>\rho_t)\le2e^{-2t\rho_t^2}=\frac{\delta_2}{2Lt^2}.$$
By 1.4, summing over $t$ gives $<\delta_2/L$, and summing further over $v\in C$ gives $\le\delta_2$. $\square$

### Lemma 3

$P(H^c)\le\delta_3$.

Proof: for fixed $v\in C$, by 1.2, $P(|\hat p_v(m)-p_v|>\varepsilon'/2)\le2e^{-m\varepsilon'^2/2}$. Summing over the $L$ values gives $2Le^{-m\varepsilon'^2/2}\le\delta_3$ (since $m\ge\frac2{\varepsilon'^2}\ln\frac{2L}{\delta_3}$). $\square$

### Lemma 4 (output of steps 1, 2)

When $v_0$ is output in step 1 or step 2, $\bar p_{v_0}\ge\bar p_{(1)}-\varepsilon$ holds under $F$ if $p_{(1)}\ge\varepsilon'$, and unconditionally otherwise.

Proof: in step 1, every value $v$ of $a$ has $\bar p_v\le n/\bar n\le r/\bar n+\varepsilon$. When $C=\emptyset$ in step 2, $p_{(1)}\ge\varepsilon'$ would contradict $F$, so $p_{(1)}<\varepsilon'$, and hence every value of $a$ has $\bar p_v<\varepsilon$. If $\bar p_{(1)}=r/\bar n$ the claim is trivial; otherwise $\bar p_{(1)}<\varepsilon\le\bar p_{v_0}+\varepsilon$. $\square$

### Lemma 5 (the mode of $\bar a$ is in the candidate set)

$\bar v^*\in C\cup\{v_0\}$ holds always if $\bar v^*=v_0$, and under $F$ if $\bar v^*\ne v_0$ and $p_{(1)}\ge\varepsilon'$. If $\bar v^*\ne v_0$ and $p_{(1)}<\varepsilon'$, then $\bar p_{(1)}<\varepsilon$, and any output satisfies the guarantee.

Proof: if $\bar v^*\ne v_0$, then $\bar v^*$ is also a mode of $a$, so it can be taken as $v^*$. If $p_{(1)}<\varepsilon'$, then $\bar p_{(1)}=\frac n{\bar n}p_{(1)}<\varepsilon$. $\square$

### Lemma 6 (correctness of early stopping)

If $\bar v^*\in C\cup\{v_0\}$, then under $G$ the output $\hat v$ of early stopping satisfies $\bar p_{\hat v}\ge\bar p_{(1)}-\varepsilon$.

Proof: let $t$ be the stopping time. For any $u\in C\cup\{v_0\}$, by $G$ ($u\in C$) or the equality ($u=v_0$) and the stopping condition,
$$\bar p_u\le\hat q_u(t)+\mathrm{rad}(u)\le\hat q_{\hat v}(t)-\mathrm{rad}(\hat v)+\varepsilon\le\bar p_{\hat v}+\varepsilon$$
(trivial when $u=\hat v$). Take $u=\bar v^*$. $\square$

### Lemma 7 (correctness of truncation)

If $\bar v^*\in C\cup\{v_0\}$, then under $H$ the output $\hat v_m$ of truncation satisfies $\bar p_{\hat v_m}\ge\bar p_{(1)}-\varepsilon$.

Proof: under $H$, $|\hat q_v(m)-\bar p_v|\le\varepsilon/2$ for all $v\in C\cup\{v_0\}$. Hence
$$\bar p_{\hat v_m}\ge\hat q_{\hat v_m}(m)-\frac\varepsilon2\ge\hat q_{\bar v^*}(m)-\frac\varepsilon2\ge\bar p_{(1)}-\varepsilon.\ \square$$

### Theorem

The probability that the output does not satisfy $\bar p_v\ge\bar p_{(1)}-\varepsilon$ is at most $\delta$.

Proof: if the algorithm stops in step 1 or 2, apply Lemma 4. If it proceeds to step 3, by Lemma 5 either $\bar v^*\in C\cup\{v_0\}$ holds ($F$ is needed only when $\bar v^*\ne v_0$ and $p_{(1)}\ge\varepsilon'$) or any output is correct; in the former case, by Lemmas 6 and 7 the output is correct under $G\cap H$. Hence the failure event is contained in the union of $F^c$ (relevant only when $p_{(1)}\ge\varepsilon'$), $G^c$, and $H^c$, and by Lemmas 1–3 its probability is at most $\delta_1+\delta_2+\delta_3=\delta$. $\square$

## 5. Number of samples

### Worst case

$k+m$. Since $\varepsilon'=\varepsilon\bar n/n$, the larger $r$ is, the more $k,l$ shrink by a factor of $n/\bar n$ and $m$ by a factor of $(n/\bar n)^2$.

### Guarantee of early stopping (adaptivity)

Let $\bar\Delta$ be the gap between the top 2 values within the candidate set $C\cup\{v_0\}$, in the units of $\bar a$ (if the set has 1 element, the stopping condition is vacuous and it stops immediately). With $\gamma=\frac{\bar n}{n}\cdot\frac{\max(\varepsilon,\bar\Delta)}4$, if $\bar v^*\in C\cup\{v_0\}$, then under $G$ the algorithm stops early by the first time $T_0$ at which $\rho_t\le\gamma$ (i.e., $\bar\rho_t\le\max(\varepsilon,\bar\Delta)/4$).

Proof: write $\bar\rho=\bar\rho_t$. The radius of every candidate is at most $\bar\rho$.
If $4\bar\rho\le\varepsilon$, then for any $u\ne\hat v_t$,
$$\hat q_{\hat v_t}(t)-\mathrm{rad}(\hat v_t)\ge\hat q_{\bar v^*}(t)-\bar\rho\ge\bar p_{(1)}-2\bar\rho\ge\bar p_u+2\bar\rho-\varepsilon\ge\hat q_u(t)+\mathrm{rad}(u)-\varepsilon$$
(the third inequality uses $\bar p_u\le\bar p_{(1)}$ and $4\bar\rho\le\varepsilon$).
If $\varepsilon<\bar\Delta$ and $4\bar\rho\le\bar\Delta$, then since $2\bar\rho<\bar\Delta$, $\hat q_{\bar v^*}(t)\ge\bar p_{(1)}-\bar\rho>\bar p_u+\bar\rho\ge\hat q_u(t)$ ($u\ne\bar v^*$), so $\hat v_t=\bar v^*$. Then for $u\ne\bar v^*$,
$$\hat q_{\bar v^*}(t)-\mathrm{rad}(\bar v^*)\ge\bar p_{(1)}-2\bar\rho\ge\bar p_u+\bar\Delta-2\bar\rho\ge\bar p_u+2\bar\rho-\varepsilon\ge\hat q_u(t)+\mathrm{rad}(u)-\varepsilon$$
(the third inequality uses $4\bar\rho\le\bar\Delta<\bar\Delta+\varepsilon$). $\square$

An explicit upper bound on $T_0$ is
$$T_0\le\max\Big\{1,\ \Big\lceil\frac1{\gamma^2}\ln\frac{4L}{\delta_2\gamma^4}\Big\rceil\Big\}$$
(that $\ln(4Lt^2/\delta_2)\le2\gamma^2t$ at this $t$ reduces, with $A=\ln(4L/\delta_2)$ and $\Gamma=\ln(1/\gamma^2)$, to $A+2\Gamma\ge2\ln(A+2\Gamma)$, which follows from $x\ge2\ln x$. If $A+2\Gamma\le0$, then $\rho_1\le\gamma$ and $T_0=1$).

Since the radius of $v_0$ is 0, when $v_0$ is a clear mode the algorithm stops earlier than this bound (in comparisons involving $v_0$, the radius appears on only one side).

### Numerical example ($\varepsilon=0.1$, $\delta=0.01$)

| | $\varepsilon'$ | $k$ | $l$ | $m$ ($L=l$) | Worst case $k+m$ |
|---|---|---|---|---|---|
| $r=0$ | $0.1$ | $457$ | $20$ | $1{,}879$ | $2{,}336$ |
| $r=0.3\bar n$ | $0.143$ | $320$ | $14$ | $886$ | $1{,}206$ |

Upper bound $T_0$ on early stopping at $r=0$: about $7{,}500$ for $\bar\Delta\le0.1$ (truncation at $m$ comes first), about $1{,}200$ for $\bar\Delta=0.5$, and about $410$ for $\bar\Delta=0.8$.

- The $T_0$ above is an upper bound as a guarantee, and actual stopping is often earlier. However, in terms of the guarantee, early stopping beats truncation only when the gap $\bar\Delta$ is at least several times $\varepsilon$. For $\bar\Delta\le\varepsilon$, truncation comes first, and the cost is the same as the fixed-sample method.
- The constant in $\rho_t$ can be improved by confidence intervals that use a $\ln\ln t$-type term instead of $\ln t^2$ (law of the iterated logarithm type confidence sequences) or by empirical Bernstein-type inequalities, which use the variance. This document prioritizes simplicity of the proof.

## 6. Remarks

- The case $r=0$ (nothing removed): $\varepsilon'=\varepsilon$, step 1 never fires, and the candidate set is $C$ alone. If $C=\emptyset$ in step 2, output any element of $a$ (as in Lemma 4, in this case either $p_{(1)}<\varepsilon$ or $F^c$).
- The cost $k$ of stage 1 is only of order $1/\varepsilon'$ because it requires only a multiplicative condition on the number of occurrences of $v^*$, namely "at least half of its expectation" (1.3). $1/\varepsilon'^2$ is needed only for the additive-accuracy decision in stage 2.
- Virtually restoring the removed values: drawing an index uniformly from $[0,\bar n)$ and returning the element of $a$ at that index if it is less than $n$, or $v_0$ if it is at least $n$, gives uniform sampling with replacement from $\bar a$, so the algorithm for $r=0$ can be used unchanged. However, this re-estimates the already known $\bar p_{v_0}$, and $m$ also grows by a factor of $(\bar n/n)^2$. It is meaningful only as an alternative when one does not want to change the implementation.
- What the output satisfies is $\bar p_v\ge\bar p_{(1)}-\varepsilon$; agreement with the true mode is guaranteed only when $\bar\Delta\ge\varepsilon$. If the exact mode is needed, narrow the candidates to $C\cup\{v_0\}$ with this algorithm, then scan all of $a$ to count each value of $C$ exactly and compare with $r$ ($C$ is small, so the counting is easy to vectorize with SIMD).
