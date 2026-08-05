import numpy as np
import matplotlib.pyplot as plt
from scipy.stats import beta

# ---------------- 环境 ----------------
class BernoulliBandit:
    def __init__(self, K, rng):
        self.probs = rng.uniform(size=K)
        self.best_idx = self.probs.argmax()
        self.best_prob = self.probs[self.best_idx]
        self.K = K
    def step(self, k, rng):
        return int(rng.random() < self.probs[k])

# ---------------- 求解器基类 ----------------
class Solver:
    def __init__(self, bandit):
        self.b = bandit
        self.counts = np.zeros(bandit.K)
        self.est = np.ones(bandit.K)          # 乐观初始化 1.0（书里的做法）
        self.regret = 0.0
        self.regrets = []
    def run(self, T, rng):
        for t in range(1, T + 1):
            k = self.select(t, rng)
            r = self.b.step(k, rng)
            self.counts[k] += 1
            self.est[k] += (r - self.est[k]) / self.counts[k]
            self.update(k, r)
            self.regret += self.b.best_prob - self.b.probs[k]
            self.regrets.append(self.regret)
    def update(self, k, r): pass

class EpsilonGreedy(Solver):
    def __init__(self, bandit, eps=0.01):
        super().__init__(bandit); self.eps = eps
    def select(self, t, rng):
        return rng.integers(self.b.K) if rng.random() < self.eps else self.est.argmax()

class DecayingEpsilonGreedy(Solver):
    def select(self, t, rng):
        return rng.integers(self.b.K) if rng.random() < 1.0 / t else self.est.argmax()

class UCB(Solver):
    def __init__(self, bandit, coef=1.0):
        super().__init__(bandit); self.c = coef
    def select(self, t, rng):
        ucb = self.est + self.c * np.sqrt(np.log(t) / (2 * (self.counts + 1)))
        return ucb.argmax()

class ThompsonSampling(Solver):
    def __init__(self, bandit):
        super().__init__(bandit)
        self.a = np.ones(bandit.K); self.bb = np.ones(bandit.K)
    def select(self, t, rng):
        return rng.beta(self.a, self.bb).argmax()
    def update(self, k, r):
        self.a[k] += r; self.bb[k] += 1 - r

# ① 顶部冷却的 Thompson：对后验采样加温度 α>1（拉尖后验）。
#    Beta(a,b) 改为采样 Beta(a·α, b·α)，方差 ~1/α，样本更集中于均值，减少无谓探索。
#    α→∞ 退化为贪心，α=1 即 vanilla TS。
class ColdTS(Solver):
    def __init__(self, bandit, temp=3.0):
        super().__init__(bandit)
        self.a = np.ones(bandit.K); self.bb = np.ones(bandit.K)
        self.temp = temp
    def select(self, t, rng):
        samp = rng.beta(self.a * self.temp, self.bb * self.temp)
        return samp.argmax()
    def update(self, k, r):
        self.a[k] += r; self.bb[k] += 1 - r

# ①b 温度退火版 Thompson：α 随 t 线性升温，1 → max_temp。
#    前期 α≈1 保持 vanilla TS 的充分探索，后期后验拉尖、收敛到贪心，
#    兼顾初段探索与末段利用。max_temp 即"最终冷却强度"。
class AnnealTS(ColdTS):
    def __init__(self, bandit, T, max_temp=6.0):
        super().__init__(bandit, 1.0)
        self.horizon = T
        self.max_temp = max_temp
    def select(self, t, rng):
        a = 1.0 + (self.max_temp - 1.0) * (t / self.horizon)   # α: 1 → max_temp
        s = rng.beta(self.a * a, self.bb * a)
        return s.argmax()

# ② 乐观初始化贪心：est 初值 1.0（基类已有），未拉过的臂优先。
#    tie-break：同 est 时 counts 小的臂优先 → 未失败(est 仍=1.0)的臂轮流被拉，
#    等价于"每臂先试到首次失败"再进入贪心。近乎零参数。
class OptimisticGreedy(Solver):
    def select(self, t, rng):
        order = np.lexsort((self.counts, -self.est))  # est 降序主键，counts 升序次键
        return order[0]

# ③ 有限时域正解的实用近似 Bayes-UCB：取后验 Beta 的高分位数做 index。
#    score_i = Q(1 - 1/(t (log T)^c); Beta(a_i,b_i))，常数自动贴合先验，
#    免去 UCB(coef) 对 Hoeffding 界系数的调参。
class BayesUCB(Solver):
    def __init__(self, bandit, T, c=1.0):
        super().__init__(bandit)
        self.a = np.ones(bandit.K); self.bb = np.ones(bandit.K)
        self.horizon = T; self.c = c
    def select(self, t, rng):
        q = 1 - 1.0 / (t * np.log(self.horizon) ** self.c)
        return beta.ppf(q, self.a, self.bb).argmax()
    def update(self, k, r):
        self.a[k] += r; self.bb[k] += 1 - r

# ④ 后验均值 + 后验标准差（Bayesian 单侧 1σ 乐观）。
#    σ = sqrt(a·b/((a+b)²(a+b+1)))。零参数：σ 由均匀先验 Beta(1,1) 共轭更新给出。
#    不自信的臂带一点乐观，自信的臂纯均值；早期失败的好臂不会像纯贪心那样被永久丢弃。
class PosteriorStd(Solver):
    def __init__(self, bandit):
        super().__init__(bandit)
        self.a = np.ones(bandit.K); self.bb = np.ones(bandit.K)
    def select(self, t, rng):
        n = self.a + self.bb
        m = self.a / n
        sigma = np.sqrt(self.a * self.bb / (n * n * (n + 1)))
        return (m + sigma).argmax()
    def update(self, k, r):
        self.a[k] += r; self.bb[k] += 1 - r

# ⑤ 乐观初始化 + 后验标准差：est 用 1.0 乐观初值（基类），再附加 σ。
#    未失败臂之间按"更有把握者优先"（σ 更小）打破平局，比纯 counts 平局更稳。
class OptStd(Solver):
    def __init__(self, bandit):
        super().__init__(bandit)
        self.a = np.ones(bandit.K); self.bb = np.ones(bandit.K)
    def select(self, t, rng):
        n = self.a + self.bb
        sigma = np.sqrt(self.a * self.bb / (n * n * (n + 1)))
        return (self.est + sigma).argmax()
    def update(self, k, r):
        self.a[k] += r; self.bb[k] += 1 - r

# ⑥ 期望改进 EI：score_i = E[(p_i - m_best)⁺]，后验 Beta 下闭式可算。
#    只对有"可能超过当前最优"的臂探索，且随最优变强而自然停止。零参数。
class ExpectedImprovement(Solver):
    def __init__(self, bandit):
        super().__init__(bandit)
        self.a = np.ones(bandit.K); self.bb = np.ones(bandit.K)
    def select(self, t, rng):
        n = self.a + self.bb
        m = self.a / n
        mbest = m.max()
        # EI = (a/n)·[1-F_{a+1,b}(mbest)] - mbest·[1-F_{a,b}(mbest)]
        ei = (self.a / n) * (1 - beta.cdf(mbest, self.a + 1, self.bb)) \
             - mbest * (1 - beta.cdf(mbest, self.a, self.bb))
        return ei.argmax()
    def update(self, k, r):
        self.a[k] += r; self.bb[k] += 1 - r

# ⑦ 贪心混合 TS：以 1-K/T 走后验均值最优，以 K/T 走后验采样 argmax。
#    探索预算 = K 次（每臂平均 1 次），用后验采样而非均匀随机，集中在有希望的臂。
class GreedyMixTS(Solver):
    def __init__(self, bandit, T):
        super().__init__(bandit)
        self.a = np.ones(bandit.K); self.bb = np.ones(bandit.K)
        self.eps = bandit.K / T
    def select(self, t, rng):
        if rng.random() < self.eps:
            return rng.beta(self.a, self.bb).argmax()
        return (self.a / (self.a + self.bb)).argmax()
    def update(self, k, r):
        self.a[k] += r; self.bb[k] += 1 - r

# ⑧ 方差感知 UCB（经验 Bernstein）：bonus = sqrt(2·m(1-m)·logT/n) + 3·logT/n。
#    唯一"参数"是 T（问题给定）。方差小的臂几乎不探索。
class EmpBernUCB(Solver):
    def __init__(self, bandit, T):
        super().__init__(bandit)
        self.a = np.ones(bandit.K); self.bb = np.ones(bandit.K)
        self.horizon = T
    def select(self, t, rng):
        n = self.a + self.bb
        m = self.a / n
        lT = np.log(self.horizon)
        score = m + np.sqrt(2 * m * (1 - m) * lT / n) + 3 * lT / n
        return score.argmax()
    def update(self, k, r):
        self.a[k] += r; self.bb[k] += 1 - r

# ⑨ EI 的乐观参考版：把"当前均值最优"换成"最优臂后验的 0.9 分位数"作参考。
#    参考更高 → 各臂 EI 更小 → 只探索真正可能反超的臂，尾部更稳。
#    常数 0.9 是固定的设计选择（非对问题调参）。
class EIQuantile(Solver):
    def __init__(self, bandit):
        super().__init__(bandit)
        self.a = np.ones(bandit.K); self.bb = np.ones(bandit.K)
    def select(self, t, rng):
        n = self.a + self.bb
        m = self.a / n
        j = m.argmax()
        ref = beta.ppf(0.9, self.a[j], self.bb[j])   # 最优臂的乐观分位作参考
        ei = (self.a / n) * (1 - beta.cdf(ref, self.a + 1, self.bb)) \
             - ref * (1 - beta.cdf(ref, self.a, self.bb))
        return ei.argmax()
    def update(self, k, r):
        self.a[k] += r; self.bb[k] += 1 - r

# ⑩ PosteriorStd 的 2σ 版：更强乐观，探索更多 → 尾部更稳。
class PosteriorStd2(Solver):
    def __init__(self, bandit):
        super().__init__(bandit)
        self.a = np.ones(bandit.K); self.bb = np.ones(bandit.K)
    def select(self, t, rng):
        n = self.a + self.bb
        m = self.a / n
        sigma = np.sqrt(self.a * self.bb / (n * n * (n + 1)))
        return (m + 2 * sigma).argmax()
    def update(self, k, r):
        self.a[k] += r; self.bb[k] += 1 - r

# ⑪ EI + 后验标准差：在"改进期望"上加"不确定性"作为探索垫。
#    与 EI 同量纲自然相加，零新参数；比纯 EI 多点探索、比纯 σ 多点利用。
class EIplusSigma(Solver):
    def __init__(self, bandit):
        super().__init__(bandit)
        self.a = np.ones(bandit.K); self.bb = np.ones(bandit.K)
    def select(self, t, rng):
        n = self.a + self.bb
        m = self.a / n
        mbest = m.max()
        ei = (self.a / n) * (1 - beta.cdf(mbest, self.a + 1, self.bb)) \
             - mbest * (1 - beta.cdf(mbest, self.a, self.bb))
        sigma = np.sqrt(self.a * self.bb / (n * n * (n + 1)))
        return (ei + sigma).argmax()
    def update(self, k, r):
        self.a[k] += r; self.bb[k] += 1 - r

# ⑫ EIQuantile 的 0.95 版：参考更高 → 探索更克制，可能更低 mean / 更低方差。
class EIQuantile95(EIQuantile):
    def select(self, t, rng):
        n = self.a + self.bb
        m = self.a / n
        j = m.argmax()
        ref = beta.ppf(0.95, self.a[j], self.bb[j])
        ei = (self.a / n) * (1 - beta.cdf(ref, self.a + 1, self.bb)) \
             - ref * (1 - beta.cdf(ref, self.a, self.bb))
        return ei.argmax()

# ⑬ EIQuantile + 首轮覆盖：前 K 步每臂各拉一次（确定性，零参数）。
#    保证没有任何臂在开局被饿死，之后进入 EIQuantile 决策。
class EIQuantileWarmup(EIQuantile):
    def __init__(self, bandit):
        super().__init__(bandit)
        self.pending = list(range(bandit.K))
    def select(self, t, rng):
        if self.pending:
            return self.pending.pop()
        return super().select(t, rng)

# ⑭ EIQuantile + 剪枝：m + 3σ < ref 的臂视为无望，EI 置 0。
#    只对能反超参考的臂花探索，其余直接放弃 → 减少废拉、稳方差。
class EIQuantilePrune(EIQuantile):
    def select(self, t, rng):
        n = self.a + self.bb
        m = self.a / n
        j = m.argmax()
        ref = beta.ppf(0.9, self.a[j], self.bb[j])
        sigma = np.sqrt(self.a * self.bb / (n * n * (n + 1)))
        ei = (self.a / n) * (1 - beta.cdf(ref, self.a + 1, self.bb)) \
             - ref * (1 - beta.cdf(ref, self.a, self.bb))
        ei = np.where(m + 3 * sigma < ref, 0.0, ei)   # 无望臂不探索
        return ei.argmax()

# ⑮ EIQuantile 的参考动态收紧版：参考 = 最优臂 Beta 的 (1 - 1/t) 分位。
#    前期 ref 高（宽松）→ 各臂 EI 都非零，充分探索；随 t 增长 ref→1（收紧）→ 只有最优臂有 EI，贪心利用。
#    零参数：时间衰减量 1/t 与 ε_t=1/t 同源（"剩下的探索量≈1/t"）。
class EIDyn(Solver):
    def __init__(self, bandit):
        super().__init__(bandit)
        self.a = np.ones(bandit.K); self.bb = np.ones(bandit.K)
    def select(self, t, rng):
        n = self.a + self.bb
        m = self.a / n
        j = m.argmax()
        ref = beta.ppf(1 - 1.0 / t, self.a[j], self.bb[j])   # 随 t 收紧
        ei = (self.a / n) * (1 - beta.cdf(ref, self.a + 1, self.bb)) \
             - ref * (1 - beta.cdf(ref, self.a, self.bb))
        return ei.argmax()
    def update(self, k, r):
        self.a[k] += r; self.bb[k] += 1 - r

# ⑯ EIDyn 的剩余时域锚定版：ref 用 (T-t)/T 的倒数收紧，即 ref = Q(1 - (T-t)/T, ...)。
#    剩余时间越少 → 越贪心。t 小时探索最多，t→T 收敛到纯利用。
class EIResidual(EIDyn):
    def __init__(self, bandit, T):
        super().__init__(bandit)
        self.horizon = T
    def select(self, t, rng):
        n = self.a + self.bb
        m = self.a / n
        j = m.argmax()
        ref = beta.ppf(1 - (self.horizon - t) / self.horizon, self.a[j], self.bb[j])
        ei = (self.a / n) * (1 - beta.cdf(ref, self.a + 1, self.bb)) \
             - ref * (1 - beta.cdf(ref, self.a, self.bb))
        return ei.argmax()

# ⑰ EIDyn 在引用臂上改用固定 0.9（若 1-1/t<0.9 则用 0.9），其他臂用动态 ref。
#    组合"常温和"动态"，下限封底避免 ref 过松。
class EIDynFloor(EIDyn):
    def select(self, t, rng):
        n = self.a + self.bb
        m = self.a / n
        j = m.argmax()
        ref = beta.ppf(max(0.9, 1 - 1.0 / t), self.a[j], self.bb[j])
        ei = (self.a / n) * (1 - beta.cdf(ref, self.a + 1, self.bb)) \
             - ref * (1 - beta.cdf(ref, self.a, self.bb))
        return ei.argmax()

# ⑱ EIQuantile 的自适应参考：ref 的分位随最优领先幅度自适应。
#    最优臂第 2 名差距大（领先稳）→ 更高分位，更贪心；领先胶着 → 低分位，多探索。
#    q = sigmoid(d/σ_j)，d = m_j - m_{j2}，σ_j 是最优臂后验标准差。零参数。
class EIAdapt(Solver):
    def __init__(self, bandit):
        super().__init__(bandit)
        self.a = np.ones(bandit.K); self.bb = np.ones(bandit.K)
    def select(self, t, rng):
        n = self.a + self.bb
        m = self.a / n
        j = m.argmax()
        sigma_j = np.sqrt(self.a[j] * self.bb[j] / (n[j] * n[j] * (n[j] + 1)))
        second = np.partition(m, -2)[-2]                      # 第 2 大均值
        d = m[j] - second
        q = 1.0 / (1.0 + np.exp(-(d / max(sigma_j, 1e-9))))  # → 领先越稳，q 越高
        ref = beta.ppf(q, self.a[j], self.bb[j])
        ei = (self.a / n) * (1 - beta.cdf(ref, self.a + 1, self.bb)) \
             - ref * (1 - beta.cdf(ref, self.a, self.bb))
        return ei.argmax()
    def update(self, k, r):
        self.a[k] += r; self.bb[k] += 1 - r

# ⑲ EIAdapt 的区间夹取版：q 夹在 [0.85, 0.95] 之间，避免过松/过紧。
class EIAdaptClipped(EIAdapt):
    def select(self, t, rng):
        n = self.a + self.bb
        m = self.a / n
        j = m.argmax()
        sigma_j = np.sqrt(self.a[j] * self.bb[j] / (n[j] * n[j] * (n[j] + 1)))
        second = np.partition(m, -2)[-2]
        d = m[j] - second
        q = 1.0 / (1.0 + np.exp(-(d / max(sigma_j, 1e-9))))
        q = np.clip(q, 0.85, 0.95)
        ref = beta.ppf(q, self.a[j], self.bb[j])
        ei = (self.a / n) * (1 - beta.cdf(ref, self.a + 1, self.bb)) \
             - ref * (1 - beta.cdf(ref, self.a, self.bb))
        return ei.argmax()

# ⑳ EI 的非对称参考：当前最优臂参考 0.9（保持贪心），其余臂参考 0.7（更宽松→多探索）。
#    修复 EI 的根本弱点：最优臂从不自我探索，早期错判的最优臂永远不反悔。
#    零参数（常数 0.9/0.7 是固定设计）。
class EIASym(Solver):
    def __init__(self, bandit):
        super().__init__(bandit)
        self.a = np.ones(bandit.K); self.bb = np.ones(bandit.K)
    def select(self, t, rng):
        n = self.a + self.bb
        m = self.a / n
        j = m.argmax()
        refs = np.where(np.arange(n.size) == j, 0.9, 0.7)
        qs = beta.ppf(refs, self.a, self.bb)                 # 每臂各自的参考
        ei = (self.a / n) * (1 - beta.cdf(qs, self.a + 1, self.bb)) \
             - qs * (1 - beta.cdf(qs, self.a, self.bb))
        return ei.argmax()
    def update(self, k, r):
        self.a[k] += r; self.bb[k] += 1 - r

# ㉑ EIASym 的夹取版：非最优臂参考夹 [0.7, 0.85]，避免极端宽松。
class EIASymClipped(EIASym):
    def select(self, t, rng):
        n = self.a + self.bb
        m = self.a / n
        j = m.argmax()
        refs = np.where(np.arange(n.size) == j, 0.9,
                        np.clip(0.7 + (m - m.min()) * 0.1, 0.7, 0.85))
        qs = beta.ppf(refs, self.a, self.bb)
        ei = (self.a / n) * (1 - beta.cdf(qs, self.a + 1, self.bb)) \
             - qs * (1 - beta.cdf(qs, self.a, self.bb))
        return ei.argmax()

# ㉒ EI 的"前二参考"版：EI 对最优和第二名之间的 gap 也评分。
#    若某臂有希望反超第二名（challenger），也给它探索 → 对"最优臂是错"的情况更稳。
class EIChallenger(Solver):
    def __init__(self, bandit):
        super().__init__(bandit)
        self.a = np.ones(bandit.K); self.bb = np.ones(bandit.K)
    def select(self, t, rng):
        n = self.a + self.bb
        m = self.a / n
        top2 = np.partition(m, -2)[-2]
        ref = top2 + (m.max() - top2) * 0.5               # 介于第 2 与最优之间
        ei = (self.a / n) * (1 - beta.cdf(ref, self.a + 1, self.bb)) \
             - ref * (1 - beta.cdf(ref, self.a, self.bb))
        return ei.argmax()
    def update(self, k, r):
        self.a[k] += r; self.bb[k] += 1 - r

# ㉓ EIQuantile 的分层先验版：后验由 0.1/(N+p) + N̂/(N+p) 构成，
#    N̂ = E[独立臂均值] 的收缩估计（均值还原），p = 均值还原强度。零参数（p=1 固定）。
#    把每个臂的后验计数向全局均值收缩：a'_i = a_i + p·ḡ，b'_i = b_i + p·(1−ḡ)。
#    减少开局噪声对后验的污染 → 探索决策更准，方差更稳。
class EIQuantileShrink(Solver):
    def __init__(self, bandit):
        super().__init__(bandit)
        self.a = np.ones(bandit.K); self.bb = np.ones(bandit.K)
    def select(self, t, rng):
        n = self.a + self.bb
        m = self.a / n
        g = m.mean()                                     # 全局均值
        p = 1.0                                          # 收缩强度（固定）
        as_ = self.a + p * g
        bs = self.bb + p * (1 - g)
        j = m.argmax()
        ref = beta.ppf(0.9, self.a[j], self.bb[j])
        ns = as_ + bs
        ei = (as_ / ns) * (1 - beta.cdf(ref, as_ + 1, bs)) \
             - ref * (1 - beta.cdf(ref, as_, bs))
        return ei.argmax()
    def update(self, k, r):
        self.a[k] += r; self.bb[k] += 1 - r

# ㉔ EIQuantile 的强制首轮版：前 K 步各臂强制拉一次（确定性）。
#    保证任何臂都不会开局被饿死 → 收敛更稳，方差更低。
class EIQuantileTour(EIQuantile):
    def __init__(self, bandit):
        super().__init__(bandit)
        self.pending = list(range(bandit.K))
    def select(self, t, rng):
        if self.pending:
            return self.pending.pop()
        return super().select(t, rng)

# ㉕ EIQuantile + 最优臂概率下界：如果当前最优臂胜率极高（后验最优概率>0.99），
#    则完全停止探索、纯贪心利用（末期快速收敛）。
class EIQuantileStop(EIQuantile):
    def select(self, t, rng):
        n = self.a + self.bb
        m = self.a / n
        j = m.argmax()
        # 最优臂后验胜率下界：用其 0.05 分位 > 次优 0.95 分位 近似
        second = np.partition(m, -2)[-2]
        i2 = np.where(m == second)[0][0]
        if beta.ppf(0.05, self.a[j], self.bb[j]) > beta.ppf(0.95, self.a[i2], self.bb[i2]):
            return j
        return super().select(t, rng)

# ---------------- 多实例多种子平均 ----------------
K, T, N_RUNS = 20, 400, 500

algos = {
    'eps=0.01 (fixed)':   lambda b: EpsilonGreedy(b, 0.01),
    'eps_t = 1/t':        lambda b: DecayingEpsilonGreedy(b),
    'UCB coef=1.0':       lambda b: UCB(b, 1.0),
    'UCB coef=0.2':       lambda b: UCB(b, 0.2),
    'Thompson':           lambda b: ThompsonSampling(b),
    'ColdTS α=3':         lambda b: ColdTS(b, 3.0),
    'AnnealTS 1→6':       lambda b: AnnealTS(b, T),
    'OptimisticGreedy':   lambda b: OptimisticGreedy(b),
    'BayesUCB':           lambda b: BayesUCB(b, T),
    'PosteriorStd':       lambda b: PosteriorStd(b),
    'OptStd':             lambda b: OptStd(b),
    'ExpectedImprovement':lambda b: ExpectedImprovement(b),
    'EIQuantile':         lambda b: EIQuantile(b),
    'EIQuantileShrink':   lambda b: EIQuantileShrink(b),
}

curves = {name: np.zeros((N_RUNS, T)) for name in algos}
for run in range(N_RUNS):
    rng_env = np.random.default_rng(10000 + run)
    bandit = BernoulliBandit(K, rng_env)          # 每次换一个 bandit 实例
    for name, ctor in algos.items():
        s = ctor(bandit)
        s.run(T, np.random.default_rng(run))      # 同 run 内各算法同种子，配对比较
        curves[name][run] = s.regrets

print(f"{'algorithm':<20}{'mean':>8}{'median':>8}{'p90':>8}{'max':>8}{'std':>8}")
for name, c in curves.items():
    f = c[:, -1]
    print(f"{name:<20}{f.mean():8.1f}{np.median(f):8.1f}"
          f"{np.percentile(f,90):8.1f}{f.max():8.1f}{f.std():8.1f}")

print("\n按 mean 排序：")
for name, c in sorted(curves.items(), key=lambda kv: kv[1][:, -1].mean()):
    f = c[:, -1]
    print(f"  {name:<20} mean={f.mean():7.1f}  std={f.std():6.1f}  max={f.max():7.1f}")

# ---------------- 画图 ----------------
plt.figure(figsize=(11, 4))
plt.subplot(1, 2, 1)
for name, c in curves.items():
    m = c.mean(0)
    plt.plot(m, label=name)
    plt.fill_between(range(T), np.percentile(c, 10, 0), np.percentile(c, 90, 0), alpha=.12)
plt.xlabel('t'); plt.ylabel('cumulative regret'); plt.legend(); plt.title(f'mean over {N_RUNS} instances')

plt.subplot(1, 2, 2)
plt.boxplot([curves[n][:, -1] for n in algos], tick_labels=list(algos))
plt.xticks(rotation=30, ha='right'); plt.ylabel('final regret'); plt.title('tail risk')
plt.tight_layout(); plt.show()
