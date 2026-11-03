"""RQ2 - Kruskal-Wallis + Benjamini-Hochberg + Cliff's delta.

Two families, each corrected with BH separately (across all cells and responses in the family):
  workload : per app x release, factor = level (low/medium/high)
  release  : per app x level,   factor = release (3 releases, chronological)

Responses
  epr_m2_j        M2-attributed energy per request (J/request)
  m2_total_j      M2-attributed system energy (J)
  net_j           wall-meter net workload energy (J); Online Boutique included but low-signal (~0.1 W)
  dominant_share  per-run M2 share of the app's dominant service (release family only)

Cliff's delta: delta(b vs a) = P(b > a) - P(b < a), with b = higher level / newer release.
95% CI by stratified percentile bootstrap. With n = 5 per group and complete separation (|delta| = 1)
the bootstrap CI degenerates to [1, 1]; such rows are flagged `separated` and should be reported as
"complete separation" rather than as a precise interval.

Outputs: results/rq2_kruskal.csv, results/rq2_cliff.csv
Usage:   python rq2_kruskal_cliff.py [--boot 2000] [--seed 42]
"""
import argparse
import itertools

import numpy as np
import pandas as pd
from scipy import stats

from common import LEVELS, bh, load_runs, load_services, save

LOW_SIGNAL_APPS = {"onlineboutique"}  # informational flag only (net workload power ~0.1 W); not excluded
RESPONSES = {"epr_m2_j": "EPR (M2)", "m2_total_j": "Energy (M2)", "net_j": "Energy (meter)",
             "dominant_share": "Dominant-service share"}
# Kitchenham et al. 2017, Table 5 (Vargha & Delaney / Kraemer & Kupfer)
THRESHOLDS = [(0.112, "negligible"), (0.276, "small"), (0.428, "medium"), (np.inf, "large")]


def magnitude(d):
    return next(lbl for t, lbl in THRESHOLDS if abs(d) < t)


def cliff_delta(a, b, boot, rng):
    """delta(b vs a) and stratified bootstrap 95% CI."""
    a, b = np.asarray(a, float), np.asarray(b, float)
    d = np.sign(b[:, None] - a[None, :]).mean()
    ab = a[rng.integers(0, len(a), (boot, len(a)))]
    bb = b[rng.integers(0, len(b), (boot, len(b)))]
    ds = np.sign(bb[:, :, None] - ab[:, None, :]).mean(axis=(1, 2))
    lo, hi = np.percentile(ds, [2.5, 97.5])
    return d, lo, hi


def kruskal(groups):
    groups = [g for g in groups if len(g) > 0]
    if len(groups) < 2:
        return np.nan, np.nan
    try:
        r = stats.kruskal(*groups)
        return r.statistic, r.pvalue
    except ValueError:  # all values identical
        return 0.0, 1.0


def add_dominant_share(runs, svc):
    m2 = svc[svc["model"] == "M2"].copy()
    m2["share"] = m2["energy_j"] / m2.groupby(["release_folder", "level", "iteration"], observed=True)["energy_j"].transform("sum")
    dominant = m2.groupby(["app", "service"], observed=True)["share"].mean().reset_index() \
                 .sort_values("share").groupby("app").tail(1).set_index("app")["service"]
    m2 = m2[m2["service"] == m2["app"].map(dominant)]
    runs = runs.merge(m2[["release_folder", "level", "iteration", "share"]].rename(columns={"share": "dominant_share"}),
                      on=["release_folder", "level", "iteration"], how="left")
    runs["dominant_service"] = runs["app"].map(dominant)
    return runs


def analyse(runs, family, boot, rng):
    if family == "workload":
        cell_keys, factor, order = ["app", "release"], "level", lambda g: LEVELS
        responses = ["epr_m2_j", "m2_total_j", "net_j"]
    else:
        cell_keys, factor = ["app", "level"], "release"
        order = lambda g: list(g.sort_values("release_order")["release"].unique())
        responses = list(RESPONSES)
    kw_rows, cd_rows = [], []
    for key, g in runs.groupby(cell_keys, observed=True):
        app = key[0]
        for resp in responses:
            levels = order(g)
            data = {lv: g.loc[g[factor] == lv, resp].dropna().to_numpy() for lv in levels}
            if resp in ("epr_m2_j", "m2_total_j", "dominant_share"):
                data = {k: v[v > 0] for k, v in data.items()}  # runs with no attribution output
            h, p = kruskal(list(data.values()))
            base = dict(family=family, app=app, context=str(key[1]), response=RESPONSES[resp])
            kw_rows.append(dict(base, groups=" | ".join(f"{k}:{len(v)}" for k, v in data.items()),
                                H=h, df=sum(len(v) > 0 for v in data.values()) - 1, p=p,
                                **{f"median_{i}": np.median(v) if len(v) else np.nan for i, v in enumerate(data.values())}))
            for lo_lv, hi_lv in itertools.combinations(levels, 2):
                a, b = data[lo_lv], data[hi_lv]
                if len(a) < 2 or len(b) < 2:
                    continue
                d, cl, ch = cliff_delta(a, b, boot, rng)
                cd_rows.append(dict(base, a=lo_lv, b=hi_lv, n_a=len(a), n_b=len(b), delta=d, ci_low=cl, ci_high=ch,
                                    magnitude=magnitude(d), separated=abs(d) == 1))
    kw = pd.DataFrame(kw_rows)
    kw["p_bh"] = bh(kw["p"])
    cd = pd.DataFrame(cd_rows).merge(kw[["family", "app", "context", "response", "p_bh"]].rename(columns={"p_bh": "kw_p_bh"}),
                                     on=["family", "app", "context", "response"], how="left")
    return kw, cd


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--boot", type=int, default=2000)
    ap.add_argument("--seed", type=int, default=42)
    a = ap.parse_args()
    rng = np.random.default_rng(a.seed)

    runs = add_dominant_share(load_runs(), load_services())
    kws, cds = zip(*(analyse(runs, fam, a.boot, rng) for fam in ("workload", "release")))
    kw, cd = pd.concat(kws, ignore_index=True), pd.concat(cds, ignore_index=True)
    print("Saved:", save(kw, "rq2_kruskal.csv"), save(cd, "rq2_cliff.csv"), sep="\n  ")

    pd.set_option("display.width", 180)
    print("\nDominant services:", runs.groupby("app")["dominant_service"].first().to_dict())
    print("\nKruskal-Wallis: cells significant after BH (p_bh < .05) / cells tested")
    print(kw.assign(sig=kw.p_bh < .05).groupby(["family", "response"])["sig"].agg(["sum", "count"]).to_string())
    print("\nCliff's delta magnitudes (b vs a)")
    print(pd.crosstab([cd.family, cd.response], cd.magnitude).to_string())
    wl = cd[(cd.family == "workload") & (cd.a == "low") & (cd.b == "high")]
    print("\nWorkload low -> high: delta per app x release")
    print(wl.pivot_table(index=["app", "context"], columns="response", values="delta").round(2).to_string(na_rep="excl."))
    rel = cd[cd.family == "release"].assign(pair=lambda d: d.a + " -> " + d.b)
    print("\nRelease: delta per app x level x release pair (KW p_bh in kw_p_bh column of rq2_cliff.csv)")
    print(rel.pivot_table(index=["app", "pair"], columns=["response", "context"], values="delta", observed=True)
             .round(2).to_string(na_rep="excl."))


if __name__ == "__main__":
    main()
