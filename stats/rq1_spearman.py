"""RQ1 - Spearman's rho.

(a) Agreement between measured wall-meter energy and attributed/proxy energy, per app (all 45 runs) and per app x level (15 runs):
      meter net J  vs  M2 total, M1 total, CPU total
(b) Service-level agreement between M1 and M2: Spearman across services within each run, summarised per app.

Outputs: results/rq1_spearman_meter.csv, results/rq1_spearman_m1_m2_runs.csv, results/rq1_spearman_m1_m2_summary.csv
Usage:   python rq1_spearman.py [--boot 2000] [--seed 42]
"""
import argparse

import numpy as np
import pandas as pd
from scipy import stats

from common import bh, load_runs, load_services, save

PREDICTORS = {"m2_total_j": "M2 total", "m1_total_j": "M1 total", "cpu_total_mean": "CPU total"}
LOW_SIGNAL_APPS: set = set()  # Online Boutique now removed by the run-validity rule in common.valid_runs()


def spearman_ci(x, y, boot, rng):
    """Spearman rho, p and percentile bootstrap 95% CI (resampling runs)."""
    x, y = np.asarray(x, float), np.asarray(y, float)
    res = stats.spearmanr(x, y)
    n = len(x)
    rhos = np.empty(boot)
    for i in range(boot):
        idx = rng.integers(0, n, n)
        rhos[i] = stats.spearmanr(x[idx], y[idx]).statistic
    lo, hi = np.nanpercentile(rhos, [2.5, 97.5])
    return res.statistic, res.pvalue, lo, hi


def meter_agreement(runs, boot, rng):
    rows = []
    for scope, keys in (("app", ["app"]), ("app_level", ["app", "level"])):
        for key, g in runs.groupby(keys, observed=True):
            key = key if isinstance(key, tuple) else (key,)
            for col, label in PREDICTORS.items():
                d = g[["net_j", col]].dropna()
                d = d[d[col] > 0] if col != "cpu_total_mean" else d
                if len(d) < 5:
                    continue
                rho, p, lo, hi = spearman_ci(d["net_j"], d[col], boot, rng)
                rows.append(dict(scope=scope, app=key[0], level=key[1] if len(key) > 1 else "all",
                                 predictor=label, n=len(d), rho=rho, ci_low=lo, ci_high=hi, p=p,
                                 low_signal=key[0] in LOW_SIGNAL_APPS))
    out = pd.DataFrame(rows)
    # BH within each scope (family); all apps included
    out["p_bh"] = np.nan
    for scope, idx in out.groupby("scope").groups.items():
        out.loc[idx, "p_bh"] = bh(out.loc[idx, "p"])
    return out


def m1_m2_agreement(svc):
    wide = (svc[svc["model"].isin(["M1", "M2"])]
            .pivot_table(index=["app", "release", "level", "iteration", "service"], columns="model",
                         values="energy_j", aggfunc="sum", observed=True)
            .dropna().reset_index())
    rows = []
    for (app, rel, lvl, it), g in wide.groupby(["app", "release", "level", "iteration"], observed=True):
        if (g["M1"] > 0).sum() < 3:  # M1 has no usable per-service telemetry in this run (e.g. Online Boutique)
            rows.append(dict(app=app, release=rel, level=lvl, iteration=it, n_services=len(g), rho=np.nan, p=np.nan,
                             top1_agree=np.nan))
            continue
        r = stats.spearmanr(g["M1"], g["M2"])
        rows.append(dict(app=app, release=rel, level=lvl, iteration=it, n_services=len(g), rho=r.statistic, p=r.pvalue,
                         top1_agree=g.loc[g["M1"].idxmax(), "service"] == g.loc[g["M2"].idxmax(), "service"]))
    per_run = pd.DataFrame(rows)
    summary = (per_run.groupby("app")
               .agg(runs=("rho", "size"), runs_with_m1=("rho", lambda s: s.notna().sum()),
                    rho_median=("rho", "median"), rho_q1=("rho", lambda s: s.quantile(.25)),
                    rho_q3=("rho", lambda s: s.quantile(.75)), top1_agree_rate=("top1_agree", "mean"))
               .reset_index())
    return per_run, summary


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--boot", type=int, default=2000)
    ap.add_argument("--seed", type=int, default=42)
    a = ap.parse_args()
    rng = np.random.default_rng(a.seed)

    runs, svc = load_runs(), load_services()
    meter = meter_agreement(runs, a.boot, rng)
    per_run, summary = m1_m2_agreement(svc)

    print("Saved:", save(meter, "rq1_spearman_meter.csv"), save(per_run, "rq1_spearman_m1_m2_runs.csv"),
          save(summary, "rq1_spearman_m1_m2_summary.csv"), sep="\n  ")
    pd.set_option("display.width", 160)
    print("\n(a) meter net J vs attributed/proxy energy, per app (n = runs)")
    print(meter[meter.scope == "app"].drop(columns="scope").round(3).to_string(index=False))
    print("\n(b) M1 vs M2 service-level agreement")
    print(summary.round(3).to_string(index=False))


if __name__ == "__main__":
    main()
