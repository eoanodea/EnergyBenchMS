"""Shared loading, analysis-set definition and helpers for the EnergyBenchMS statistical analysis.

Inputs are the tidy tables produced by extract_tidy.py and extract_meter.py.
Analysis set = workstation runs of the 12 release folders used in the paper (EC2 dropped).
Data-quality fixes belong in the pipeline (summarise_run.py, attribution_postprocess.py). The only filter here is the
run-validity rule: a run is invalid if the SUT served no successful requests (see valid_runs()).
"""
from pathlib import Path

import numpy as np
import pandas as pd
from statsmodels.stats.multitest import multipletests

HERE = Path(__file__).resolve().parent
RESULTS = HERE / "results"

# release_folder -> (app, release label, chronological order). Source: final-runs/results_aggregates.csv + paper Table III.
ANALYSIS_SET = {
    "onlineboutique-0.4.2": ("onlineboutique", "v0.4.2", 0),
    "onlineboutique-0.9":   ("onlineboutique", "v0.9.0", 1),
    "onlineboutique":       ("onlineboutique", "v0.10.5", 2),
    "otel-demo-1.12.0":     ("otel-demo", "v1.12.0", 0),
    "otel-demo-2.0.0":      ("otel-demo", "v2.0.0", 1),
    "otel-demo":            ("otel-demo", "v2.2.0", 2),
    "socialnetwork-0.1.0":  ("socialnetwork", "v0.1.0", 0),
    "socialnetwork-0.2.0":  ("socialnetwork", "v0.2.0", 1),
    "socialnetwork":        ("socialnetwork", "v0.4.1", 2),
    "trainticket-0.0.4":    ("trainticket", "v0.0.4", 0),
    "trainticket-0.2.0":    ("trainticket", "v0.2.0", 1),
    "trainticket":          ("trainticket", "v1.0.0", 2),
}
LEVELS = ["low", "medium", "high"]
KEYS = ["release_folder", "level", "iteration"]


def _tag(df: pd.DataFrame) -> pd.DataFrame:
    df = df[df["release_folder"].isin(ANALYSIS_SET)].copy()
    meta = df["release_folder"].map(ANALYSIS_SET)
    df["app"] = meta.str[0]
    df["release"] = meta.str[1]
    df["release_order"] = meta.str[2]
    df["level"] = pd.Categorical(df["level"], LEVELS, ordered=True)
    return df


def valid_runs(verbose: bool = True) -> pd.DataFrame:
    """Run-validity rule: a run is invalid if the SUT served no successful requests (the workload never reached it).

    On the workstation this removes every Online Boutique run: all requests were refused or timed out at the
    cluster entry point, and service CPU was flat across workload levels.
    """
    runs = pd.read_csv(HERE / "tidy_runs.csv")
    runs = runs[(runs["env"] == "workstation") & runs["release_folder"].isin(ANALYSIS_SET)]
    invalid = runs["successful_requests"].fillna(0) <= 0
    if verbose and invalid.any():
        print("Run-validity rule: excluded", int(invalid.sum()), "of", len(runs), "runs with no successful requests:",
              runs.loc[invalid].groupby("release_folder").size().to_dict())
    return runs[~invalid]


def load_services() -> pd.DataFrame:
    """One row per valid workstation run x model x service, exactly as produced by the attribution pipeline."""
    svc = pd.read_csv(HERE / "tidy_services.csv")
    svc = svc.merge(valid_runs(verbose=False)[KEYS], on=KEYS, how="inner")
    return _tag(svc[svc["env"] == "workstation"]).reset_index(drop=True)


def load_runs() -> pd.DataFrame:
    """One row per valid workstation run: workload, CPU, M1/M2 totals and wall-meter net energy, as produced by the pipeline."""
    runs = valid_runs().rename(columns={"meter_net_j": "net_j"})
    runs = _tag(runs)
    runs["epr_m2_j"] = runs["m2_total_j"] / runs["total_requests"]
    return runs.reset_index(drop=True)


def bh(pvalues) -> np.ndarray:
    """Benjamini-Hochberg adjusted p-values (NaNs kept as NaN)."""
    p = np.asarray(pvalues, dtype=float)
    out = np.full_like(p, np.nan)
    ok = ~np.isnan(p)
    if ok.any():
        out[ok] = multipletests(p[ok], method="fdr_bh")[1]
    return out


def save(df: pd.DataFrame, name: str) -> Path:
    RESULTS.mkdir(exist_ok=True)
    path = RESULTS / name
    df.to_csv(path, index=False)
    return path
