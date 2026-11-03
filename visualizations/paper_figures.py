"""Paper artefacts (IEEE two-column): PDF figures and LaTeX result tables.

Run after stats/rq1_spearman.py and stats/rq2_kruskal_cliff.py (data loading is shared via stats/common.py).
Figures -> visualizations/figures/paper/, tables -> stats/results/tables/.
  fig_epr_workload.pdf        EPR per run across workload levels, per app (replaces Fig. 3)
  fig_service_composition.pdf median M2 share of top-3 services per release, medium load (replaces Fig. 5)
  fig_m1_coverage.pdf         M1 / M2 attributed energy per run, per app (replaces Fig. 4)
  tab_rq1.tex, tab_rq2_workload.tex, tab_rq2_release.tex
"""
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "stats"))
from common import LEVELS, RESULTS, load_runs, load_services  # noqa: E402

FIG_OUT = Path(__file__).resolve().parent / "figures" / "paper"
TAB_OUT = RESULTS / "tables"
APPS = {"otel-demo": "OTel Demo", "socialnetwork": "Social Network", "trainticket": "Train Ticket"}
# validated categorical slots 1-3 (dataviz palette, light); markers give a non-colour channel for print/CVD
REL_COLORS = ["#2a78d6", "#eb6834", "#1baf7a"]
REL_MARKERS = ["o", "s", "^"]
OTHER = "#b4b2a9"
INK, MUTED = "#2b2b29", "#6f6e69"

plt.rcParams.update({
    "font.family": "serif", "font.size": 7, "axes.titlesize": 7.5, "axes.labelsize": 7, "legend.fontsize": 6.5,
    "xtick.labelsize": 6.5, "ytick.labelsize": 6.5, "axes.edgecolor": MUTED, "axes.labelcolor": INK,
    "xtick.color": MUTED, "ytick.color": MUTED, "axes.spines.top": False, "axes.spines.right": False,
    "axes.grid": True, "grid.color": "#e6e5e0", "grid.linewidth": 0.5, "axes.axisbelow": True,
    "pdf.fonttype": 42, "savefig.bbox": "tight", "savefig.pad_inches": 0.02,
})


def releases(runs, app):
    return list(runs[runs.app == app].sort_values("release_order")["release"].unique())


# ---------------------------------------------------------------- figures
def fig_epr(runs):
    fig, axes = plt.subplots(len(APPS), 1, figsize=(3.4, 4.8), sharex=True)
    for ax, (app, label) in zip(axes, APPS.items()):
        g = runs[runs.app == app]
        for i, rel in enumerate(releases(runs, app)):
            for j, lvl in enumerate(LEVELS):
                y = g[(g.release == rel) & (g.level == lvl)]["epr_m2_j"].dropna().to_numpy() * 1e3
                x = j + (i - 1) * 0.22
                ax.scatter(np.full(len(y), x) + np.linspace(-0.04, 0.04, len(y)), y, s=9, marker=REL_MARKERS[i],
                           color=REL_COLORS[i], edgecolor="white", linewidth=0.4, zorder=3,
                           label=rel if j == 0 else None)
                if len(y):
                    ax.hlines(np.median(y), x - 0.09, x + 0.09, color=INK, linewidth=1.0, zorder=4)
        ax.set_title(label, loc="left", color=INK)
        ax.set_ylabel("EPR (mJ/request)")
        ax.legend(frameon=False, loc="lower right", bbox_to_anchor=(1.0, 0.98), ncol=3, handletextpad=0.2,
                  columnspacing=0.8, borderaxespad=0)
        ax.grid(axis="x", visible=False)
    axes[-1].set_xticks(range(len(LEVELS)), [l.capitalize() for l in LEVELS])
    axes[-1].set_xlabel("Workload level")
    fig.align_ylabels(axes)
    fig.savefig(FIG_OUT / "fig_epr_workload.pdf")
    plt.close(fig)


def fig_composition(runs, svc):
    m2 = svc[(svc.model == "M2") & (svc.level == "medium")].copy()
    m2["share"] = m2.energy_j / m2.groupby(["release_folder", "iteration"]).energy_j.transform("sum")
    fig, axes = plt.subplots(len(APPS), 1, figsize=(3.4, 3.9))
    for ax, (app, label) in zip(axes, APPS.items()):
        g = m2[m2.app == app]
        rels = releases(runs, app)
        top = g.groupby("service").share.mean().nlargest(3).index.tolist()
        med = g.groupby(["release", "service"]).share.median().unstack().reindex(rels)
        cols = top + ["Other"]
        med["Other"] = 1 - med[top].sum(axis=1)
        left = np.zeros(len(rels))
        for k, s in enumerate(cols):
            v = med[s].to_numpy()
            ax.barh(range(len(rels)), v, left=left, height=0.62, color=(REL_COLORS + [OTHER])[k] if s != "Other" else OTHER,
                    edgecolor="white", linewidth=1.0, label=s)
            for r, (l, w) in enumerate(zip(left, v)):
                if w >= 0.08:
                    ax.text(l + w / 2, r, f"{w*100:.0f}%", ha="center", va="center", fontsize=6,
                            color="white" if s != "Other" else INK)
            left += v
        ax.set_yticks(range(len(rels)), rels)
        ax.set_xlim(0, 1)
        ax.xaxis.set_major_formatter(matplotlib.ticker.PercentFormatter(1.0, decimals=0))
        ax.set_title(label, loc="left", color=INK)
        ax.grid(axis="y", visible=False)
        ax.legend(frameon=False, loc="upper center", bbox_to_anchor=(0.5, -0.18), ncol=4, handlelength=1,
                  handletextpad=0.3, columnspacing=0.8)
    fig.tight_layout(h_pad=1.4)
    fig.savefig(FIG_OUT / "fig_service_composition.pdf")
    plt.close(fig)


def fig_coverage(runs):
    fig, ax = plt.subplots(figsize=(3.4, 1.5))
    for i, (app, label) in enumerate(APPS.items()):
        y = (runs[runs.app == app].m1_total_j / runs[runs.app == app].m2_total_j).dropna().to_numpy() * 100
        ax.scatter(y, np.full(len(y), i) + np.random.default_rng(i).uniform(-0.15, 0.15, len(y)), s=7,
                   color=REL_COLORS[0], edgecolor="white", linewidth=0.3, zorder=3)
        ax.vlines(np.median(y), i - 0.28, i + 0.28, color=INK, linewidth=1.2, zorder=4)
        ax.text(np.median(y), i + 0.34, f"median {np.median(y):.0f}%", ha="center", va="bottom", fontsize=6, color=INK)
    ax.set_yticks(range(len(APPS)), list(APPS.values()))
    ax.set_ylim(-0.5, len(APPS) - 0.3)
    ax.set_xlabel("M1 attributed energy as % of M2 total (per run, n = 45 per app)")
    ax.grid(axis="y", visible=False)
    fig.savefig(FIG_OUT / "fig_m1_coverage.pdf")
    plt.close(fig)


# ---------------------------------------------------------------- tables
def ci(v, lo, hi, d=2):
    return f"{v:.{d}f} [{lo:.{d}f}, {hi:.{d}f}]"


def pfmt(p):
    return "$<$.001" if p < 0.001 else f"{p:.3f}"


def tab_rq1():
    m = pd.read_csv(RESULTS / "rq1_spearman_meter.csv")
    m = m[m.scope == "app"]
    s = pd.read_csv(RESULTS / "rq1_spearman_m1_m2_summary.csv").set_index("app")
    rows = []
    for app, label in APPS.items():
        g = m[m.app == app].set_index("predictor")
        cell = lambda k: ci(g.loc[k, "rho"], g.loc[k, "ci_low"], g.loc[k, "ci_high"]) if k in g.index else "--"
        rows.append(f"{label} & {cell('M1 total')} & {cell('M2 total')} & {cell('CPU total')} & "
                    f"{s.loc[app, 'rho_median']:.2f} & {s.loc[app, 'top1_agree_rate']*100:.0f}\\% \\\\")
    body = "\n".join(rows)
    (TAB_OUT / "tab_rq1.tex").write_text(rf"""\begin{{table*}}[t]
\centering
\caption{{RQ1: agreement between measured and attributed energy (workstation). Spearman's $\rho$ with 95\% bootstrap CI over the 45 runs of each application (3 releases $\times$ 3 workload levels $\times$ 5 repetitions); all $p_{{BH}} < .05$. Service level: median per-run $\rho$ between M1 and M2 service energies, and share of runs in which both models identify the same top service.}}
\label{{tab:rq1}}
\small
\begin{{tabular}}{{lccccc}}
\toprule
 & \multicolumn{{3}}{{c}}{{Wall-meter net energy vs.}} & \multicolumn{{2}}{{c}}{{M1 vs.\ M2 (service level)}} \\
\cmidrule(lr){{2-4}}\cmidrule(lr){{5-6}}
Application & M1 total & M2 total & CPU total & median $\rho$ & same top service \\
\midrule
{body}
\bottomrule
\end{{tabular}}
\end{{table*}}
""")


def tab_rq2_workload():
    kw = pd.read_csv(RESULTS / "rq2_kruskal.csv")
    cd = pd.read_csv(RESULTS / "rq2_cliff.csv")
    kw, cd = kw[kw.family == "workload"], cd[(cd.family == "workload") & (cd.a == "low") & (cd.b == "high")]
    resp = ["EPR (M2)", "Energy (M2)", "Energy (meter)"]
    rows = []
    for app, label in APPS.items():
        for k, rel in enumerate(sorted(kw[kw.app == app].context.unique(), key=lambda r: [int(x) for x in r[1:].split(".")])):
            cells = []
            for r in resp:
                h = kw[(kw.app == app) & (kw.context == rel) & (kw.response == r)].iloc[0]
                c = cd[(cd.app == app) & (cd.context == rel) & (cd.response == r)].iloc[0]
                d = f"{c.delta:+.2f}" + ("$^\\dagger$" if c.separated else f" [{c.ci_low:+.2f}, {c.ci_high:+.2f}]")
                cells.append(f"{h.H:.1f} & {pfmt(h.p_bh)} & {d}")
            rows.append(f"{label if k == 0 else ''} & {rel} & " + " & ".join(cells) + " \\\\")
        rows.append("\\midrule")
    body = "\n".join(rows[:-1])
    hdr = " & ".join(f"\\multicolumn{{3}}{{c}}{{{r}}}" for r in resp)
    cm = "".join(f"\\cmidrule(lr){{{3+3*i}-{5+3*i}}}" for i in range(3))
    (TAB_OUT / "tab_rq2_workload.tex").write_text(rf"""\begin{{table*}}[t]
\centering
\caption{{RQ2: effect of workload level (low/medium/high) per release. Kruskal--Wallis $H$ (df = 2) with Benjamini--Hochberg adjusted $p$ across the workload family, and Cliff's $\delta$ (high vs.\ low) with 95\% bootstrap CI; $\delta < 0$: lower at high load. $^\dagger$complete separation ($|\delta| = 1$; every high-load run below/above every low-load run), for which the bootstrap CI is degenerate. $n = 5$ runs per level.}}
\label{{tab:rq2-workload}}
\small
\setlength{{\tabcolsep}}{{4pt}}
\begin{{tabular}}{{ll ccc ccc ccc}}
\toprule
 & & {hdr} \\
{cm}
App & Release & $H$ & $p_{{BH}}$ & $\delta$ & $H$ & $p_{{BH}}$ & $\delta$ & $H$ & $p_{{BH}}$ & $\delta$ \\
\midrule
{body}
\bottomrule
\end{{tabular}}
\end{{table*}}
""")


def tab_rq2_release():
    kw = pd.read_csv(RESULTS / "rq2_kruskal.csv")
    cd = pd.read_csv(RESULTS / "rq2_cliff.csv")
    kw, cd = kw[kw.family == "release"], cd[cd.family == "release"]
    resp = ["EPR (M2)", "Energy (M2)", "Energy (meter)", "Dominant-service share"]
    rows = []
    for app, label in APPS.items():
        pairs = cd[cd.app == app][["a", "b"]].drop_duplicates().itertuples(index=False)
        for k, (a, b) in enumerate(pairs):
            cells = []
            for r in resp:
                vals = []
                for lvl in LEVELS:
                    c = cd[(cd.app == app) & (cd.a == a) & (cd.b == b) & (cd.response == r) & (cd.context == lvl)]
                    if c.empty:
                        vals.append("--"); continue
                    c = c.iloc[0]
                    v = f"{c.delta:+.2f}"
                    vals.append(f"\\textbf{{{v}}}" if c.kw_p_bh < .05 and abs(c.delta) >= 0.428 else v)
                cells.append(" / ".join(vals))
            rows.append(f"{label if k == 0 else ''} & {a}$\\rightarrow${b} & " + " & ".join(cells) + " \\\\")
        sig = kw[kw.app == app]
        rows.append("\\midrule")
    body = "\n".join(rows[:-1])
    (TAB_OUT / "tab_rq2_release.tex").write_text(rf"""\begin{{table*}}[t]
\centering
\caption{{RQ2: effect of software release. Cliff's $\delta$ (newer vs.\ older release) at low / medium / high workload; $\delta > 0$: higher in the newer release. Bold: large effect ($|\delta| \geq 0.43$) in a cell whose Kruskal--Wallis test across the three releases is significant after Benjamini--Hochberg correction ($p_{{BH}} < .05$). $n = 5$ runs per release and level. Dominant service: {", ".join(f"{APPS[a]} \texttt{{{s}}}" for a, s in dominant().items())}.}}
\label{{tab:rq2-release}}
\small
\setlength{{\tabcolsep}}{{4pt}}
\begin{{tabular}}{{llcccc}}
\toprule
App & Releases & EPR (M2) & Energy (M2) & Energy (meter) & Dominant-service share \\
\midrule
{body}
\bottomrule
\end{{tabular}}
\end{{table*}}
""")


_DOM = {}
def dominant():
    return _DOM


def main():
    FIG_OUT.mkdir(parents=True, exist_ok=True)
    TAB_OUT.mkdir(parents=True, exist_ok=True)
    runs, svc = load_runs(), load_services()
    m2 = svc[svc.model == "M2"].copy()
    m2["share"] = m2.energy_j / m2.groupby(["release_folder", "level", "iteration"]).energy_j.transform("sum")
    _DOM.update(m2.groupby(["app", "service"]).share.mean().reset_index().sort_values("share")
                .groupby("app").tail(1).set_index("app")["service"].to_dict())
    fig_epr(runs); fig_composition(runs, svc); fig_coverage(runs)
    tab_rq1(); tab_rq2_workload(); tab_rq2_release()
    for d in (FIG_OUT, TAB_OUT):
        print("Written to", d); [print("  ", p.name) for p in sorted(d.iterdir())]


if __name__ == "__main__":
    main()
