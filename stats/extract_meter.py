"""Recompute wall-meter baseline/workload power from raw physical_power_meter.csv (stdlib only)."""
import csv, glob, os, sys, statistics as st
ROOT = sys.argv[1] if len(sys.argv) > 1 else "final-runs/experiment-runs-with-attribution"
rows = []
for p in sorted(glob.glob(os.path.join(ROOT, "*", "workstation*", "*", "iteration_*", "physical_power_meter.csv"))):
    parts = p.split(os.sep)
    b, w = [], []
    for r in csv.DictReader(open(p)):
        if r.get("error"): continue
        try: v = float(r["apower"])
        except (TypeError, ValueError): continue
        (b if r["phase"] == "baseline" else w if r["phase"] == "workload" else []).append(v)
    if len(b) < 10 or len(w) < 10: continue
    rows.append(dict(release_folder=parts[-5], level=parts[-3], iteration=parts[-2], n_base=len(b), n_wl=len(w),
                     base_w_mean=st.mean(b), base_w_sd=st.stdev(b), wl_w_mean=st.mean(w), wl_w_sd=st.stdev(w),
                     net_w=st.mean(w) - st.mean(b), net_j=(st.mean(w) - st.mean(b)) * len(w), gross_wl_j=sum(w)))
with open(sys.argv[2] if len(sys.argv) > 2 else "stats/tidy_meter.csv", "w", newline="") as f:
    wr = csv.DictWriter(f, fieldnames=list(rows[0].keys())); wr.writeheader(); wr.writerows(rows)
print(len(rows), "meter rows")
