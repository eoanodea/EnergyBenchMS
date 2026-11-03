"""Extract per-iteration tidy tables from final-runs/experiment-runs-with-attribution (stdlib only)."""
import csv, json, os, sys, glob
ROOT = sys.argv[1] if len(sys.argv) > 1 else "final-runs/experiment-runs-with-attribution"
OUT = sys.argv[2] if len(sys.argv) > 2 else "stats"
runs, svc = [], []
for summ in sorted(glob.glob(os.path.join(ROOT, "*", "*", "*", "iteration_*", "summary.json"))):
    it = os.path.dirname(summ)
    parts = it.split(os.sep)
    rel_folder, exp_folder, level, iteration = parts[-4], parts[-3], parts[-2], parts[-1]
    env = "ec2" if exp_folder.startswith("ec2") else "workstation"
    d = json.load(open(summ))
    wl = d.get("workload", {}); pm = d.get("physical_power_meter", {}) or {}
    es = d.get("energy_source", {}) or {}
    row = dict(release_folder=rel_folder, exp_folder=exp_folder, env=env, level=level, iteration=iteration,
               throughput_rps=wl.get("throughput_mean_rps"), error_rate=wl.get("error_rate"), p95_ms=wl.get("p95_latency"),
               cpu_total_mean=(d.get("cpu_total") or {}).get("mean"), energy_source=es.get("selected_energy_source"),
               meter_baseline_w=pm.get("baseline_power_watts"), meter_mean_w=((pm.get("metrics") or {}).get("apower") or {}).get("mean"),
               meter_net_j=pm.get("baseline_corrected_workload_energy_joules"), meter_flags="|".join(pm.get("quality_flags") or []),
               meter_drift=pm.get("baseline_drift_ratio"))
    sa = os.path.join(it, "attribution", "service_attribution.csv")
    m1_tot = m2_tot = 0.0; dur = req = ok = None; n_m1 = 0
    if os.path.exists(sa):
        for r in csv.DictReader(open(sa)):
            dur = r.get("duration_seconds") or dur; req = r.get("total_requests") or req
            ok = r.get("successful_requests") or ok
            e = float(r["allocated_energy_joules"] or 0)
            if r["model_variant"] == "M1": m1_tot += e; n_m1 += 1
            elif r["model_variant"] == "M2": m2_tot += e
            svc.append(dict(release_folder=rel_folder, env=env, level=level, iteration=iteration, model=r["model_variant"],
                            service=r["service_name"], group=r.get("service_group"), energy_j=e,
                            share=r.get("share_of_reference_energy")))
    row.update(duration_s=dur, total_requests=req, successful_requests=ok, m1_total_j=m1_tot, m1_services=n_m1, m2_total_j=m2_tot)
    runs.append(row)
os.makedirs(OUT, exist_ok=True)
for name, rows in (("tidy_runs.csv", runs), ("tidy_services.csv", svc)):
    with open(os.path.join(OUT, name), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys())); w.writeheader(); w.writerows(rows)
print(len(runs), "runs;", len(svc), "service rows")
