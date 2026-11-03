# Statistical analysis (ICSA 2027)

Workstation runs only; analysis set = the 12 release folders in `common.ANALYSIS_SET`. No filtering happens here:
data-quality fixes belong in the pipeline (`scripts/summarise_run.py`, `scripts/attribution_postprocess.py`).

```bash
pip install -r stats/requirements.txt
# 1. regenerate per-run outputs (must run in the controller's time zone: run metadata stores naive local time)
export TZ=Europe/Rome
for it in $(find final-runs/experiment-runs-with-attribution -type d -name 'iteration_*'); do python3 scripts/summarise_run.py --run-dir "$it"; done
for b in final-runs/experiment-runs-with-attribution/*/*/; do python3 scripts/batch_attribution_postprocess.py --batch-dir "$b"; done
# 2. tidy tables
python3 stats/extract_tidy.py && python3 stats/extract_meter.py   # extract_meter = independent cross-check of meter net energy
# 3. analyses
python3 stats/rq1_spearman.py        # RQ1: Spearman rho (meter vs M1/M2/CPU; M1 vs M2 per service)
python3 stats/rq2_kruskal_cliff.py   # RQ2: Kruskal-Wallis + Benjamini-Hochberg + Cliff's delta
```
Outputs land in `stats/results/`.
