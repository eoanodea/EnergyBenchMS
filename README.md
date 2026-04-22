## How to use

To install the required dependencies, you can use the following command:

```bash
pip install -r requirements.txt
```

To run an experiment, you can use the following command:

```bash
python scripts/run_experiment.py --app apps/simple-web --workload workloads/simple-web.yaml --locustfile apps/simple-web/locustfile.py
```

To compile the results, you can use the following command:

```bash
python scripts/query_prometheus.py \
  --run-dir runs/20260413_173747 \
  --prom-url http://192.168.0.100:9090 \
  --energy-source auto
```

Supported values for `--energy-source` are: `auto`, `joules`, `bpf_cpu_time`, and `bpf_block_irq`.
The default `auto` mode prefers joules and falls back to BPF-based metrics when joules are unavailable or zero.

To summarize the results, you can use the following command:

```bash
python scripts/summarise_run.py --run-dir runs/20260413_173747
```

To run the same experiment multiple times, query each run, summarise each run, and generate the comparison dashboard:

```bash
python scripts/run_pipeline.py \
  --count 5 \
  --app apps/simple-web \
  --workload workloads/simple-web.yaml \
  --locustfile apps/simple-web/locustfile.py \
  --cooldown-seconds 60 \
  --energy-source auto \
  --prom-url http://192.168.0.100:9090
```

The pipeline performs one warmup run before the measured runs and waits for the configured cooldown after warmup and between each measured run.
It also creates a batch directory under `runs/` named like `timestamp_sutname`, with measured runs stored as `iteration_timestamp` directories inside it.

## Saturation calibration mode (stepwise)

Use saturation mode to run fixed user levels with fixed dwell time and reset/cooldown between levels.

### Default config in workload YAML

The workload file supports a simple saturation section:

```yaml
saturation:
  levels: [20, 40, 60, 80, 100]
  dwell_seconds: 120
  spawn_rate: 5
  ramp_exclusion_seconds: 20
  reset_between_levels: true
  cooldown_seconds: 30
```

### Run saturation mode

If values are set in the workload YAML, this is enough:

```bash
python scripts/run_pipeline.py \
  --saturation-enabled \
  --app apps/simple-web \
  --workload workloads/simple-web.yaml \
  --locustfile apps/simple-web/locustfile.py \
  --energy-source auto \
  --prom-url http://192.168.0.100:9090
```

CLI `--sat-*` flags are optional overrides for one-off runs.

### Saturation outputs

A saturation batch directory under `runs/` contains:

- `saturation_plan.json`: execution config and run mapping per user level
- `calibration_summary.csv`: primary calibration dataset (one row per level)
- `saturation_summary.json`: threshold-based interpretation
- `level_XXX_iteration_*` directories with full per-level artifacts

Per-level artifacts include:

- Prometheus metrics (`cpu_total.json`, `cpu_k8s_by_id.json`, `energy.json`, ...)
- Locust exports (`locust_stats.csv`, `locust_stats_history.csv`, failures/exceptions)
- `metadata.json` with `workload_effective_start` and ramp exclusion metadata
- `summary.json` and `summary.csv`

### Calibration CSV columns

`calibration_summary.csv` contains:

- `user_level`
- `throughput_mean`
- `cpu_mean`
- `cpu_max`
- `energy_total`
- `energy_per_request`
- `p95_latency`
- `error_rate`

Definitions used:

- `effective_duration = dwell_seconds - ramp_exclusion_seconds`
- `energy_total = energy_mean * effective_duration`
- `energy_per_request = energy_total / successful_requests`

### Saturation analysis thresholds

`saturation_summary.json` is produced by `scripts/saturation_analyse.py` using simple adjacent-level threshold logic:

- Throughput plateau (primary): marginal throughput gain below threshold
- Degradation (primary): latency jump and/or error-rate threshold
- CPU threshold (secondary): first level crossing CPU mean threshold

Threshold defaults:

- `--sat-plateau-threshold 0.05`
- `--sat-latency-jump-threshold 0.30`
- `--sat-error-rate-threshold 0.01`
- `--sat-cpu-threshold 0.90`

No smoothing, regressions, or curve fitting are used in this phase.

To clean up only the application stack, you can use:

```bash
python scripts/cleanup_sut.py \
  --app apps/simple-web \
  --sleep-seconds 30
```

The cleanup step deletes only the manifests in the application directory, waits for the matching SUT pods to terminate in the app's namespace, and does not touch observability components or other namespaces.
