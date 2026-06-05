## Setup

Install dependencies:

```bash
pip install -r requirements.txt
```

## Single experiment run

Run one experiment:

```bash
python scripts/run_experiment.py \
  --app apps/simple-web \
  --workload workloads/simple-web.yaml \
  --locustfile apps/simple-web/locustfile.py
```

Useful optional flags:

- `--ramp-exclusion-seconds`: excludes initial workload seconds from downstream summary windows
- `--baseline-seconds`: captures baseline power before workload starts (default: `20`)

Run with physical meter sampling:

```bash
python scripts/run_experiment.py \
  --app apps/simple-web \
  --workload workloads/simple-web.yaml \
  --locustfile apps/simple-web/locustfile.py \
  --baseline-seconds 20 \
  --power-meter-url http://192.168.0.105/rpc/Switch.GetStatus?id=0 \
  --power-meter-interval-seconds 1 \
  --power-meter-request-timeout-seconds 5
```

Notes:

- Physical meter data is additive, not a replacement for Prometheus metrics.
- The meter is treated as the whole-system source; Kepler remains container-level attribution/proxy data.

## Per-run post-processing

Query Prometheus for a run:

```bash
python scripts/query_prometheus.py \
  --run-dir runs/<run_dir> \
  --prom-url http://192.168.0.100:9090 \
  --energy-source auto
```

Supported `--energy-source` values: `auto`, `joules`, `bpf_cpu_time`, `bpf_block_irq`.
`auto` prefers joules and falls back to BPF metrics when needed.

Summarize a run:

```bash
python scripts/summarise_run.py --run-dir runs/<run_dir>
```

When `physical_power_meter.csv` is present, the summary includes a `physical_power_meter` block with:

- raw meter delta
- baseline-corrected workload energy
- meter energy per request
- baseline/quality flags

## Batch pipeline

Run repeated experiments and generate a comparison dashboard:

```bash
python scripts/run_pipeline.py \
  --count 3 \
  --app apps/simple-web \
  --workload workloads/simple-web.yaml \
  --locustfile apps/simple-web/locustfile.py \
  --baseline-seconds 20 \
  --cooldown-seconds 60 \
  --energy-source auto \
  --prom-url http://192.168.0.100:9090 \
  --power-meter-url http://192.168.0.105/rpc/Switch.GetStatus?id=0 \
  --power-meter-interval-seconds 1
```

Pipeline behavior:

- one warmup run is executed before measured runs
- cooldown is applied after warmup and between measured runs
- baseline window is configurable via `--baseline-seconds` and passed to all `run_experiment.py` invocations
- meter flags are forwarded to measured runs only

## Multiple workload levels

You can define named workload levels in workload YAML:

```yaml
workload_levels:
  - low: 20
  - medium: 40
  - high: 60
```

The pipeline executes `--count` iterations per level and stores them under level-specific folders.

## Saturation mode

Workload YAML example:

```yaml
saturation:
  levels: [20, 40, 60, 80, 100]
  dwell_seconds: 120
  spawn_rate: 5
  ramp_exclusion_seconds: 20
  reset_between_levels: true
  cooldown_seconds: 30
```

Run saturation mode:

```bash
python scripts/run_pipeline.py \
  --saturation-enabled \
  --app apps/simple-web \
  --workload workloads/simple-web.yaml \
  --locustfile apps/simple-web/locustfile.py \
  --energy-source auto \
  --prom-url http://192.168.0.100:9090
```

Outputs include:

- `saturation_plan.json`
- `calibration_summary.csv`
- `saturation_summary.json`
- one run directory per level iteration

## App onboarding and deployment config

If an app does not use the simple `apps/<name>/deployment.yaml` layout, add a config file.

Supported config locations (priority order):

1. `apps/<app>/pipeline_app.yaml`
2. `app-configs/<app-name>.yaml`
3. `app-configs/<relative-app-path>/pipeline_app.yaml`

Example:

```yaml
manifest_path: deploy/kubernetes/manifests
namespace: sock-shop
exclude_resource_patterns:
  - "(^|/)prometheus($|[-/])"
  - "(^|/)kepler($|[-/])"
  - "(^|/)load-?test($|[-/])"
```

One-off CLI overrides:

- `--manifest-path`
- `--namespace`
- `--exclude-resource-pattern` (repeatable)
- `--exclude-kind` (repeatable)

## SUT lifecycle helper

Bring the application stack up/down without running workload traffic:

```bash
python scripts/manage_sut.py up --app apps/simple-web

python scripts/manage_sut.py down \
  --app apps/simple-web \
  --sleep-seconds 30
```

`up` applies filtered manifests and waits for deployment readiness.
`down` deletes the same filtered manifests, waits for matching pod termination, then optionally sleeps.

## Submodule management

The pipeline expects an application to be it's own full git repository. If you want to add a new application you can run the following command:

```
git submodule add git@github.com:open-telemetry/opentelemetry-demo.git apps/oteldemo
```

If you need to fetch existing submodules after cloning the repository, you can run:

```git submodule update --init --recursive

```

Adding a new submodule (using otel demo 2.0.0 as an example):

1. Create the submodule in the desired location (e.g. `apps/oteldemo-2.0.0`):

```
git submodule add --name apps/oteldemo-2.0.0 git@github.com:open-telemetry/opentelemetry-demo.git apps/oteldemo-2.0.0
```

2. Check out the desired version in the submodule:

```
git -C apps/oteldemo-2.0.0 fetch --tags
git -C apps/oteldemo-2.0.0 checkout 2.0.0
```

3. If you need to sync on another machine, run

```
git submodule update --init --recursive
```
