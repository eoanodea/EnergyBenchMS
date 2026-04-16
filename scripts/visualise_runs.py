#!/usr/bin/env python3
"""Generate an HTML dashboard to compare run consistency and SUT energy."""

import argparse
import json
import statistics
from datetime import datetime
from pathlib import Path


# Configure which containers belong to the system under test.
SUT_CONTAINERS = [
    "nginx",
]

# Exclude infrastructure services from application-focused energy analysis.
EXCLUDED_CONTAINERS = {
    "kepler",
    "coredns",
    "metrics-server",
    "traefik",
    "local-path-provisioner",
}


def load_json(path):
    path = Path(path)
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as infile:
        return json.load(infile)


def parse_duration_seconds(metadata):
    if not metadata:
        return None

    timestamps = metadata.get("timestamps", {})
    start = timestamps.get("workload_start")
    end = timestamps.get("workload_end")
    if not start or not end:
        return None

    try:
        start_dt = datetime.fromisoformat(start)
        end_dt = datetime.fromisoformat(end)
    except ValueError:
        return None

    return max(0.0, (end_dt - start_dt).total_seconds())


def make_workload_group(metadata):
    if not metadata:
        return "unknown"

    params = metadata.get("workload_parameters", {})
    users = params.get("users", "?")
    duration = params.get("duration", "?")
    return f"users={users}, duration={duration}s"


def collect_runs(runs_dir, specific_run=None):
    runs = []
    runs_root = Path(runs_dir)

    if specific_run:
        run_path = Path(specific_run)
        if run_path.is_dir() and (run_path / "summary.json").exists():
            candidates = [run_path / "summary.json"]
        else:
            candidates = sorted(run_path.rglob("summary.json"))
    else:
        candidates = sorted(runs_root.rglob("summary.json"))

    for summary_path in candidates:
        run_dir = summary_path.parent
        summary = load_json(summary_path)
        if summary is None:
            continue
        metadata = load_json(run_dir / "metadata.json")
        try:
            run_name = str(run_dir.relative_to(runs_root))
        except ValueError:
            run_name = run_dir.name
        runs.append((run_name, summary, metadata))

    return runs


def safe_stdev(values):
    cleaned = [v for v in values if isinstance(v, (int, float))]
    if len(cleaned) <= 1:
        return 0.0
    return statistics.pstdev(cleaned)


def get_filtered_sut_energy_means(summary):
    energy = summary.get("energy_by_container_name", {})
    out = {}
    for container_name, stats in energy.items():
        if container_name in EXCLUDED_CONTAINERS:
            continue
        if container_name not in SUT_CONTAINERS:
            continue
        if not isinstance(stats, dict):
            continue
        value = stats.get("mean")
        if isinstance(value, (int, float)):
            out[container_name] = value
    return out


def make_dashboard_data(runs, sut_container):
    run_rows = []

    for run_name, summary, metadata in runs:
        cpu_total = summary.get("cpu_total", {})
        cpu_total_mean = cpu_total.get("mean") if isinstance(cpu_total, dict) else None
        cpu_total_max = cpu_total.get("max") if isinstance(cpu_total, dict) else None

        duration_seconds = parse_duration_seconds(metadata)
        sut_means = get_filtered_sut_energy_means(summary)
        sut_energy_mean = sut_means.get(sut_container)

        sut_energy_total = None
        if isinstance(sut_energy_mean, (int, float)) and isinstance(duration_seconds, (int, float)):
            sut_energy_total = sut_energy_mean * duration_seconds

        run_rows.append(
            {
                "name": run_name,
                "workload_group": make_workload_group(metadata),
                "duration_seconds": duration_seconds,
                "cpu_total_mean": cpu_total_mean,
                "cpu_total_max": cpu_total_max,
                "sut_energy_mean": sut_energy_mean,
                "sut_energy_total": sut_energy_total,
            }
        )

    cpu_values = [row["cpu_total_mean"] for row in run_rows]
    sut_values = [row["sut_energy_mean"] for row in run_rows]

    return {
        "sut_container": sut_container,
        "sut_containers": SUT_CONTAINERS,
        "excluded_containers": sorted(EXCLUDED_CONTAINERS),
        "runs": run_rows,
        "overall_consistency": {
            "cpu_total_mean_stdev": safe_stdev(cpu_values),
            "sut_energy_mean_stdev": safe_stdev(sut_values),
        },
    }


def build_html(data):
    payload = json.dumps(data)
    return f"""<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>Run Consistency Dashboard</title>
  <script src=\"https://cdn.jsdelivr.net/npm/chart.js\"></script>
  <style>
    :root {{
      --bg: #f7f4ee;
      --card: #fffdfa;
      --ink: #1f1b16;
      --muted: #6f665d;
      --line: #ddd2c3;
      --accent: #116466;
      --accent2: #b85c38;
    }}
    body {{
      margin: 0;
      font-family: Georgia, "Times New Roman", serif;
      background: radial-gradient(circle at top left, #fff, var(--bg));
      color: var(--ink);
    }}
    .wrap {{
      max-width: 1200px;
      margin: 24px auto;
      padding: 0 16px 24px;
    }}
    h1 {{
      margin: 0 0 8px;
      letter-spacing: 0.4px;
    }}
    p {{
      margin: 0 0 18px;
      color: var(--muted);
    }}
    .grid {{
      display: grid;
      grid-template-columns: 1fr;
      gap: 14px;
    }}
    .controls, .card {{
      border: 1px solid var(--line);
      background: var(--card);
      border-radius: 12px;
      padding: 12px;
    }}
    .controls {{
      margin-bottom: 14px;
    }}
    .controls h2, .card h2 {{
      margin: 0 0 10px;
      font-size: 18px;
    }}
    .run-list {{
      display: flex;
      flex-wrap: wrap;
      gap: 10px 14px;
      margin-bottom: 10px;
    }}
    .run-item {{
      display: inline-flex;
      align-items: center;
      gap: 6px;
      font-size: 14px;
    }}
    .actions {{
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      margin-bottom: 8px;
    }}
    .actions button {{
      border: 1px solid var(--line);
      background: #fff;
      border-radius: 8px;
      padding: 6px 10px;
      cursor: pointer;
      color: var(--ink);
    }}
    .small {{
      font-size: 13px;
      color: var(--muted);
      margin-top: 6px;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 14px;
    }}
    th, td {{
      border-bottom: 1px solid var(--line);
      padding: 8px;
      text-align: left;
    }}
    th {{
      color: var(--muted);
      font-weight: 600;
    }}
    canvas {{
      width: 100% !important;
      max-height: 360px;
    }}
    @media (min-width: 980px) {{
      .grid {{
        grid-template-columns: 1fr 1fr;
      }}
    }}
  </style>
</head>
<body>
  <div class=\"wrap\">
    <h1>Run Consistency Dashboard</h1>
    <p>Answers: Is this experiment consistent, and how much energy does the application consume?</p>

    <section class=\"card\" style=\"margin-bottom:14px;\">
      <h2>Per-Run Summary</h2>
      <table>
        <thead>
          <tr>
            <th>Run</th>
            <th>Workload Group</th>
            <th>CPU Total Mean</th>
            <th>CPU Total Max</th>
            <th id=\"sutMeanHeader\"></th>
            <th id=\"sutTotalHeader\"></th>
          </tr>
        </thead>
        <tbody id=\"summaryTableBody\"></tbody>
      </table>
    </section>

    <section class=\"controls\">
      <h2>Run Selection</h2>
      <div id=\"runSelector\" class=\"run-list\"></div>
      <div class=\"actions\">
        <button id=\"selectAllBtn\" type=\"button\">Select all</button>
        <button id=\"selectNoneBtn\" type=\"button\">Select none</button>
      </div>
      <div class=\"small\" id=\"consistencyText\"></div>
      <div class=\"small\" id=\"groupText\"></div>
      <div class=\"small\" id=\"filtersText\"></div>
    </section>

    <div class=\"grid\">
      <section class=\"card\">
        <h2>Total CPU Load per Run</h2>
        <canvas id=\"cpuTotalChart\"></canvas>
      </section>

      <section class=\"card\">
        <h2 id=\"energyChartTitle\"></h2>
        <canvas id=\"sutEnergyChart\"></canvas>
      </section>
    </div>
  </div>

  <script>
    const data = {payload};
    const runs = data.runs;
    const runNames = runs.map((r) => r.name);
    const sutContainer = data.sut_container;
    const selected = new Set(runNames);

    document.getElementById('sutMeanHeader').textContent = `${{sutContainer}} energy_mean`;
    document.getElementById('sutTotalHeader').textContent = `${{sutContainer}} energy_total`;
    document.getElementById('energyChartTitle').textContent = `Application Energy Consumption (${{sutContainer}})`;
    document.getElementById('filtersText').textContent =
      `SUT containers: ${{data.sut_containers.join(', ')}} | Excluded infra: ${{data.excluded_containers.join(', ')}}`;

    const commonOptions = {{
      responsive: true,
      maintainAspectRatio: false,
      interaction: {{ mode: 'index', intersect: false }},
      plugins: {{
        legend: {{ position: 'top' }}
      }},
      scales: {{
        y: {{ beginAtZero: true }}
      }}
    }};

    const cpuTotalChart = new Chart(document.getElementById('cpuTotalChart'), {{
      type: 'line',
      data: {{ labels: [], datasets: [
        {{
          label: 'cpu_total mean',
          data: [],
          borderColor: '#116466',
          backgroundColor: 'rgba(17,100,102,0.2)',
          tension: 0.2
        }},
        {{
          label: 'cpu_total max',
          data: [],
          borderColor: '#b85c38',
          backgroundColor: 'rgba(184,92,56,0.2)',
          tension: 0.2
        }}
      ]}},
      options: commonOptions
    }});

    const sutEnergyChart = new Chart(document.getElementById('sutEnergyChart'), {{
      type: 'line',
      data: {{ labels: [], datasets: [
        {{
          label: `${{sutContainer}} energy_mean (rate)`,
          data: [],
          borderColor: '#116466',
          backgroundColor: 'rgba(17,100,102,0.2)',
          tension: 0.2
        }},
        {{
          label: `${{sutContainer}} energy_total`,
          data: [],
          borderColor: '#b85c38',
          backgroundColor: 'rgba(184,92,56,0.2)',
          tension: 0.2
        }}
      ]}},
      options: commonOptions
    }});

    function selectedRuns() {{
      return runs.filter((r) => selected.has(r.name));
    }}

    function formatMaybeNumber(value) {{
      if (typeof value !== 'number' || Number.isNaN(value)) return '-';
      return value.toFixed(6);
    }}

    function computeStdev(values) {{
      const nums = values.filter((v) => typeof v === 'number' && !Number.isNaN(v));
      if (nums.length <= 1) return 0;
      const mean = nums.reduce((a, b) => a + b, 0) / nums.length;
      const variance = nums.reduce((acc, v) => acc + ((v - mean) ** 2), 0) / nums.length;
      return Math.sqrt(variance);
    }}

    function renderSummaryTable() {{
      const body = document.getElementById('summaryTableBody');
      body.innerHTML = '';

      selectedRuns().forEach((r) => {{
        const tr = document.createElement('tr');
        const cells = [
          r.name,
          r.workload_group || 'unknown',
          formatMaybeNumber(r.cpu_total_mean),
          formatMaybeNumber(r.cpu_total_max),
          formatMaybeNumber(r.sut_energy_mean),
          formatMaybeNumber(r.sut_energy_total)
        ];

        cells.forEach((value) => {{
          const td = document.createElement('td');
          td.textContent = value;
          tr.appendChild(td);
        }});

        body.appendChild(tr);
      }});
    }}

    function renderConsistency() {{
      const rows = selectedRuns();
      const cpuStdev = computeStdev(rows.map((r) => r.cpu_total_mean));
      const sutStdev = computeStdev(rows.map((r) => r.sut_energy_mean));

      document.getElementById('consistencyText').textContent =
        `Std dev (selected runs): cpu_total_mean=${{cpuStdev.toFixed(6)}}, ` +
        `${{sutContainer}} energy_mean=${{sutStdev.toFixed(6)}}`;

      const groups = new Map();
      rows.forEach((r) => {{
        const key = r.workload_group || 'unknown';
        groups.set(key, (groups.get(key) || 0) + 1);
      }});

      const groupLines = Array.from(groups.entries()).map(([k, v]) => `${{k}} -> ${{v}} run(s)`);
      document.getElementById('groupText').textContent =
        `Workload grouping: ${{groupLines.join(' | ') || 'none'}}`;
    }}

    function renderCharts() {{
      const rows = selectedRuns();
      const labels = rows.map((r) => r.name);

      cpuTotalChart.data.labels = labels;
      cpuTotalChart.data.datasets[0].data = rows.map((r) => r.cpu_total_mean);
      cpuTotalChart.data.datasets[1].data = rows.map((r) => r.cpu_total_max);
      cpuTotalChart.update();

      sutEnergyChart.data.labels = labels;
      sutEnergyChart.data.datasets[0].data = rows.map((r) => r.sut_energy_mean);
      sutEnergyChart.data.datasets[1].data = rows.map((r) => r.sut_energy_total);
      sutEnergyChart.update();

      renderSummaryTable();
      renderConsistency();
    }}

    function renderRunSelector() {{
      const host = document.getElementById('runSelector');
      host.innerHTML = '';

      runNames.forEach((name) => {{
        const label = document.createElement('label');
        label.className = 'run-item';

        const input = document.createElement('input');
        input.type = 'checkbox';
        input.checked = selected.has(name);
        input.addEventListener('change', () => {{
          if (input.checked) selected.add(name);
          else selected.delete(name);
          renderCharts();
        }});

        label.appendChild(input);
        label.appendChild(document.createTextNode(name));
        host.appendChild(label);
      }});
    }}

    document.getElementById('selectAllBtn').addEventListener('click', () => {{
      runNames.forEach((name) => selected.add(name));
      renderRunSelector();
      renderCharts();
    }});

    document.getElementById('selectNoneBtn').addEventListener('click', () => {{
      selected.clear();
      renderRunSelector();
      renderCharts();
    }});

    renderRunSelector();
    renderCharts();
  </script>
</body>
</html>
"""


def main():
    parser = argparse.ArgumentParser(description="Build an HTML run comparison dashboard")
    parser.add_argument(
        "--runs-dir",
        default="runs",
        help="Directory containing run folders (default: runs)",
    )
    parser.add_argument(
        "--run-dir",
        help="Optional single run directory to visualize",
    )
    parser.add_argument(
        "--output",
        default="runs_comparison.html",
        help="Output HTML path (default: runs_comparison.html)",
    )
    parser.add_argument(
        "--sut-container",
        default="nginx",
        help="Container name to highlight as application SUT (default: nginx)",
    )
    args = parser.parse_args()

    runs = collect_runs(args.runs_dir, args.run_dir)
    if not runs:
        raise SystemExit("No summary.json files found in the selected run directories")

    data = make_dashboard_data(runs, args.sut_container)
    html = build_html(data)

    output_path = Path(args.output)
    output_path.write_text(html, encoding="utf-8")
    print(f"Dashboard written to {output_path}")
    print(f"Runs included: {', '.join(row['name'] for row in data['runs'])}")


if __name__ == "__main__":
    main()
