## How to use

To install the required dependencies, you can use the following command:

```pip install -r requirements.txt

```

To run an experiment, you can use the following command:

```
python scripts/run_experiment.py --app apps/simple-web --workload workloads/simple-web.yaml --locustfile apps/simple-web/locustfile.py
```

To compile the results, you can use the following command:

```
python scripts/query_prometheus.py \
  --run-dir runs/20260413_173747 \
  --prom-url http://192.168.0.100:9090
```

To summarize the results, you can use the following command:

```
python scripts/summarise_run.py --run-dir runs/20260413_173747

```

To run the same experiment multiple times, query each run, summarise each run, and generate the comparison dashboard, you can use:

```bash
python scripts/run_pipeline.py \
  --count 3 \
  --app apps/simple-web \
  --workload workloads/simple-web.yaml \
  --locustfile apps/simple-web/locustfile.py \
  --cooldown-seconds 30 \
  --prom-url http://192.168.0.100:9090
```

The pipeline performs one warmup run before the measured runs and waits for the configured cooldown after warmup and between each measured run.

To clean up only the application stack, you can use:

```bash
python scripts/cleanup_sut.py \
  --app apps/simple-web \
  --sleep-seconds 30
```

The cleanup step deletes only the manifests in the application directory, waits for the matching SUT pods to terminate in the app's namespace, and does not touch observability components or other namespaces.

## Setting up an SSH tunnel

To set up an SSH tunnel, you can use the following command:

```bash
ssh-keygen -t ed25519 -f ~/.ssh/id_ed25519_framelab -C "framelab"
```

In our case, we called the key `framelab`

Copy the public key to the server:

```bash
ssh-copy-id -i ~/.ssh/id_ed25519_framelab.pub user@server_ip
```

Then, add the configuration to your SSH config file (`~/.ssh/config`):

```bash
Host framelab
    HostName server_ip
    User user
    IdentityFile ~/.ssh/id_ed25519_framelab
    LocalForward 8080 localhost:8080
```
