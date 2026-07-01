# Workstation Changes

<aside>
⚠️

After system restart run

```bash
docker run cadvisor prometheus node-exporter
```

</aside>

# 1. Setup in windows

Allocated 150 GB = 150 × 1024 = 153600 MB of space

Disabled windows fast startup

```markdown
powercfg -h off
```

```markdown
bcdedit /set {current} safeboot minimal
```

# 2. BIOS

Storage → SATA enabled AHCI/NVMe (previously RAID)

Boot Configuration → Secure Boot disabled

# 3. Windows (Safe Mode)

```markdown
bcdedit /deletevalue {current} safeboot
```

# 4. Ubuntu

servername: framelab

username: eoan

password: eoan

```markdown
sudo apt update
sudo apt upgrade -y 
```

Time sync

```bash
sudo timedatectl set-ntp true
timedatectl # should output the right time

#if not follow these commands
sudo timedatectl set-timezone Europe/Rome
sudo timedatectl set-ntp false
sudo date -s "2024-03-30 10:00:00" # Manual time & date set
sudo timedatectl set-ntp true
```

If that STILL doesn’t work you can change the time server

```yaml
sudo vim /etc/systemd/timesyncd.conf
```

and add these lines

```yaml
NTP=time.google.com time.cloudflare.com
FallbackNTP=pool.ntp.org
```

Then restart sync service

```yaml
sudo systemctl restart systemd-timesyncd
```

wait 20 seconds or so and check again

```yaml
timedatectl # pray for the correct time
```

Docker Config

```markdown
# Add Docker's official GPG key:
sudo apt install ca-certificates curl
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc

# Add the repository to Apt sources:
sudo tee /etc/apt/sources.list.d/docker.sources <<EOF
Types: deb
URIs: https://download.docker.com/linux/ubuntu
Suites: $(. /etc/os-release && echo "${UBUNTU_CODENAME:-$VERSION_CODENAME}")
Components: stable
Architectures: $(dpkg --print-architecture)
Signed-By: /etc/apt/keyrings/docker.asc
EOF

sudo apt update
```

Docker install

```markdown
sudo apt install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

# Verify installation
sudo systemctl status docker
```

Add user

```markdown
sudo groupadd docker
sudo usermod -aG docker $USER
```

Then logout and log back in again

## Prometheus Setup

```markdown
mkdir ~/observability
vim ~/observability/prometheus.yml
```

```yaml
global:
  scrape_interval: 5s

scrape_configs:
  - job_name: 'node'
    static_configs:
      - targets: ['node-exporter:9100']
	- job_name: 'cadvisor'
	  static_configs:
	    - targets: ['cadvisor:8080']
```

```markdown
# Create network, run node-exporter & prometheus
docker network create monitoring

docker run -d \
  --name=node-exporter \
  --network=monitoring \
  -p 9100:9100 \
  prom/node-exporter
  
  docker run -d \
  --name=prometheus \
  --network=monitoring \
  -p 9090:9090 \
  -v ~/observability/prometheus.yml:/etc/prometheus/prometheus.yml \
  prom/prometheus
  
  VERSION=0.55.1 # use the latest release version from https://github.com/google/cadvisor/releases
sudo docker run -d \
  --volume=/:/rootfs:ro \
  --volume=/var/run:/var/run:ro \
  --volume=/sys:/sys:ro \
  --volume=/var/lib/docker/:/var/lib/docker:ro \
  --volume=/dev/disk/:/dev/disk:ro \
  --publish=8081:8080 \
  --detach=true \
  --name=cadvisor \
  --network=monitoring \
  --privileged \
  --device=/dev/kmsg \
  ghcr.io/google/cadvisor:$VERSION # for versions prior to v0.53.0, use gcr.io/cadvisor/cadvisor instead
```

# Static IP Setup (workstation)

Edit the file `/etc/netplan/00-installer-config.yaml` (or whatever file is inside of `/etc/netplan`  and change to the following content

```yaml
network:
  version: 2
  ethernets:
    enp2s0:
      dhcp4: true
      addresses:
        - 192.168.0.100/24 # The IP you want for the workstation
      routes:
        - to: default
          via: 192.168.0.1 # the IP of the router
      nameservers:
        addresses: [8.8.8.8, 1.1.1.1]
```

# CPU Performance Setup

Governor = performance

Turbo = off

Verify across all cores

```bash
sudo apt install linux-tools-common linux-tools-generic linux-tools-$(uname -r)
# set performance mode
sudo cpupower frequency-set -g performance
cpupower frequency-info # you should see The governor "performance"
```

Disable turbo

```bash
echo 1 | sudo tee /sys/devices/system/cpu/intel_pstate/no_turbo

# verify
cat /sys/devices/system/cpu/intel_pstate/no_turbo #output 1
```

Persistance on startup

```bash
sudo vim /etc/rc.local
```

Input this code into the file

```bash
#!/bin/bash
cpupower frequency-set -g performance
echo 1 > /sys/devices/system/cpu/intel_pstate/no_turbo
exit 0
```

Save & Quit and then change it’s permissions

```bash
sudo chmod +x /etc/rc.local
```

Stress test

```bash
sudo apt install stress
stress --cpu 8 --timeout 30
```

Simultaneously run this command in another terminal

```bash
watch -n 1 "grep \"cpu MHz\" /proc/cpuinfo"
```

CPU Mhz should be (mostly) consistent

# Install Kubernetes

Installing k3s for low overhead, lightweight & widely used

Disable firewall

```bash
ufw disable
```

Install and config k3s

```bash
curl -sfL https://get.k3s.io | sh -
sudo k3s kubectl get nodes # you should see status ready
```

Set up ownership & permissions for your user

```bash
sudo cp /etc/rancher/k3s/k3s.yaml ~/.kube/config
sudo chown $(id -u):$(id -g) ~/.kube/config
chmod 600 ~/.kube/config
```

Add to bash path

```bash
vim ~/.bashrc
```

```bash
export KUBECONFIG=~/.kube/config # add this line
```

At this point if you close and reopen your SSH connection to the machine, and type `kubectl get nodes` you should see `STATUS Ready`

# Installing Helm 3.20.0

```bash
sudo apt-get install curl gpg apt-transport-https --yes
curl -fsSL https://packages.buildkite.com/helm-linux/helm-debian/gpgkey | gpg --dearmor | sudo tee /usr/share/keyrings/helm.gpg > /dev/null
echo "deb [signed-by=/usr/share/keyrings/helm.gpg] https://packages.buildkite.com/helm-linux/helm-debian/any/ any main" | sudo tee /etc/apt/sources.list.d/helm-stable-debian.list
sudo apt-get update
sudo apt-get install helm

# verify
helm version # v3.20.0
```

# Install Kepler

```bash
helm install kepler oci://quay.io/sustainable_computing_io/charts/kepler \
  --version 0.11.1 \
  --namespace kepler \
  --create-namespace
```

Verify

```bash
kubectl get pods -n kepler
```

If you see `Error` or `CrashLoopBackOff`, follow these steps

```bash
kubectl create clusterrolebinding kepler-kubelet-access --clusterrole=system:kubelet-api-admin --serviceaccount=kepler:kepler

# restart pod
kubectl delete pod -n kepler -l app.kubernetes.io/name=kepler

# verify again
kubectl get pods -n kepler #Status Running
```

## Connect Kepler to Prometheus

Get the IP and port of kepler

```bash
kubectl get svc -n kepler # in my case was 28282/TCP
```

```bash
kubectl port-forward -n kepler svc/kepler 28282:28282
```

In another terminal, run 

```bash
curl http://localhost:28282/metrics | head
```

And check if you see some metrics e.g.

```bash
# TYPE go_gc_duration_seconds summary
go_gc_duration_seconds{quantile="0"} 3.8417e-05
go_gc_duration_seconds{quantile="0.25"} 6.0978e-05
go_gc_duration_seconds{quantile="0.5"} 6.5416e-05
go_gc_duration_seconds{quantile="0.75"} 7.0016e-05
go_gc_duration_seconds{quantile="1"} 0.000336571
go_gc_duration_seconds_sum 0.010373048
go_gc_duration_seconds_count 149
```

If you got metrics and then `curl: (23) Failure writing output to destination`, that’s grand don’t worry

Next, we need to expose Kepler via NodePort

```yaml
kubectl patch svc kepler -n kepler -p '{"spec": {"type": "NodePort"}}'
```

Now run 

```yaml
kubectl get svc -n kepler # look for the second port e.g. 3xxxx
```

Now verify that with curl

```yaml
curl http://localhost:30779/metrics | head
```

Update the `observability/prometheus.yml` file to the following

```yaml
global:
  scrape_interval: 5s

scrape_configs:
  - job_name: 'node'
    static_configs:
      - targets: ['node-exporter:9100']
  - job_name: 'cadvisor'
    static_configs:
      - targets: ['cadvisor:8080']
  - job_name: 'kepler'
	  static_configs:
	    - targets: ['host_ip:30779']
```

And then `docker restart prometheus`

next step is check with prometheus UI and make sure all the kepler metrics are there

# Simple test kubernetes app

Create a new directory `mkdir -p ~/apps/simple-web`

Create two files - `deployment.yml` and `service.yml`

```yaml
# deployment.yml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: simple-web
spec:
  replicas: 1
  selector:
    matchLabels:
      app: simple-web
  template:
    metadata:
      labels:
        app: simple-web
    spec:
      containers:
        - name: nginx
          image: nginx:latest
          ports:
            - containerPort: 80

# service.yml
apiVersion: v1
kind: Service
metadata:
  name: simple-web
spec:
  type: NodePort
  selector:
    app: simple-web
  ports:
    - port: 80
      targetPort: 80
      nodePort: 30007

```

Apply both files

```yaml
kubectl apply -f deployment.yaml
kubectl apply -f service.yaml
```

And run 

```yaml
kubectl get pods # Pod running
kubectl get svc #service simple-web with 30007
```

Final check

```yaml
curl http://localhost:30007 # basic nginx html response
```

From another machine preferaby, run locust

```yaml
pip install locust
```

```yaml
mkdir ./simple-web-locust # from whereever you are or organise it somewhere
touch vim ./simple-web-locust/locustfile.py
```

And drop in this code

```yaml
from locust import HttpUser, task

class SimpleWebUser(HttpUser):
    @task
    def index(self):
        self.client.get("/")
```

Now run locust 

```yaml
locust -H http://WORKSTATION_IP:30007 # The IP of the simple-web app running in kubernetes
```

And run a quick experiment, for example `60 users` over `120 seconds`

Query in prometheus and see the energy consumption by containers

```yaml
sum by (container_name) (
  rate(kepler_container_cpu_joules_total[1m])
)
```

You should see some sort of increase over time in energy - now you’re cooking with gas

# On Control machine - Install kubectl

```bash
# control machine
sudo apt update
sudo apt install -y ca-certificates curl

sudo mkdir -p /etc/apt/keyrings
curl -fsSL https://pkgs.k8s.io/core:/stable:/v1.29/deb/Release.key \
  | gpg --dearmor \
  | sudo tee /etc/apt/keyrings/kubernetes-apt-keyring.gpg > /dev/null

echo 'deb [signed-by=/etc/apt/keyrings/kubernetes-apt-keyring.gpg] https://pkgs.k8s.io/core:/stable:/v1.29/deb/ /' \
  | sudo tee /etc/apt/sources.list.d/kubernetes.list
  
sudo apt update
sudo apt install -y kubectl

kubectl version --client # verify
```

On the workstation, get the k3s.yaml file

```bash
# workstation
sudo cat /etc/rancher/k3s/k3s.yaml # copy the contents
```

Create a config file on the controller

```bash
# control machine
mkdir -p ~/.kube
vim ~/.kube/config # paste the contents here
```

Make sure to edit the `server` IP to match the IP of the measured machine

Now try to run the (simple) automated experiment runner on the control machine located at this repo `https://github.com/eoanodea/EnergyBenchMS`