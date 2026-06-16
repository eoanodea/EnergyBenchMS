## Pre-requisites

These instructions assume you have already set up the repository and its submodules. If you haven't, run the following command in the root of the repository:

```
git submodule update --init --recursive
```

## Otel Demo

```
cp app-specific-config/oteldemo/otel-demo.yaml apps/oteldemo/otel-demo.yaml
```

## Social Network

Add the `social-network.yaml` manifest to the `socialNetwork` directory of the DeathStarBench application:

```
cp app-specific-config/socialnetwork/social-network.yaml apps/deathstarbench/socialNetwork
```

## Train Ticket

Replace `deployment/kubetnetes-manifests/k8s-with-jaeger` with the `k8s-with-jaeger` directory

```
cp -r app-specific-config/trainticket/k8s-with-jaeger apps/train-ticket/deployment/kubetnetes-manifests/k8s-with-jaeger
```

## Online Boutique

The Online Boutique application already functions with the default configuration.
