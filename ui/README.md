# Kubecost Open Source UI
The preferred install path for Kubecost is via Helm chart, and is explained [here](http://docs.kubecost.com/install)

To manually run an open source demo UI, follow the steps below.

## Requirements

* `nodejs >= 12.0.0`
* `npm >= 7.0.0`

## Installation & Running
To run the UI, open a terminal to the `cost-model/ui/` directory (where this README is located) and run

```
npm install
```

This will install required depndencies and build tools. To launch the UI, run

```
BASE_URL=<kubecost-url> npx parcel src/index.html
```

This will launch a development server, serving the UI at `http://localhost:1234` and targeting the data for an instance of
Kubecost running at `<kubecost-url>`. To access an arbitrary Kubecost install, you can use

```
kubectl port-forward deployment/kubecost-cost-analyzer 9090
```

for your choice of namespace and cloud context.
