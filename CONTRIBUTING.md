# Contributing to our project

Thanks for your help improving the OpenCost project! There are many ways to contribute to the project, including the following:

* contributing or providing feedback on the [OpenCost Spec](https://github.com/opencost/opencost/tree/develop/spec)
* contributing documentation here or to the [OpenCost website](https://github.com/opencost/opencost-website)
* joining the discussion in the [CNCF Slack](https://slack.cncf.io/) in the [#opencost](https://cloud-native.slack.com/archives/C03D56FPD4G) channel
* keep up with community events using our [Calendar](https://bit.ly/opencost-calendar)
* participating in the fortnightly [OpenCost Working Group](https://bit.ly/opencost-calendar) meetings ([notes here](https://bit.ly/opencost-meeting))
* committing software via the workflow below

## Getting Help

If you have a question about OpenCost or have encountered problems using it,
you can start by asking a question on [CNCF Slack](https://slack.cncf.io/) in the [#opencost](https://cloud-native.slack.com/archives/C03D56FPD4G) channel or attend the biweekly [OpenCost Working Group community meeting](https://bit.ly/opencost-meeting) from the [Community Calendar](https://bit.ly/opencost-calendar) to discuss OpenCost development.

## Workflow

This repository's contribution workflow follows a typical open-source model:

- [Fork](https://docs.github.com/en/get-started/quickstart/fork-a-repo) this repository
- Work on the forked repository
- Open a pull request to [merge the fork back into this repository](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request-from-a-fork)

## Building OpenCost

Follow these steps to build the OpenCost cost-model and UI from source and
deploy. The provided build tooling is natively multi-architecture (built images
will run on both AMD64 and ARM64 clusters).

Dependencies:
1. Docker (with `buildx`)
2. [just](https://github.com/casey/just) (if you don't want to install it , Just read the `justfile` and run the commands manually)
3. Multi-arch `buildx` builders set up via https://github.com/tonistiigi/binfmt
4. `manifest-tool` via https://github.com/estesp/manifest-tool
4. `npm` (if you want to build the UI)

### Build the backend

1. `just build "<repo>/opencost:<tag>"`
2. Edit the [pulled image](https://github.com/opencost/opencost/blob/develop/kubernetes/opencost.yaml#L145) in the `kubernetes/opencost.yaml` to `<repo>/opencost:<tag>`
3. Set [this environment variable](https://github.com/opencost/opencost/blob/develop/kubernetes/opencost.yaml#L155) to the address of your Prometheus server

### Build the frontend
1. `cd ui && just build "<repo>/opencost-ui:<tag>"`
2. Edit the [pulled image](https://github.com/opencost/opencost/blob/develop/kubernetes/opencost.yaml#L162) in the `kubernetes/opencost.yaml` to `<repo>/opencost-ui:<tag>`

### Deploy to a cluster

1. `kubectl create namespace opencost`
2. `kubectl apply -f kubernetes/opencost --namespace opencost`
3. `kubectl -n opencost port-forward service/opencost 9090 9003`

To test, build the OpenCost containers and then push them to a Kubernetes cluster with a running Prometheus.

To confirm that the server and UI are running, you can hit [http://localhost:9090](http://localhost:9090) to access the OpenCost UI.
You can test the server API with `curl http://localhost:9003/allocation/compute -d window=60m -G`.

## Running locally

To run locally cd into `cmd/costmodel` and `go run main.go`

OpenCost requires a connection to Prometheus in order to operate so setting the environment variable `PROMETHEUS_SERVER_ENDPOINT` is required.
In order to expose Prometheus to OpenCost it may be required to port-forward using kubectl to your Prometheus endpoint.

For example:

```bash
kubectl port-forward svc/prometheus-server 9080:80
```

This would expose Prometheus on port 9080 and allow setting the environment variable as so:

```bash
PROMETHEUS_SERVER_ENDPOINT="http://127.0.0.1:9080"
```

If you want to run with a specific kubeconfig the environment variable `KUBECONFIG` can be used. OpenCost will attempt to connect to your Kubernetes cluster in a similar fashion as kubectl so the env is not required. The order of precedence is `KUBECONFIG` > default kubeconfig file location ($HOME/.kube/config) > in cluster config

Example:

```bash
export KUBECONFIG=~/.kube/config
```

An example of the full command:

```bash
PROMETHEUS_SERVER_ENDPOINT="http://127.0.0.1:9090" go run main.go
```

## Testing code

Testing is provided by the `just test` command which runs
- [go test](https://go.dev/doc/tutorial/add-a-test) unit tests
- [go vet](https://pkg.go.dev/cmd/vet) code quality checks

## Running the integration tests

To run these tests:

- Make sure you have a kubeconfig that can point to your cluster, and have permissions to create/modify a namespace called "test"
- Connect to your the Prometheus OpenCost emits to on localhost:9003:
  `kubectl port-forward --namespace opencost service/prometheus-server 9003:80`
- Temporary workaround: Copy the default.json file in this project at cloud/default.json to /models/default.json on the machine your test is running on. TODO: fix this and inject the cloud/default.json path into provider.go.
- Navigate to cost-model/test
- Run `go test -timeout 700s` from the testing directory. The tests right now take about 10 minutes (600s) to run because they bring up and down pods and wait for Prometheus to scrape data about them.

## Certificate of Origin

By contributing to this project, you certify that your contribution was created in whole or in part by you and that you have the right to submit it under the open source license indicated in the project. In other words, please confirm that you, as a contributor, have the legal right to make the contribution. This is enforced on Pull Requests and requires `Signed-off-by` with the email address for the author in the commit message.

## Committing

Please write a commit message with Fixes Issue # if there is an outstanding issue that is fixed. It’s okay to submit a PR without a corresponding issue; just please try to be detailed in the description of the problem you’re addressing.

Please run `go fmt` on the project directory. Lint can be okay (for example, comments on exported functions are nice but not required on the server).

Please reach us on [CNCF Slack](https://slack.cncf.io/) in the [#opencost](https://cloud-native.slack.com/archives/C03D56FPD4G) channel or attend the biweekly [OpenCost Working Group community meeting](https://bit.ly/opencost-meeting) from the [Community Calendar](https://bit.ly/opencost-calendar) to discuss OpenCost development.

## Third Party Attributions

The THIRD_PARTY_LICENSES.txt file should contain the up-to-date license, copyright, and notice information for dependencies used by Opencost.
When adding, updating, or removing dependencies, please update the associate section(s) of the [THIRD_PARTY_LICENSES.txt](THIRD_PARTY_LICENSES.txt) file. 

For example, the `github.com/opencost/opencost/core` dependency contains the following third party attributions.
The license associated with the SPDX identifier should also be included in the attributions file, if it is not already present.

```
== Dependency
github.com/opencost/opencost/core

== License Type
SPDX:Apache-2.0

== Copyright
Copyright 2019 - 2022 Stackwatch Incorporated. All Rights Reserved.
Copyright 2022 - 2024 Cloud Native Computing Foundation

== Notices
OpenCost
Copyright 2022 - 2024 Cloud Native Computing Foundation

This product includes software developed at
The Cloud Native Computing Foundation (http://www.cncf.io).

The Initial Developer of some parts of the specification and project is
Kubecost (http://www.kubecost.com).
Copyright 2019 - 2022 Stackwatch Incorporated. All Rights Reserved.
```