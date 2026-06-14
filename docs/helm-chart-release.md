<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->
# Helm Chart Release Playbook

The Druid Operator Helm repository is served from the ASF-managed GitHub Pages site:

```text
https://apache.github.io/druid-operator
```

Published files live on the `gh-pages` branch:

```text
index.yaml
helm-releases/
  druid-operator-<chart-version>.tgz
```

## Release Flow

1. Prepare and approve the Apache Druid Operator release using the normal ASF release process.
2. Update `chart/Chart.yaml` so `version` is the chart version being published and `appVersion` matches the operator version.
3. Confirm the chart packages locally:

```bash
make helm-lint
make helm-template
make helm-package
```

The `helm-package` target packages `chart/`, verifies that the archive contains `LICENSE` and `NOTICE`, and generates a local `index.yaml` under `dist/helm`.

4. After the release is approved, run the `Helm Chart` GitHub Actions workflow from the `master` branch with `publish=true`.
5. Verify the published repository:

```bash
helm repo add apache-druid https://apache.github.io/druid-operator
helm repo update
helm search repo apache-druid/druid-operator
```

The publish workflow refuses to overwrite an existing chart package with the same chart version. Bump `chart/Chart.yaml` before publishing a new chart.
