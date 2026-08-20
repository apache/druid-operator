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

# Apache Druid Operator

## Helm Chart

> **Note:** the hosted chart repository at `https://apache.github.io/druid-operator` is not
> published yet (its index is currently empty), so the `helm repo add` flow below does not
> work for now. This also means there is currently no hosted helm repository for this
> chart at all: the repository previously documented by this operator,
> `https://charts.datainfra.io`, is a lapsed domain no longer under the project's control
> and must not be added.

Until a chart release is published, install the chart directly from this repository:

```bash
git clone https://github.com/apache/druid-operator.git
helm -n druid-operator-system upgrade -i --create-namespace cluster-druid-operator ./druid-operator/chart
```

Once the hosted repository is published:

```bash
helm repo add apache-druid https://apache.github.io/druid-operator
helm repo update
helm -n druid-operator-system upgrade -i --create-namespace cluster-druid-operator apache-druid/druid-operator
```
