#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# E2E test: Deterministic Rolling Deploy Ordering
#
# Verifies the fix that ensures, when rollingDeploy is enabled and multiple
# StatefulSets share the same NodeType, the operator rolls them out one at a
# time in a deterministic, lexicographic-by-key order. Without the fix, the
# two historical StatefulSets in the test fixture could update concurrently
# because Go map iteration ordering would flap across reconciles.

set -o errexit
set -o pipefail
set -x

# NAMESPACE is exported by e2e.sh; default if running standalone.
NAMESPACE=${NAMESPACE:-druid}
CR_NAME=rolling-deploy-cluster
HISTORICAL_TIER1_STS="druid-${CR_NAME}-historicalstier1"
HISTORICAL_TIER2_STS="druid-${CR_NAME}-historicalstier2"

# concurrent_observations_threshold is the maximum number of polling
# windows in which both historical StatefulSets are allowed to be
# updating at the same time during the rollout. With the fix this
# must remain 0 throughout the rollout.
concurrent_observations_threshold=0

# poll_interval_seconds and poll_timeout_seconds bound the rollout
# observation loop so the test fails fast on a stuck cluster.
poll_interval_seconds=5
poll_timeout_seconds=900

echo "Test: RollingDeployOrdering => START"

# Apply the test fixture and wait for both historical StatefulSets to
# become Ready before mutating anything. RollingDeploy gating only
# applies to updates after Generation > 1.
kubectl apply -f e2e/configs/druid-rolling-deploy-cr.yaml -n "${NAMESPACE}"
sleep 10

for sts in "${HISTORICAL_TIER1_STS}" "${HISTORICAL_TIER2_STS}"; do
  kubectl rollout status sts "${sts}" -n "${NAMESPACE}" --timeout=10m
done

echo "Test: RollingDeployOrdering => initial cluster ready"

# Capture the revisions both historicals are running at before the
# mutation so we can detect when each one begins and finishes its update.
t1_revision_before=$(kubectl get sts "${HISTORICAL_TIER1_STS}" -n "${NAMESPACE}" -o 'jsonpath={.status.updateRevision}')
t2_revision_before=$(kubectl get sts "${HISTORICAL_TIER2_STS}" -n "${NAMESPACE}" -o 'jsonpath={.status.updateRevision}')
echo "Pre-update revisions: tier1=${t1_revision_before} tier2=${t2_revision_before}"

# Trigger a rolling update by bumping a workload annotation on both
# historical specs. A workloadAnnotation change forces the operator to
# update both StatefulSets, which is exactly the scenario where shared
# NodeType ordering must remain deterministic.
update_marker="rolling-deploy-test-$(date +%s)"
kubectl patch druid "${CR_NAME}" -n "${NAMESPACE}" --type=merge -p "{
  \"spec\": {
    \"nodes\": {
      \"historicalstier1\": {\"workloadAnnotations\": {\"rolling-deploy-test\": \"${update_marker}\"}},
      \"historicalstier2\": {\"workloadAnnotations\": {\"rolling-deploy-test\": \"${update_marker}\"}}
    }
  }
}"

echo "Test: RollingDeployOrdering => triggered update with marker=${update_marker}"

# Poll the rollout and observe two invariants:
#   (1) At any single moment, AT MOST ONE historical StatefulSet should
#       have CurrentRevision != UpdateRevision (i.e. be mid-rollout).
#   (2) historicalstier1 (lex-smaller key) must finish its update before
#       historicalstier2 starts its update.
t1_finished_at=""
t2_started_at=""
concurrent_observations=0
deadline=$(( $(date +%s) + poll_timeout_seconds ))

while [ "$(date +%s)" -lt "${deadline}" ]; do
  t1_current=$(kubectl get sts "${HISTORICAL_TIER1_STS}" -n "${NAMESPACE}" -o 'jsonpath={.status.currentRevision}')
  t1_update=$(kubectl  get sts "${HISTORICAL_TIER1_STS}" -n "${NAMESPACE}" -o 'jsonpath={.status.updateRevision}')
  t2_current=$(kubectl get sts "${HISTORICAL_TIER2_STS}" -n "${NAMESPACE}" -o 'jsonpath={.status.currentRevision}')
  t2_update=$(kubectl  get sts "${HISTORICAL_TIER2_STS}" -n "${NAMESPACE}" -o 'jsonpath={.status.updateRevision}')

  t1_updating="false"
  t2_updating="false"
  if [ "${t1_current}" != "${t1_update}" ]; then t1_updating="true"; fi
  if [ "${t2_current}" != "${t2_update}" ]; then t2_updating="true"; fi

  echo "poll: t1(updating=${t1_updating} cur=${t1_current} upd=${t1_update}) t2(updating=${t2_updating} cur=${t2_current} upd=${t2_update})"

  if [ "${t1_updating}" = "true" ] && [ "${t2_updating}" = "true" ]; then
    concurrent_observations=$(( concurrent_observations + 1 ))
    echo "WARN: both historicals are updating in the same poll (count=${concurrent_observations})"
  fi

  if [ -z "${t1_finished_at}" ] && [ "${t1_update}" != "${t1_revision_before}" ] && [ "${t1_updating}" = "false" ]; then
    t1_finished_at=$(date +%s)
    echo "t1 finished updating at ${t1_finished_at}"
  fi

  if [ -z "${t2_started_at}" ] && [ "${t2_update}" != "${t2_revision_before}" ] && [ "${t2_updating}" = "true" ]; then
    t2_started_at=$(date +%s)
    echo "t2 started updating at ${t2_started_at}"
  fi

  if [ "${t1_updating}" = "false" ] && [ "${t2_updating}" = "false" ] \
     && [ "${t1_update}" != "${t1_revision_before}" ] \
     && [ "${t2_update}" != "${t2_revision_before}" ]; then
    echo "Both historicals finished rolling update."
    break
  fi

  sleep "${poll_interval_seconds}"
done

if [ "$(date +%s)" -ge "${deadline}" ]; then
  echo "Test: RollingDeployOrdering => FAILED (rollout did not complete within ${poll_timeout_seconds}s)"
  exit 1
fi

# Invariant 1: deterministic ordering => at most threshold concurrent windows.
if [ "${concurrent_observations}" -gt "${concurrent_observations_threshold}" ]; then
  echo "Test: RollingDeployOrdering => FAILED (observed ${concurrent_observations} polls where both historicals were updating concurrently; threshold=${concurrent_observations_threshold})"
  exit 1
fi

# Invariant 2: t1 (lex-smaller key) must complete before t2 starts.
if [ -z "${t1_finished_at}" ] || [ -z "${t2_started_at}" ]; then
  echo "Note: did not observe both transitions (t1_finished_at='${t1_finished_at}', t2_started_at='${t2_started_at}'). Rollout may have been faster than poll interval; relying on concurrency invariant alone."
elif [ "${t2_started_at}" -lt "${t1_finished_at}" ]; then
  echo "Test: RollingDeployOrdering => FAILED (t2 started updating at ${t2_started_at} BEFORE t1 finished at ${t1_finished_at})"
  exit 1
else
  echo "Order verified: t1 finished at ${t1_finished_at}, t2 started at ${t2_started_at} (t1-before-t2)."
fi

echo "Cleaning up rolling-deploy test resources."
kubectl delete -f e2e/configs/druid-rolling-deploy-cr.yaml -n "${NAMESPACE}" --ignore-not-found
for d in $(kubectl get pods -n "${NAMESPACE}" -l app=druid -l "druid_cr=${CR_NAME}" -o name); do
  kubectl wait -n "${NAMESPACE}" "$d" --for=delete --timeout=5m || true
done

echo "Test: RollingDeployOrdering => SUCCESS"
