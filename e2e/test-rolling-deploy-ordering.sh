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

NAMESPACE=${NAMESPACE:-druid}
CR_NAME=rolling-deploy-cluster
HISTORICAL_TIER1_STS="druid-${CR_NAME}-historicalstier1"
HISTORICAL_TIER2_STS="druid-${CR_NAME}-historicalstier2"

CONCURRENT_THRESHOLD=0
POLL_INTERVAL=5
POLL_TIMEOUT=900
REVISION_PICKUP_TIMEOUT=120

cleanup() {
  echo "Cleaning up rolling-deploy test resources."
  kubectl delete -f e2e/configs/druid-rolling-deploy-cr.yaml -n "${NAMESPACE}" --ignore-not-found
  for d in $(kubectl get pods -n "${NAMESPACE}" -l app=druid -l "druid_cr=${CR_NAME}" -o name 2>/dev/null); do
    kubectl wait -n "${NAMESPACE}" "$d" --for=delete --timeout=5m || true
  done
}
trap cleanup EXIT

echo "Test: RollingDeployOrdering => START"

kubectl apply -f e2e/configs/druid-rolling-deploy-cr.yaml -n "${NAMESPACE}"
sleep 10

for sts in "${HISTORICAL_TIER1_STS}" "${HISTORICAL_TIER2_STS}"; do
  kubectl rollout status sts "${sts}" -n "${NAMESPACE}" --timeout=10m
done

echo "Test: RollingDeployOrdering => initial cluster ready"

t1_revision_before=$(kubectl get sts "${HISTORICAL_TIER1_STS}" -n "${NAMESPACE}" -o 'jsonpath={.status.updateRevision}')
t2_revision_before=$(kubectl get sts "${HISTORICAL_TIER2_STS}" -n "${NAMESPACE}" -o 'jsonpath={.status.updateRevision}')
echo "Pre-update revisions: tier1=${t1_revision_before} tier2=${t2_revision_before}"

# Trigger a rolling update by bumping a pod annotation on both historical
# specs. podAnnotations flow into the PodTemplateSpec, so the StatefulSet
# controller creates a new updateRevision and actually rolls the pods.
# (workloadAnnotations only touch the StatefulSet object metadata and do
# NOT change the pod template, so they never produce a new revision.)
update_marker="rolling-deploy-test-$(date +%s)"
kubectl patch druid "${CR_NAME}" -n "${NAMESPACE}" --type=merge -p "{
  \"spec\": {
    \"nodes\": {
      \"historicalstier1\": {\"podAnnotations\": {\"rolling-deploy-test\": \"${update_marker}\"}},
      \"historicalstier2\": {\"podAnnotations\": {\"rolling-deploy-test\": \"${update_marker}\"}}
    }
  }
}"

echo "Test: RollingDeployOrdering => triggered update with marker=${update_marker}"

# Fail fast: wait up to REVISION_PICKUP_TIMEOUT for tier1 to pick up a
# new updateRevision. If it never does, the patch didn't produce a pod
# template change and the rest of the test is pointless.
pickup_deadline=$(( $(date +%s) + REVISION_PICKUP_TIMEOUT ))
while [ "$(date +%s)" -lt "${pickup_deadline}" ]; do
  t1_update=$(kubectl get sts "${HISTORICAL_TIER1_STS}" -n "${NAMESPACE}" -o 'jsonpath={.status.updateRevision}')
  if [ "${t1_update}" != "${t1_revision_before}" ]; then
    echo "tier1 picked up new revision: ${t1_update}"
    break
  fi
  sleep "${POLL_INTERVAL}"
done
if [ "${t1_update}" = "${t1_revision_before}" ]; then
  echo "Test: RollingDeployOrdering => FAILED (tier1 never received a new updateRevision within ${REVISION_PICKUP_TIMEOUT}s; patch may not affect the pod template)"
  exit 1
fi

# Poll the rollout and observe two invariants:
#   (1) At most one historical StatefulSet is mid-rollout
#       (currentRevision != updateRevision) at any poll.
#   (2) tier1 (lex-smaller key) finishes before tier2 starts.
t1_finished_at=""
t2_started_at=""
concurrent_observations=0
deadline=$(( $(date +%s) + POLL_TIMEOUT ))

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
    echo "tier1 finished updating at ${t1_finished_at}"
  fi

  if [ -z "${t2_started_at}" ] && [ "${t2_update}" != "${t2_revision_before}" ] && [ "${t2_updating}" = "true" ]; then
    t2_started_at=$(date +%s)
    echo "tier2 started updating at ${t2_started_at}"
  fi

  # Both tiers have new revisions and neither is mid-rollout.
  if [ "${t1_updating}" = "false" ] && [ "${t2_updating}" = "false" ] \
     && [ "${t1_update}" != "${t1_revision_before}" ] \
     && [ "${t2_update}" != "${t2_revision_before}" ]; then
    echo "Both historicals finished rolling update."
    break
  fi

  sleep "${POLL_INTERVAL}"
done

if [ "$(date +%s)" -ge "${deadline}" ]; then
  echo "Test: RollingDeployOrdering => FAILED (rollout did not complete within ${POLL_TIMEOUT}s)"
  exit 1
fi

if [ "${concurrent_observations}" -gt "${CONCURRENT_THRESHOLD}" ]; then
  echo "Test: RollingDeployOrdering => FAILED (observed ${concurrent_observations} polls where both historicals were updating concurrently; threshold=${CONCURRENT_THRESHOLD})"
  exit 1
fi

if [ -z "${t1_finished_at}" ] || [ -z "${t2_started_at}" ]; then
  echo "Note: did not observe both transitions (t1_finished_at='${t1_finished_at}', t2_started_at='${t2_started_at}'). Rollout may have been faster than poll interval; relying on concurrency invariant alone."
elif [ "${t2_started_at}" -lt "${t1_finished_at}" ]; then
  echo "Test: RollingDeployOrdering => FAILED (tier2 started at ${t2_started_at} BEFORE tier1 finished at ${t1_finished_at})"
  exit 1
else
  echo "Order verified: tier1 finished at ${t1_finished_at}, tier2 started at ${t2_started_at}."
fi

echo "Test: RollingDeployOrdering => SUCCESS"
