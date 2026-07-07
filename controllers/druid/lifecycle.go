/*
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
*/
package druid

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/apache/druid-operator/apis/druid/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const deploymentLifecycleTimeoutReasonPrefix = "Deployment lifecycle exceeded timeout"

func isTerminalLifecyclePhase(phase v1alpha1.DeploymentLifecyclePhase) bool {
	return phase == v1alpha1.DeploymentLifecycleSucceeded
}

type lifecycleDependencies struct {
	now     func() time.Time
	timeout time.Duration
}

func defaultLifecycleDependencies() lifecycleDependencies {
	return lifecycleDependencies{
		now:     time.Now,
		timeout: LookupDeploymentLifecycleTimeout(),
	}
}

func reconcileDeploymentLifecycle(
	ctx context.Context,
	sdk client.Client,
	drd *v1alpha1.Druid,
	emitEvent EventEmitter,
) error {
	return reconcileDeploymentLifecycleWithDeps(
		ctx,
		sdk,
		drd,
		emitEvent,
		defaultLifecycleDependencies(),
	)
}

func reconcileDeploymentLifecycleWithDeps(
	ctx context.Context,
	sdk client.Client,
	drd *v1alpha1.Druid,
	emitEvent EventEmitter,
	deps lifecycleDependencies,
) error {
	snapshot, err := collectManagedResourceSnapshot(ctx, sdk, drd)
	if err != nil {
		return err
	}
	if !snapshot.hasWorkloads() {
		return nil
	}

	return reconcileDeploymentLifecycleWithSnapshot(drd, snapshot, deps, func(updated v1alpha1.DeploymentLifecycleStatus) error {
		return patchDeploymentLifecycleStatus(ctx, sdk, drd, updated, emitEvent)
	})
}

func reconcileDeploymentLifecycleWithSnapshot(
	drd *v1alpha1.Druid,
	snapshot *managedResourceSnapshot,
	deps lifecycleDependencies,
	patchStatus func(v1alpha1.DeploymentLifecycleStatus) error,
) error {
	current := drd.Status.DeploymentLifecycle
	if current.ObservedGeneration == drd.Generation && isTerminalLifecyclePhase(current.Phase) {
		return nil
	}

	updated := buildLifecycleStatusForGeneration(drd, current)
	if updated.StartedAt == nil {
		now := metav1.NewTime(deps.now())
		updated.StartedAt = &now
	}

	workloadsReady, reason, err := areManagedWorkloadsReadyFromSnapshot(snapshot)
	if err != nil {
		return err
	}
	if !workloadsReady {
		return patchStatus(withDeploymentLifecycleInProgress(updated, timeoutAwareReason(updated, reason, deps)))
	}

	if updated.Phase == v1alpha1.DeploymentLifecycleSucceeded {
		return nil
	}

	updated = withDeploymentLifecycleSucceeded(updated, deps.now())
	return patchStatus(updated)
}

func withDeploymentLifecycleInProgress(
	status v1alpha1.DeploymentLifecycleStatus,
	reason string,
) v1alpha1.DeploymentLifecycleStatus {
	status.Phase = v1alpha1.DeploymentLifecycleInProgress
	status.Reason = reason
	status.CompletedAt = nil
	return status
}

func withDeploymentLifecycleSucceeded(
	status v1alpha1.DeploymentLifecycleStatus,
	now time.Time,
) v1alpha1.DeploymentLifecycleStatus {
	completedAt := metav1.NewTime(now)
	status.Phase = v1alpha1.DeploymentLifecycleSucceeded
	status.Reason = "Deployment lifecycle completed after managed workloads became ready"
	status.CompletedAt = &completedAt
	return status
}

func buildLifecycleStatusForGeneration(
	drd *v1alpha1.Druid,
	current v1alpha1.DeploymentLifecycleStatus,
) v1alpha1.DeploymentLifecycleStatus {
	updated := current
	updated.ObservedGeneration = drd.Generation
	updated.CompletedAt = nil
	if current.ObservedGeneration != drd.Generation {
		updated.StartedAt = nil
	}
	return updated
}

type managedResourceSnapshot struct {
	deployments  []appsv1.Deployment
	statefulSets []appsv1.StatefulSet
}

func (s *managedResourceSnapshot) hasWorkloads() bool {
	return s != nil && (len(s.deployments) > 0 || len(s.statefulSets) > 0)
}

func collectManagedResourceSnapshot(
	ctx context.Context,
	sdk client.Client,
	drd *v1alpha1.Druid,
) (*managedResourceSnapshot, error) {
	listOpts := []client.ListOption{
		client.InNamespace(drd.Namespace),
		client.MatchingLabels(makeLabelsForDruid(drd)),
	}

	deployments := &appsv1.DeploymentList{}
	if err := sdk.List(ctx, deployments, listOpts...); err != nil {
		return nil, err
	}

	statefulSets := &appsv1.StatefulSetList{}
	if err := sdk.List(ctx, statefulSets, listOpts...); err != nil {
		return nil, err
	}

	return &managedResourceSnapshot{
		deployments:  deployments.Items,
		statefulSets: statefulSets.Items,
	}, nil
}

func timeoutAwareReason(
	status v1alpha1.DeploymentLifecycleStatus,
	reason string,
	deps lifecycleDependencies,
) string {
	if deps.timeout <= 0 || status.StartedAt == nil || deps.now().Sub(status.StartedAt.Time) < deps.timeout {
		return reason
	}

	return fmt.Sprintf("%s of %s; still %s", deploymentLifecycleTimeoutReasonPrefix, deps.timeout, waitingReasonFragment(reason))
}

func waitingReasonFragment(reason string) string {
	if reason == "" {
		return "waiting for Druid workloads to roll out"
	}
	if strings.HasPrefix(reason, "Waiting ") {
		return "waiting " + strings.TrimPrefix(reason, "Waiting ")
	}
	return reason
}

func deploymentLifecycleTimedOut(status v1alpha1.DeploymentLifecycleStatus) bool {
	return status.Phase == v1alpha1.DeploymentLifecycleInProgress &&
		strings.HasPrefix(status.Reason, deploymentLifecycleTimeoutReasonPrefix)
}

func areManagedWorkloadsReady(ctx context.Context, sdk client.Client, drd *v1alpha1.Druid) (bool, string, error) {
	snapshot, err := collectManagedResourceSnapshot(ctx, sdk, drd)
	if err != nil {
		return false, "", err
	}
	return areManagedWorkloadsReadyFromSnapshot(snapshot)
}

func areManagedWorkloadsReadyFromSnapshot(snapshot *managedResourceSnapshot) (bool, string, error) {
	for _, deployment := range snapshot.deployments {
		for _, condition := range deployment.Status.Conditions {
			if condition.Type == appsv1.DeploymentReplicaFailure {
				return false, fmt.Sprintf("Waiting for Deployment [%s] replica failure to clear: %s", deployment.Name, condition.Reason), nil
			}
		}
		specReplicas := int32(1)
		if deployment.Spec.Replicas != nil {
			specReplicas = *deployment.Spec.Replicas
		}
		if deployment.Status.ObservedGeneration < deployment.Generation {
			return false, fmt.Sprintf("Waiting for Deployment [%s] controller to observe generation", deployment.Name), nil
		}
		if deployment.Status.UpdatedReplicas != specReplicas ||
			deployment.Status.ReadyReplicas != specReplicas ||
			deployment.Status.Replicas != specReplicas ||
			deployment.Status.UnavailableReplicas != 0 {
			return false, fmt.Sprintf("Waiting for Deployment [%s] rollout", deployment.Name), nil
		}
	}

	for _, statefulSet := range snapshot.statefulSets {
		specReplicas := int32(1)
		if statefulSet.Spec.Replicas != nil {
			specReplicas = *statefulSet.Spec.Replicas
		}
		if statefulSet.Status.ObservedGeneration < statefulSet.Generation {
			return false, fmt.Sprintf("Waiting for StatefulSet [%s] controller to observe generation", statefulSet.Name), nil
		}
		if statefulSet.Status.CurrentRevision != statefulSet.Status.UpdateRevision ||
			statefulSet.Status.UpdatedReplicas != specReplicas {
			return false, fmt.Sprintf("Waiting for StatefulSet [%s] revision rollout", statefulSet.Name), nil
		}
		if statefulSet.Status.ReadyReplicas != specReplicas {
			return false, fmt.Sprintf("Waiting for StatefulSet [%s] ready replicas", statefulSet.Name), nil
		}
	}

	return true, "", nil
}

func patchDeploymentLifecycleStatus(
	ctx context.Context,
	sdk client.Client,
	drd *v1alpha1.Druid,
	updated v1alpha1.DeploymentLifecycleStatus,
	emitEvent EventEmitter,
) error {
	current := drd.Status.DeploymentLifecycle
	if reflect.DeepEqual(current, updated) {
		return nil
	}

	patchBytes, err := json.Marshal(map[string]interface{}{
		"status": map[string]interface{}{
			"deploymentLifecycle": deploymentLifecycleStatusPatch(updated),
		},
	})
	if err != nil {
		return fmt.Errorf("failed to serialize deployment lifecycle status patch: %v", err)
	}

	if err := writers.Patch(ctx, sdk, drd, drd, true, client.RawPatch(types.MergePatchType, patchBytes), emitEvent); err != nil {
		return err
	}

	drd.Status.DeploymentLifecycle = updated
	emitDeploymentLifecycleEvent(drd, emitEvent, current, updated)
	return nil
}

func deploymentLifecycleStatusPatch(status v1alpha1.DeploymentLifecycleStatus) map[string]interface{} {
	return map[string]interface{}{
		"phase":              status.Phase,
		"reason":             status.Reason,
		"observedGeneration": status.ObservedGeneration,
		"startedAt":          status.StartedAt,
		"completedAt":        status.CompletedAt,
	}
}

func emitDeploymentLifecycleEvent(
	drd *v1alpha1.Druid,
	emitEvent EventEmitter,
	previous, current v1alpha1.DeploymentLifecycleStatus,
) {
	if previous.Phase == current.Phase &&
		previous.ObservedGeneration == current.ObservedGeneration &&
		previous.Reason == current.Reason {
		return
	}

	msg := fmt.Sprintf("observedGeneration=%d phase=%s reason=%s", current.ObservedGeneration, current.Phase, current.Reason)
	if deploymentLifecycleTimedOut(current) && !deploymentLifecycleTimedOut(previous) {
		emitEvent.EmitEventGeneric(drd, string(druidDeploymentLifecycleTimedOut), msg, errors.New(current.Reason))
		return
	}

	switch current.Phase {
	case v1alpha1.DeploymentLifecyclePending, v1alpha1.DeploymentLifecycleInProgress:
		emitEvent.EmitEventGeneric(drd, string(druidDeploymentLifecycleStarted), msg, nil)
	case v1alpha1.DeploymentLifecycleSucceeded:
		emitEvent.EmitEventGeneric(drd, string(druidDeploymentLifecycleSucceeded), msg, nil)
	}
}
