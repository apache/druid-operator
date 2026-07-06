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
	"testing"
	"time"

	"github.com/apache/druid-operator/apis/druid/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type noopEventEmitter struct{}

func (noopEventEmitter) EmitEventGeneric(obj object, eventReason, msg string, err error)         {}
func (noopEventEmitter) EmitEventRollingDeployWait(obj, k8sObj object, nodeSpecUniqueStr string) {}
func (noopEventEmitter) EmitEventOnGetError(obj, getObj object, err error)                       {}
func (noopEventEmitter) EmitEventOnUpdate(obj, updateObj object, err error)                      {}
func (noopEventEmitter) EmitEventOnDelete(obj, deleteObj object, err error)                      {}
func (noopEventEmitter) EmitEventOnCreate(obj, createObj object, err error)                      {}
func (noopEventEmitter) EmitEventOnPatch(obj, patchObj object, err error)                        {}
func (noopEventEmitter) EmitEventOnList(obj object, listObj objectList, err error)               {}

func TestDeploymentLifecycleStatusPatchIncludesNullsForClearedFields(t *testing.T) {
	startedAt := metav1.NewTime(time.Unix(100, 0))
	payload, err := json.Marshal(map[string]interface{}{
		"status": map[string]interface{}{
			"deploymentLifecycle": deploymentLifecycleStatusPatch(v1alpha1.DeploymentLifecycleStatus{
				Phase:              v1alpha1.DeploymentLifecycleInProgress,
				Reason:             "Waiting for rollout",
				ObservedGeneration: 3,
				StartedAt:          &startedAt,
				CompletedAt:        nil,
			}),
		},
	})
	assert.NoError(t, err)

	var decoded map[string]interface{}
	assert.NoError(t, json.Unmarshal(payload, &decoded))

	status := decoded["status"].(map[string]interface{})
	lifecycle := status["deploymentLifecycle"].(map[string]interface{})
	assert.Equal(t, nil, lifecycle["completedAt"])
	assert.NotNil(t, lifecycle["startedAt"])
	assert.NotContains(t, lifecycle, "revision")
	assert.NotContains(t, lifecycle, "trigger")
	assert.NotContains(t, lifecycle, "lastSuccessfulImage")
}

func TestPatchDeploymentLifecycleStatusDoesNotUpdateLocalStatusWhenPatchFails(t *testing.T) {
	drd := &v1alpha1.Druid{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "example",
			Namespace: "default",
		},
	}
	k8sClient := newLifecycleTestClient(t, drd)

	startedAt := metav1.NewTime(time.Unix(100, 0))
	updated := v1alpha1.DeploymentLifecycleStatus{
		Phase:     v1alpha1.DeploymentLifecyclePending,
		StartedAt: &startedAt,
		Reason:    "Waiting for Druid workloads to roll out",
	}

	previousWriter := writers
	writers = failingPatchWriter{err: errors.New("patch failed")}
	t.Cleanup(func() {
		writers = previousWriter
	})

	err := patchDeploymentLifecycleStatus(context.Background(), k8sClient, drd, updated, noopEventEmitter{})
	require.EqualError(t, err, "patch failed")
	assert.Empty(t, drd.Status.DeploymentLifecycle.Phase)
}

func newLifecycleTestClient(t *testing.T, drd *v1alpha1.Druid, objects ...client.Object) client.Client {
	scheme := runtime.NewScheme()
	assert.NoError(t, v1alpha1.AddToScheme(scheme))
	assert.NoError(t, appsv1.AddToScheme(scheme))
	assert.NoError(t, v1.AddToScheme(scheme))

	allObjects := append([]client.Object{drd}, objects...)
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(drd).
		WithObjects(allObjects...).
		Build()
}

func readyManagedDeployment(drd *v1alpha1.Druid) *appsv1.Deployment {
	replicas := int32(1)
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "example-broker",
			Namespace:  drd.Namespace,
			Labels:     makeLabelsForDruid(drd),
			Generation: 2,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
		},
		Status: appsv1.DeploymentStatus{
			ObservedGeneration: 2,
			Replicas:           1,
			ReadyReplicas:      1,
			UpdatedReplicas:    1,
		},
	}
}

func readyManagedStatefulSet(drd *v1alpha1.Druid) *appsv1.StatefulSet {
	replicas := int32(1)
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "example-historical",
			Namespace:  drd.Namespace,
			Labels:     makeLabelsForDruid(drd),
			Generation: 2,
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas: &replicas,
		},
		Status: appsv1.StatefulSetStatus{
			ObservedGeneration: 2,
			CurrentRevision:    "rev-2",
			UpdateRevision:     "rev-2",
			UpdatedReplicas:    1,
			ReadyReplicas:      1,
		},
	}
}

func TestReconcileDeploymentLifecycleSucceedsWhenKubernetesResourcesAreReady(t *testing.T) {

	drd := &v1alpha1.Druid{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "druid.apache.org/v1alpha1",
			Kind:       "Druid",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:       "example",
			Namespace:  "default",
			Generation: 2,
		},
		Spec: v1alpha1.DruidSpec{
			CommonRuntimeProperties: "druid.service=druid/router",
		},
	}

	deployment := readyManagedDeployment(drd)
	k8sClient := newLifecycleTestClient(t, drd, deployment)

	assert.NoError(t, reconcileDeploymentLifecycle(context.Background(), k8sClient, drd, noopEventEmitter{}))

	stored := &v1alpha1.Druid{}
	assert.NoError(t, k8sClient.Get(context.Background(), client.ObjectKeyFromObject(drd), stored))
	assert.Equal(t, v1alpha1.DeploymentLifecycleSucceeded, stored.Status.DeploymentLifecycle.Phase)
	assert.Equal(t, "Deployment lifecycle completed after managed workloads became ready", stored.Status.DeploymentLifecycle.Reason)
	assert.Equal(t, drd.Generation, stored.Status.DeploymentLifecycle.ObservedGeneration)
	assert.NotNil(t, stored.Status.DeploymentLifecycle.CompletedAt)
}

func TestAreManagedWorkloadsReadyWaitsForDeploymentObservedGeneration(t *testing.T) {
	drd := &v1alpha1.Druid{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "example",
			Namespace: "default",
		},
	}

	replicas := int32(1)
	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "example-broker",
			Namespace:  drd.Namespace,
			Labels:     makeLabelsForDruid(drd),
			Generation: 2,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
		},
		Status: appsv1.DeploymentStatus{
			ObservedGeneration: 1,
			Replicas:           1,
			ReadyReplicas:      1,
			UpdatedReplicas:    1,
		},
	}

	k8sClient := newLifecycleTestClient(t, drd, deployment)
	ready, reason, err := areManagedWorkloadsReady(context.Background(), k8sClient, drd)
	assert.NoError(t, err)
	assert.False(t, ready)
	assert.Equal(t, "Waiting for Deployment [example-broker] controller to observe generation", reason)
}

func TestAreManagedWorkloadsReadyTreatsDeploymentReplicaFailureAsWaiting(t *testing.T) {
	drd := &v1alpha1.Druid{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "example",
			Namespace: "default",
		},
	}

	deployment := readyManagedDeployment(drd)
	deployment.Status.Conditions = []appsv1.DeploymentCondition{
		{
			Type:   appsv1.DeploymentReplicaFailure,
			Status: v1.ConditionTrue,
			Reason: "FailedCreate",
		},
	}

	k8sClient := newLifecycleTestClient(t, drd, deployment)
	ready, reason, err := areManagedWorkloadsReady(context.Background(), k8sClient, drd)
	assert.NoError(t, err)
	assert.False(t, ready)
	assert.Equal(t, "Waiting for Deployment [example-broker] replica failure to clear: FailedCreate", reason)
}

func TestAreManagedWorkloadsReadyWaitsForStatefulSetObservedGeneration(t *testing.T) {
	drd := &v1alpha1.Druid{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "example",
			Namespace: "default",
		},
	}

	statefulSet := readyManagedStatefulSet(drd)
	statefulSet.Status.ObservedGeneration = 1

	k8sClient := newLifecycleTestClient(t, drd, statefulSet)
	ready, reason, err := areManagedWorkloadsReady(context.Background(), k8sClient, drd)
	assert.NoError(t, err)
	assert.False(t, ready)
	assert.Equal(t, "Waiting for StatefulSet [example-historical] controller to observe generation", reason)
}

func TestAreManagedWorkloadsReadyAcceptsObservedStatefulSetGeneration(t *testing.T) {
	drd := &v1alpha1.Druid{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "example",
			Namespace: "default",
		},
	}

	statefulSet := readyManagedStatefulSet(drd)

	k8sClient := newLifecycleTestClient(t, drd, statefulSet)
	ready, reason, err := areManagedWorkloadsReady(context.Background(), k8sClient, drd)
	assert.NoError(t, err)
	assert.True(t, ready)
	assert.Equal(t, "", reason)
}

func TestReconcileDeploymentLifecycleKeepsTimedOutRolloutInProgress(t *testing.T) {
	startedAt := metav1.NewTime(time.Unix(100, 0))
	drd := &v1alpha1.Druid{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "druid.apache.org/v1alpha1",
			Kind:       "Druid",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:       "example",
			Namespace:  "default",
			Generation: 2,
		},
		Spec: v1alpha1.DruidSpec{
			CommonRuntimeProperties: "druid.service=druid/router",
		},
		Status: v1alpha1.DruidClusterStatus{
			DeploymentLifecycle: v1alpha1.DeploymentLifecycleStatus{
				Phase:              v1alpha1.DeploymentLifecycleInProgress,
				ObservedGeneration: 2,
				StartedAt:          &startedAt,
			},
		},
	}

	deployment := readyManagedDeployment(drd)
	deployment.Status.ReadyReplicas = 0
	k8sClient := newLifecycleTestClient(t, drd, deployment)

	assert.NoError(t, reconcileDeploymentLifecycleWithDeps(context.Background(), k8sClient, drd, noopEventEmitter{}, lifecycleDependencies{
		now:     func() time.Time { return time.Unix(110, 0) },
		timeout: 5 * time.Second,
	}))

	stored := &v1alpha1.Druid{}
	assert.NoError(t, k8sClient.Get(context.Background(), client.ObjectKeyFromObject(drd), stored))
	assert.Equal(t, v1alpha1.DeploymentLifecycleInProgress, stored.Status.DeploymentLifecycle.Phase)
	assert.Contains(t, stored.Status.DeploymentLifecycle.Reason, deploymentLifecycleTimeoutReasonPrefix)
	assert.Contains(t, stored.Status.DeploymentLifecycle.Reason, "still waiting for Deployment [example-broker] rollout")
	assert.Nil(t, stored.Status.DeploymentLifecycle.CompletedAt)
}

func TestReconcileDeploymentLifecycleSucceedsAfterTimeoutWithoutNewGeneration(t *testing.T) {
	startedAt := metav1.NewTime(time.Unix(100, 0))
	drd := &v1alpha1.Druid{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "druid.apache.org/v1alpha1",
			Kind:       "Druid",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:       "example",
			Namespace:  "default",
			Generation: 2,
		},
		Spec: v1alpha1.DruidSpec{
			CommonRuntimeProperties: "druid.service=druid/router",
		},
		Status: v1alpha1.DruidClusterStatus{
			DeploymentLifecycle: v1alpha1.DeploymentLifecycleStatus{
				Phase:              v1alpha1.DeploymentLifecycleInProgress,
				Reason:             deploymentLifecycleTimeoutReasonPrefix + " of 5s; still waiting for Deployment [example-broker] rollout",
				ObservedGeneration: 2,
				StartedAt:          &startedAt,
			},
		},
	}

	deployment := readyManagedDeployment(drd)
	k8sClient := newLifecycleTestClient(t, drd, deployment)

	assert.NoError(t, reconcileDeploymentLifecycleWithDeps(context.Background(), k8sClient, drd, noopEventEmitter{}, lifecycleDependencies{
		now:     func() time.Time { return time.Unix(120, 0) },
		timeout: 5 * time.Second,
	}))

	stored := &v1alpha1.Druid{}
	assert.NoError(t, k8sClient.Get(context.Background(), client.ObjectKeyFromObject(drd), stored))
	assert.Equal(t, v1alpha1.DeploymentLifecycleSucceeded, stored.Status.DeploymentLifecycle.Phase)
	assert.Equal(t, "Deployment lifecycle completed after managed workloads became ready", stored.Status.DeploymentLifecycle.Reason)
	assert.NotNil(t, stored.Status.DeploymentLifecycle.CompletedAt)
}

func TestReconcileDeploymentLifecycleIsIdempotentAfterSuccess(t *testing.T) {
	startedAt := metav1.NewTime(time.Unix(100, 0))
	completedAt := metav1.NewTime(time.Unix(160, 0))
	drd := &v1alpha1.Druid{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "druid.apache.org/v1alpha1",
			Kind:       "Druid",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:       "example",
			Namespace:  "default",
			Generation: 2,
		},
		Spec: v1alpha1.DruidSpec{
			CommonRuntimeProperties: "druid.service=druid/router",
		},
		Status: v1alpha1.DruidClusterStatus{
			DeploymentLifecycle: v1alpha1.DeploymentLifecycleStatus{
				Phase:              v1alpha1.DeploymentLifecycleSucceeded,
				Reason:             "Deployment lifecycle completed after managed workloads became ready",
				ObservedGeneration: 2,
				StartedAt:          &startedAt,
				CompletedAt:        &completedAt,
			},
		},
	}

	deployment := readyManagedDeployment(drd)
	k8sClient := newLifecycleTestClient(t, drd, deployment)

	before := &v1alpha1.Druid{}
	assert.NoError(t, k8sClient.Get(context.Background(), client.ObjectKeyFromObject(drd), before))

	assert.NoError(t, reconcileDeploymentLifecycle(context.Background(), k8sClient, drd, noopEventEmitter{}))

	after := &v1alpha1.Druid{}
	assert.NoError(t, k8sClient.Get(context.Background(), client.ObjectKeyFromObject(drd), after))
	assert.Equal(t, before.Status.DeploymentLifecycle, after.Status.DeploymentLifecycle)
}

type failingPatchWriter struct {
	err error
}

func (w failingPatchWriter) Delete(ctx context.Context, sdk client.Client, drd *v1alpha1.Druid, obj object, emitEvent EventEmitter, deleteOptions ...client.DeleteOption) error {
	return nil
}

func (w failingPatchWriter) Create(ctx context.Context, sdk client.Client, drd *v1alpha1.Druid, obj object, emitEvent EventEmitter) (DruidNodeStatus, error) {
	return "", nil
}

func (w failingPatchWriter) Update(ctx context.Context, sdk client.Client, drd *v1alpha1.Druid, obj object, emitEvent EventEmitter) (DruidNodeStatus, error) {
	return "", nil
}

func (w failingPatchWriter) Patch(ctx context.Context, sdk client.Client, drd *v1alpha1.Druid, obj object, status bool, patch client.Patch, emitEvent EventEmitter) error {
	return w.err
}
