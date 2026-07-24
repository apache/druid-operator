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
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/apache/druid-operator/apis/druid/v1alpha1"
	internalhttp "github.com/apache/druid-operator/pkg/http"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type fakeMiddleManagerDruidAPI struct {
	sqlResponses map[string][]byte
	payloads     map[string]*taskPayloadResponse
	disabled     []string
	enabled      []string
	handoffs     map[string][]int
}

func (f *fakeMiddleManagerDruidAPI) DisableWorker(workerHost string) error {
	f.disabled = append(f.disabled, workerHost)
	return nil
}

func (f *fakeMiddleManagerDruidAPI) EnableWorker(workerHost string) error {
	f.enabled = append(f.enabled, workerHost)
	return nil
}

func (f *fakeMiddleManagerDruidAPI) GetTaskPayload(taskID string) (*taskPayloadResponse, error) {
	return f.payloads[taskID], nil
}

func (f *fakeMiddleManagerDruidAPI) TriggerTaskGroupHandoff(supervisorID string, taskGroupIDs []int) error {
	if f.handoffs == nil {
		f.handoffs = map[string][]int{}
	}
	f.handoffs[supervisorID] = taskGroupIDs
	return nil
}

func (f *fakeMiddleManagerDruidAPI) ExecuteSQL(query string) ([]byte, error) {
	return f.sqlResponses["default"], nil
}

func taskPayload(supervisorID string, taskGroupID int) *taskPayloadResponse {
	payload := &taskPayloadResponse{}
	payload.Payload.DataSource = supervisorID
	payload.Payload.IOConfig.TaskGroupID = &taskGroupID
	return payload
}

func TestMiddleManagerDruidHTTPAPIEscapesPathSegments(t *testing.T) {
	requestURIs := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestURIs = append(requestURIs, r.RequestURI)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"task":"task/a","payload":{"dataSource":"wiki","ioConfig":{"taskGroupId":1}}}`))
	}))
	defer server.Close()

	api := &middleManagerDruidHTTPAPI{
		baseURL:    server.URL,
		httpClient: internalhttp.NewHTTPClient(server.Client(), &internalhttp.Auth{}),
	}

	require.NoError(t, api.DisableWorker("mm-0.druid-mm.ns.svc.cluster.local:8091"))
	_, err := api.GetTaskPayload("task/with/slashes")
	require.NoError(t, err)
	require.NoError(t, api.TriggerTaskGroupHandoff("supervisor/with/slash", []int{1}))

	require.Len(t, requestURIs, 3)
	assert.Contains(t, requestURIs[0], "mm-0.druid-mm.ns.svc.cluster.local:8091")
	assert.Contains(t, requestURIs[1], "task%2Fwith%2Fslashes")
	assert.Contains(t, requestURIs[2], "supervisor%2Fwith%2Fslash")
}

func TestDrainMiddleManagerTriggersDeduplicatedHandoffs(t *testing.T) {
	api := &fakeMiddleManagerDruidAPI{
		sqlResponses: map[string][]byte{
			"default": []byte(`[
				{"task_id":"task-0","datasource":"wiki","type":"index_kafka"},
				{"task_id":"task-1","datasource":"wiki","type":"index_kafka"},
				{"task_id":"task-duplicate","datasource":"wiki","type":"index_kafka"}
			]`),
		},
		payloads: map[string]*taskPayloadResponse{
			"task-0":         taskPayload("wiki", 1),
			"task-1":         taskPayload("wiki", 2),
			"task-duplicate": taskPayload("wiki", 1),
		},
	}

	workerHost := "mm-0.druid-mm.druid.svc.cluster.local:8091"
	require.NoError(t, drainMiddleManager(api, workerHost))

	assert.Equal(t, []string{workerHost}, api.disabled)
	assert.Equal(t, []int{1, 2}, api.handoffs["wiki"])
}

func TestValidateWorkerHostnameForSQLRejectsUnsafeHost(t *testing.T) {
	_, err := validateWorkerHostnameForSQL("mm-0.druid-mm.druid.svc.cluster.local:8091")
	require.NoError(t, err)

	_, err = validateWorkerHostnameForSQL("mm-0.bad'host.druid.svc.cluster.local:8091")
	require.Error(t, err)
}

func TestNormalizeMiddleManagerDrainConfig(t *testing.T) {
	assert.Equal(t, middleManagerDrainConfig{
		DrainTimeout:    defaultMiddleManagerDrainTimeout,
		PodReadyTimeout: defaultMiddleManagerPodReadyTimeout,
	}, normalizeMiddleManagerDrainConfig(nil))

	config := normalizeMiddleManagerDrainConfig(&v1alpha1.MiddleManagerDrainStrategy{
		DrainTimeout:    metav1.Duration{Duration: 2 * time.Hour},
		PodReadyTimeout: metav1.Duration{Duration: 10 * time.Minute},
	})
	assert.Equal(t, 2*time.Hour, config.DrainTimeout)
	assert.Equal(t, 10*time.Minute, config.PodReadyTimeout)
}

func TestMiddleManagerDrainStatefulSetUpdaterFnBlocksRollout(t *testing.T) {
	replicas := int32(3)
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "druid-mm"},
		Spec: appsv1.StatefulSetSpec{
			Replicas: &replicas,
			UpdateStrategy: appsv1.StatefulSetUpdateStrategy{
				Type: appsv1.RollingUpdateStatefulSetStrategyType,
			},
		},
	}

	middleManagerDrainStatefulSetUpdaterFn(nil, sts)

	require.NotNil(t, sts.Spec.UpdateStrategy.RollingUpdate)
	require.NotNil(t, sts.Spec.UpdateStrategy.RollingUpdate.Partition)
	assert.Equal(t, replicas, *sts.Spec.UpdateStrategy.RollingUpdate.Partition)
}

func TestContinueMiddleManagerDrainCycleBlocksOnPodReadyTimeout(t *testing.T) {
	require.NoError(t, v1alpha1.AddToScheme(scheme.Scheme))

	drd := &v1alpha1.Druid{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "druid",
			Namespace: "druid",
		},
		Status: v1alpha1.DruidClusterStatus{
			MiddleManagerDrain: &v1alpha1.MiddleManagerDrainStatus{
				StatefulSet: "druid-mm",
				Phase:       middleManagerDrainPhaseWaitingForPod,
				PodName:     "druid-mm-0",
			},
		},
	}
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "druid-mm",
			Namespace: "druid",
		},
		Status: appsv1.StatefulSetStatus{
			CurrentRevision: "old",
			UpdateRevision:  "new",
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithObjects(drd, sts).
		WithStatusSubresource(drd).
		Build()

	state := &middleManagerDrainState{
		Phase:          middleManagerDrainPhaseWaitingForPod,
		PodName:        "druid-mm-0",
		PodOrdinal:     0,
		LastUpdateTime: time.Now().Add(-31 * time.Minute),
	}

	err := continueMiddleManagerDrainCycle(
		context.Background(),
		k8sClient,
		drd,
		sts,
		&fakeMiddleManagerDruidAPI{},
		state,
		8091,
		middleManagerDrainConfig{DrainTimeout: time.Hour, PodReadyTimeout: 30 * time.Minute},
		EmitEventFuncs{record.NewFakeRecorder(10)},
	)

	require.Error(t, err)
	blockedState, exists := getMiddleManagerDrainState("druid", "druid", "druid-mm")
	require.True(t, exists)
	assert.Equal(t, middleManagerDrainPhaseBlocked, blockedState.Phase)

	var updated v1alpha1.Druid
	require.NoError(t, k8sClient.Get(context.Background(), clientObjectKey("druid", "druid"), &updated))
	require.NotNil(t, updated.Status.MiddleManagerDrain)
	assert.Equal(t, middleManagerDrainPhaseBlocked, updated.Status.MiddleManagerDrain.Phase)
	assert.Contains(t, updated.Status.MiddleManagerDrain.Message, "Timed out")
}

func clientObjectKey(namespace, name string) client.ObjectKey {
	return client.ObjectKey{Namespace: namespace, Name: name}
}
