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
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/druid-operator/apis/druid/v1alpha1"
	druidapi "github.com/apache/druid-operator/pkg/druidapi"
	internalhttp "github.com/apache/druid-operator/pkg/http"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	middleManagerDrainPhaseDraining      = "Draining"
	middleManagerDrainPhaseWaitingForPod = "WaitingForPod"
	middleManagerDrainPhaseBlocked       = "Blocked"

	defaultMiddleManagerDrainTimeout    = time.Hour
	defaultMiddleManagerPodReadyTimeout = 30 * time.Minute
	statefulSetPartitionResetValue      = int32(0)
)

var (
	middleManagerDrainStates = sync.Map{}
	workerHostnamePattern    = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`)
)

type middleManagerDrainState struct {
	Phase          string
	PodName        string
	PodOrdinal     int32
	OldPodUID      string
	LastUpdateTime time.Time
}

type middleManagerDrainConfig struct {
	DrainTimeout    time.Duration
	PodReadyTimeout time.Duration
}

type middleManagerDruidAPI interface {
	DisableWorker(workerHost string) error
	EnableWorker(workerHost string) error
	GetTaskPayload(taskID string) (*taskPayloadResponse, error)
	TriggerTaskGroupHandoff(supervisorID string, taskGroupIDs []int) error
	ExecuteSQL(query string) ([]byte, error)
}

type middleManagerDruidHTTPAPI struct {
	baseURL    string
	httpClient internalhttp.DruidHTTP
}

type druidSQLRequest struct {
	Query string `json:"query"`
}

type runningTaskInfo struct {
	TaskID     string `json:"task_id"`
	DataSource string `json:"datasource"`
	Type       string `json:"type"`
}

type taskPayloadResponse struct {
	Task    string `json:"task"`
	Payload struct {
		DataSource string `json:"dataSource"`
		IOConfig   struct {
			TaskGroupID *int `json:"taskGroupId"`
		} `json:"ioConfig"`
	} `json:"payload"`
}

type taskGroupHandoffRequest struct {
	TaskGroupIDs []int `json:"taskGroupIds"`
}

func newMiddleManagerDruidAPI(ctx context.Context, sdk client.Client, drd *v1alpha1.Druid) (middleManagerDruidAPI, error) {
	routerURL, err := druidapi.GetRouterSvcUrl(drd.Namespace, drd.Name, sdk)
	if err != nil {
		return nil, fmt.Errorf("failed to discover Druid router service: %w", err)
	}

	basicAuth, err := druidapi.GetAuthCreds(ctx, sdk, drd.Spec.Auth)
	if err != nil {
		return nil, fmt.Errorf("failed to get Druid API credentials: %w", err)
	}

	return &middleManagerDruidHTTPAPI{
		baseURL: routerURL,
		httpClient: internalhttp.NewHTTPClient(
			&http.Client{},
			&internalhttp.Auth{BasicAuth: basicAuth},
		),
	}, nil
}

func (c *middleManagerDruidHTTPAPI) DisableWorker(workerHost string) error {
	path := fmt.Sprintf("%s/%s/disable", druidapi.MakePath(c.baseURL, "indexer", "worker"), url.PathEscape(workerHost))
	resp, err := c.httpClient.Do(http.MethodPost, path, nil)
	if err != nil {
		return fmt.Errorf("failed to call disable API for worker %q: %w", workerHost, err)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("disable API returned status %d for worker %q: %s", resp.StatusCode, workerHost, resp.ResponseBody)
	}
	return nil
}

func (c *middleManagerDruidHTTPAPI) EnableWorker(workerHost string) error {
	path := fmt.Sprintf("%s/%s/enable", druidapi.MakePath(c.baseURL, "indexer", "worker"), url.PathEscape(workerHost))
	resp, err := c.httpClient.Do(http.MethodPost, path, nil)
	if err != nil {
		return fmt.Errorf("failed to call enable API for worker %q: %w", workerHost, err)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("enable API returned status %d for worker %q: %s", resp.StatusCode, workerHost, resp.ResponseBody)
	}
	return nil
}

func (c *middleManagerDruidHTTPAPI) GetTaskPayload(taskID string) (*taskPayloadResponse, error) {
	path := fmt.Sprintf("%s/%s", druidapi.MakePath(c.baseURL, "indexer", "task"), url.PathEscape(taskID))
	resp, err := c.httpClient.Do(http.MethodGet, path, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch task payload for %q: %w", taskID, err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("task payload API returned status %d for %q: %s", resp.StatusCode, taskID, resp.ResponseBody)
	}

	var payload taskPayloadResponse
	if err := json.Unmarshal([]byte(resp.ResponseBody), &payload); err != nil {
		return nil, fmt.Errorf("failed to decode task payload for %q: %w", taskID, err)
	}
	return &payload, nil
}

func (c *middleManagerDruidHTTPAPI) TriggerTaskGroupHandoff(supervisorID string, taskGroupIDs []int) error {
	path := fmt.Sprintf("%s/%s/taskGroups/handoff", druidapi.MakePath(c.baseURL, "indexer", "supervisor"), url.PathEscape(supervisorID))
	reqBody, err := json.Marshal(taskGroupHandoffRequest{TaskGroupIDs: taskGroupIDs})
	if err != nil {
		return fmt.Errorf("failed to marshal handoff request for %q: %w", supervisorID, err)
	}

	resp, err := c.httpClient.Do(http.MethodPost, path, reqBody)
	if err != nil {
		return fmt.Errorf("failed to trigger handoff for supervisor %q: %w", supervisorID, err)
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("handoff API returned status %d for supervisor %q: %s", resp.StatusCode, supervisorID, resp.ResponseBody)
	}
	return nil
}

func (c *middleManagerDruidHTTPAPI) ExecuteSQL(query string) ([]byte, error) {
	reqBody, err := json.Marshal(druidSQLRequest{Query: query})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal SQL request: %w", err)
	}

	resp, err := c.httpClient.Do(http.MethodPost, druidapi.MakeSQLPath(c.baseURL), reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to execute Druid SQL: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Druid SQL API returned status %d: %s", resp.StatusCode, resp.ResponseBody)
	}
	return []byte(resp.ResponseBody), nil
}

func normalizeMiddleManagerDrainConfig(strategy *v1alpha1.MiddleManagerDrainStrategy) middleManagerDrainConfig {
	config := middleManagerDrainConfig{
		DrainTimeout:    defaultMiddleManagerDrainTimeout,
		PodReadyTimeout: defaultMiddleManagerPodReadyTimeout,
	}
	if strategy == nil {
		return config
	}
	if strategy.DrainTimeout.Duration > 0 {
		config.DrainTimeout = strategy.DrainTimeout.Duration
	}
	if strategy.PodReadyTimeout.Duration > 0 {
		config.PodReadyTimeout = strategy.PodReadyTimeout.Duration
	}
	return config
}

func middleManagerDrainStateKey(namespace, druidCR, statefulSetName string) string {
	return fmt.Sprintf("%s/%s/%s", namespace, druidCR, statefulSetName)
}

func getMiddleManagerDrainState(namespace, druidCR, statefulSetName string) (*middleManagerDrainState, bool) {
	key := middleManagerDrainStateKey(namespace, druidCR, statefulSetName)
	if value, exists := middleManagerDrainStates.Load(key); exists {
		return value.(*middleManagerDrainState), true
	}
	return nil, false
}

func loadMiddleManagerDrainState(drd *v1alpha1.Druid, statefulSetName string) (*middleManagerDrainState, bool) {
	if state, exists := getMiddleManagerDrainState(drd.Namespace, drd.Name, statefulSetName); exists {
		return state, true
	}

	status := drd.Status.MiddleManagerDrain
	if status == nil || status.StatefulSet != statefulSetName || status.Phase == "" || status.PodName == "" {
		return nil, false
	}

	state := &middleManagerDrainState{
		Phase:          status.Phase,
		PodName:        status.PodName,
		PodOrdinal:     status.PodOrdinal,
		OldPodUID:      status.OldPodUID,
		LastUpdateTime: status.LastTransitionTime.Time,
	}
	if state.LastUpdateTime.IsZero() {
		state.LastUpdateTime = time.Now()
	}
	middleManagerDrainStates.Store(middleManagerDrainStateKey(drd.Namespace, drd.Name, statefulSetName), state)
	return state, true
}

func setMiddleManagerDrainState(ctx context.Context, sdk client.Client, drd *v1alpha1.Druid, statefulSetName string, state *middleManagerDrainState, message string, emitEvent EventEmitter) error {
	if state.LastUpdateTime.IsZero() {
		state.LastUpdateTime = time.Now()
	}
	middleManagerDrainStates.Store(middleManagerDrainStateKey(drd.Namespace, drd.Name, statefulSetName), state)

	return patchMiddleManagerDrainStatus(ctx, sdk, drd, &v1alpha1.MiddleManagerDrainStatus{
		StatefulSet:        statefulSetName,
		Phase:              state.Phase,
		PodName:            state.PodName,
		PodOrdinal:         state.PodOrdinal,
		OldPodUID:          state.OldPodUID,
		LastTransitionTime: metav1.NewTime(state.LastUpdateTime),
		Message:            message,
	}, emitEvent)
}

func clearMiddleManagerDrainState(ctx context.Context, sdk client.Client, drd *v1alpha1.Druid, statefulSetName string, emitEvent EventEmitter) error {
	middleManagerDrainStates.Delete(middleManagerDrainStateKey(drd.Namespace, drd.Name, statefulSetName))
	if drd.Status.MiddleManagerDrain == nil || drd.Status.MiddleManagerDrain.StatefulSet != statefulSetName {
		return nil
	}
	return patchMiddleManagerDrainStatus(ctx, sdk, drd, nil, emitEvent)
}

func patchMiddleManagerDrainStatus(ctx context.Context, sdk client.Client, drd *v1alpha1.Druid, status *v1alpha1.MiddleManagerDrainStatus, emitEvent EventEmitter) error {
	updatedStatus := drd.Status
	updatedStatus.MiddleManagerDrain = status
	if err := druidClusterStatusPatcher(ctx, sdk, updatedStatus, drd, emitEvent); err != nil {
		return err
	}
	drd.Status = updatedStatus
	return nil
}

func updateStatefulSetPartition(ctx context.Context, sdk client.Client, statefulSetName, namespace string, partition int32) error {
	var sts appsv1.StatefulSet
	if err := sdk.Get(ctx, types.NamespacedName{Name: statefulSetName, Namespace: namespace}, &sts); err != nil {
		return fmt.Errorf("failed to get StatefulSet %q in namespace %q: %w", statefulSetName, namespace, err)
	}

	if sts.Spec.UpdateStrategy.RollingUpdate == nil {
		sts.Spec.UpdateStrategy.Type = appsv1.RollingUpdateStatefulSetStrategyType
		sts.Spec.UpdateStrategy.RollingUpdate = &appsv1.RollingUpdateStatefulSetStrategy{}
	}
	if sts.Spec.UpdateStrategy.RollingUpdate.Partition != nil && *sts.Spec.UpdateStrategy.RollingUpdate.Partition == partition {
		return nil
	}

	sts.Spec.UpdateStrategy.RollingUpdate.Partition = &partition
	return sdk.Update(ctx, &sts)
}

func middleManagerDrainStatefulSetUpdaterFn(prev, curr object) {
	currSts, ok := curr.(*appsv1.StatefulSet)
	if !ok {
		return
	}

	replicas := int32(1)
	if currSts.Spec.Replicas != nil {
		replicas = *currSts.Spec.Replicas
	}
	if currSts.Spec.UpdateStrategy.RollingUpdate == nil {
		currSts.Spec.UpdateStrategy.Type = appsv1.RollingUpdateStatefulSetStrategyType
		currSts.Spec.UpdateStrategy.RollingUpdate = &appsv1.RollingUpdateStatefulSetStrategy{}
	}
	currSts.Spec.UpdateStrategy.RollingUpdate.Partition = &replicas
}

func cleanupStaleMiddleManagerDrainState(ctx context.Context, sdk client.Client, drd *v1alpha1.Druid, statefulSetName string, emitEvent EventEmitter) {
	state, hasState := loadMiddleManagerDrainState(drd, statefulSetName)
	if !hasState {
		return
	}

	if hasState {
		api, err := newMiddleManagerDruidAPI(ctx, sdk, drd)
		if err != nil {
			logger.Error(err, "Failed to create Druid API client while cleaning stale MiddleManager drain state", "statefulSet", statefulSetName)
		} else {
			druidPort, portErr := getDruidPortFromStatefulSet(ctx, sdk, statefulSetName, drd.Namespace)
			if portErr != nil {
				logger.Error(portErr, "Failed to get Druid port while cleaning stale MiddleManager drain state", "statefulSet", statefulSetName)
			} else {
				workerHost := buildMiddleManagerWorkerHost(state.PodName, statefulSetName, drd.Namespace, druidPort)
				if err := api.EnableWorker(workerHost); err != nil {
					logger.Error(err, "Failed to re-enable MiddleManager while cleaning stale drain state", "pod", state.PodName, "statefulSet", statefulSetName)
				}
			}
		}
	}

	if err := clearMiddleManagerDrainState(ctx, sdk, drd, statefulSetName, emitEvent); err != nil {
		logger.Error(err, "Failed to clear stale MiddleManager drain status", "statefulSet", statefulSetName)
	}
	if err := updateStatefulSetPartition(ctx, sdk, statefulSetName, drd.Namespace, statefulSetPartitionResetValue); err != nil {
		logger.Error(err, "Failed to reset StatefulSet partition while cleaning stale MiddleManager drain state", "statefulSet", statefulSetName)
	}
}

func processMiddleManagerRollingRestart(ctx context.Context, sdk client.Client, drd *v1alpha1.Druid, statefulSetName string, strategy *v1alpha1.MiddleManagerDrainStrategy, emitEvent EventEmitter) error {
	var sts appsv1.StatefulSet
	if err := sdk.Get(ctx, types.NamespacedName{Name: statefulSetName, Namespace: drd.Namespace}, &sts); err != nil {
		return fmt.Errorf("failed to get StatefulSet %q in namespace %q: %w", statefulSetName, drd.Namespace, err)
	}

	if sts.Status.CurrentRevision == sts.Status.UpdateRevision {
		if err := clearMiddleManagerDrainState(ctx, sdk, drd, statefulSetName, emitEvent); err != nil {
			return err
		}
		return updateStatefulSetPartition(ctx, sdk, statefulSetName, drd.Namespace, statefulSetPartitionResetValue)
	}

	config := normalizeMiddleManagerDrainConfig(strategy)
	totalReplicas := int32(1)
	if sts.Spec.Replicas != nil {
		totalReplicas = *sts.Spec.Replicas
	}

	state, hasState := loadMiddleManagerDrainState(drd, statefulSetName)
	if !hasState {
		if err := updateStatefulSetPartition(ctx, sdk, statefulSetName, drd.Namespace, totalReplicas); err != nil {
			return fmt.Errorf("failed to block MiddleManager StatefulSet rolling update: %w", err)
		}
	}

	api, err := newMiddleManagerDruidAPI(ctx, sdk, drd)
	if err != nil {
		return err
	}

	druidPort, err := getDruidPortFromStatefulSet(ctx, sdk, statefulSetName, drd.Namespace)
	if err != nil {
		return err
	}

	if hasState {
		return continueMiddleManagerDrainCycle(ctx, sdk, drd, &sts, api, state, druidPort, config, emitEvent)
	}
	return startMiddleManagerDrainCycle(ctx, sdk, drd, &sts, api, druidPort, emitEvent)
}

func startMiddleManagerDrainCycle(ctx context.Context, sdk client.Client, drd *v1alpha1.Druid, sts *appsv1.StatefulSet, api middleManagerDruidAPI, druidPort int32, emitEvent EventEmitter) error {
	outdatedPods, err := getOutdatedMiddleManagerPods(ctx, sdk, sts.Name, sts.Namespace, sts.Status.CurrentRevision)
	if err != nil {
		return err
	}
	if len(outdatedPods) == 0 {
		return nil
	}

	sortPodsDescending(outdatedPods)
	targetPod := outdatedPods[0]
	podOrdinal := extractPodOrdinal(targetPod.Name)
	if podOrdinal < 0 {
		return fmt.Errorf("could not extract ordinal from MiddleManager pod name %q", targetPod.Name)
	}

	workerHost := buildMiddleManagerWorkerHost(targetPod.Name, sts.Name, sts.Namespace, druidPort)
	if targetPod.Labels["controller-revision-hash"] == sts.Status.UpdateRevision {
		if err := api.EnableWorker(workerHost); err != nil {
			logger.Error(err, "Failed to enable already-updated MiddleManager pod", "pod", targetPod.Name)
		}
		return nil
	}

	if err := drainMiddleManager(api, workerHost); err != nil {
		return err
	}

	return setMiddleManagerDrainState(ctx, sdk, drd, sts.Name, &middleManagerDrainState{
		Phase:      middleManagerDrainPhaseDraining,
		PodName:    targetPod.Name,
		PodOrdinal: podOrdinal,
	}, "Drain initiated; waiting for streaming ingestion tasks to finish", emitEvent)
}

func continueMiddleManagerDrainCycle(ctx context.Context, sdk client.Client, drd *v1alpha1.Druid, sts *appsv1.StatefulSet, api middleManagerDruidAPI, state *middleManagerDrainState, druidPort int32, config middleManagerDrainConfig, emitEvent EventEmitter) error {
	workerHost := buildMiddleManagerWorkerHost(state.PodName, sts.Name, sts.Namespace, druidPort)
	elapsed := time.Since(state.LastUpdateTime)

	switch state.Phase {
	case middleManagerDrainPhaseDraining:
		if elapsed < config.DrainTimeout {
			drained, err := isMiddleManagerDrained(api, workerHost)
			if err != nil {
				logger.Error(err, "Failed to check MiddleManager drain status; will retry", "pod", state.PodName)
				return nil
			}
			if !drained {
				return setMiddleManagerDrainState(ctx, sdk, drd, sts.Name, state, "Waiting for streaming ingestion tasks to drain", emitEvent)
			}
		}

		oldPodUID := ""
		var oldPod v1.Pod
		if err := sdk.Get(ctx, types.NamespacedName{Name: state.PodName, Namespace: sts.Namespace}, &oldPod); err == nil {
			oldPodUID = string(oldPod.UID)
		}
		if err := updateStatefulSetPartition(ctx, sdk, sts.Name, sts.Namespace, state.PodOrdinal); err != nil {
			return fmt.Errorf("failed to lower StatefulSet partition for pod %q: %w", state.PodName, err)
		}
		return setMiddleManagerDrainState(ctx, sdk, drd, sts.Name, &middleManagerDrainState{
			Phase:      middleManagerDrainPhaseWaitingForPod,
			PodName:    state.PodName,
			PodOrdinal: state.PodOrdinal,
			OldPodUID:  oldPodUID,
		}, "Drain complete; waiting for replacement pod to become ready", emitEvent)

	case middleManagerDrainPhaseWaitingForPod:
		if elapsed >= config.PodReadyTimeout {
			blockedState := *state
			blockedState.Phase = middleManagerDrainPhaseBlocked
			blockedState.LastUpdateTime = time.Time{}
			message := fmt.Sprintf("Timed out after %s waiting for replacement pod to become ready", config.PodReadyTimeout)
			if err := setMiddleManagerDrainState(ctx, sdk, drd, sts.Name, &blockedState, message, emitEvent); err != nil {
				return err
			}
			return fmt.Errorf("MiddleManager pod %q did not become ready before timeout %s", state.PodName, config.PodReadyTimeout)
		}

		var pod v1.Pod
		if err := sdk.Get(ctx, types.NamespacedName{Name: state.PodName, Namespace: sts.Namespace}, &pod); err != nil {
			return nil
		}
		if state.OldPodUID != "" && string(pod.UID) == state.OldPodUID {
			return nil
		}
		if !isPodReady(pod) {
			return nil
		}
		if pod.Labels["controller-revision-hash"] != sts.Status.UpdateRevision {
			if err := clearMiddleManagerDrainState(ctx, sdk, drd, sts.Name, emitEvent); err != nil {
				return err
			}
			return nil
		}
		if err := api.EnableWorker(workerHost); err != nil {
			logger.Error(err, "Failed to re-enable MiddleManager pod; will retry", "pod", state.PodName)
			return nil
		}
		return clearMiddleManagerDrainState(ctx, sdk, drd, sts.Name, emitEvent)

	case middleManagerDrainPhaseBlocked:
		message := ""
		if drd.Status.MiddleManagerDrain != nil {
			message = drd.Status.MiddleManagerDrain.Message
		}
		return fmt.Errorf("MiddleManager drain rollout is blocked for pod %q: %s", state.PodName, message)

	default:
		return clearMiddleManagerDrainState(ctx, sdk, drd, sts.Name, emitEvent)
	}
}

func drainMiddleManager(api middleManagerDruidAPI, workerHost string) error {
	if err := api.DisableWorker(workerHost); err != nil {
		return fmt.Errorf("failed to disable MiddleManager: %w", err)
	}

	runningTasks, err := getRunningTasksFromSQL(api, workerHost)
	if err != nil {
		return fmt.Errorf("failed to get running tasks after disabling MiddleManager: %w", err)
	}
	if len(runningTasks) == 0 {
		return nil
	}

	handoffs, err := resolveHandoffsForWorker(api, runningTasks)
	if err != nil {
		return err
	}
	for supervisorID, taskGroupIDs := range handoffs {
		if len(taskGroupIDs) == 0 {
			continue
		}
		if err := api.TriggerTaskGroupHandoff(supervisorID, taskGroupIDs); err != nil {
			return fmt.Errorf("failed to trigger handoff for supervisor %q: %w", supervisorID, err)
		}
	}
	return nil
}

func resolveHandoffsForWorker(api middleManagerDruidAPI, runningTaskIDs []string) (map[string][]int, error) {
	supervisorToGroupIDs := map[string]map[int]bool{}
	for _, taskID := range runningTaskIDs {
		payload, err := api.GetTaskPayload(taskID)
		if err != nil {
			logger.Error(err, "Failed to fetch task payload while resolving MiddleManager handoff", "taskID", taskID)
			continue
		}
		if payload.Payload.DataSource == "" || payload.Payload.IOConfig.TaskGroupID == nil {
			continue
		}
		if supervisorToGroupIDs[payload.Payload.DataSource] == nil {
			supervisorToGroupIDs[payload.Payload.DataSource] = map[int]bool{}
		}
		supervisorToGroupIDs[payload.Payload.DataSource][*payload.Payload.IOConfig.TaskGroupID] = true
	}

	result := map[string][]int{}
	for supervisorID, groupIDSet := range supervisorToGroupIDs {
		ids := make([]int, 0, len(groupIDSet))
		for id := range groupIDSet {
			ids = append(ids, id)
		}
		sort.Ints(ids)
		result[supervisorID] = ids
	}
	return result, nil
}

func getRunningTasksFromSQL(api middleManagerDruidAPI, workerHost string) ([]string, error) {
	hostname, err := validateWorkerHostnameForSQL(workerHost)
	if err != nil {
		return nil, err
	}

	query := fmt.Sprintf(`SELECT "task_id", "datasource", "type" FROM sys.tasks WHERE "runner_status" = 'RUNNING' AND "location" LIKE '%s:%%'`, hostname)
	body, err := api.ExecuteSQL(query)
	if err != nil {
		return nil, err
	}

	var rows []runningTaskInfo
	if err := json.Unmarshal(body, &rows); err != nil {
		return nil, fmt.Errorf("failed to decode running tasks SQL response: %w", err)
	}

	taskIDs := make([]string, 0, len(rows))
	for _, row := range rows {
		if row.TaskID != "" {
			taskIDs = append(taskIDs, row.TaskID)
		}
	}
	return taskIDs, nil
}

func isMiddleManagerDrained(api middleManagerDruidAPI, workerHost string) (bool, error) {
	hostname, err := validateWorkerHostnameForSQL(workerHost)
	if err != nil {
		return false, err
	}

	query := fmt.Sprintf(`SELECT COUNT(*) AS "cnt" FROM sys.tasks WHERE "runner_status" = 'RUNNING' AND "location" LIKE '%s:%%' AND "type" IN ('index_kafka', 'index_kinesis')`, hostname)
	body, err := api.ExecuteSQL(query)
	if err != nil {
		return false, err
	}

	var rows []struct {
		Cnt int `json:"cnt"`
	}
	if err := json.Unmarshal(body, &rows); err != nil {
		return false, fmt.Errorf("failed to decode drain check SQL response: %w", err)
	}
	return len(rows) == 0 || rows[0].Cnt == 0, nil
}

func validateWorkerHostnameForSQL(workerHost string) (string, error) {
	hostname := stripPort(workerHost)
	if !workerHostnamePattern.MatchString(hostname) {
		return "", fmt.Errorf("invalid MiddleManager worker hostname %q", hostname)
	}
	return hostname, nil
}

func stripPort(hostPort string) string {
	idx := strings.LastIndex(hostPort, ":")
	if idx < 0 {
		return hostPort
	}
	return hostPort[:idx]
}

func buildMiddleManagerWorkerHost(podName, serviceName, namespace string, port int32) string {
	return fmt.Sprintf("%s.%s.%s.svc.cluster.local:%d", podName, serviceName, namespace, port)
}

func getDruidPortFromStatefulSet(ctx context.Context, sdk client.Client, statefulSetName, namespace string) (int32, error) {
	var sts appsv1.StatefulSet
	if err := sdk.Get(ctx, types.NamespacedName{Name: statefulSetName, Namespace: namespace}, &sts); err != nil {
		return 0, err
	}
	for _, container := range sts.Spec.Template.Spec.Containers {
		for _, port := range container.Ports {
			if port.Name == "druid-port" {
				return port.ContainerPort, nil
			}
		}
	}
	return 0, fmt.Errorf("druid-port not found in StatefulSet %q pod template", statefulSetName)
}

func getOutdatedMiddleManagerPods(ctx context.Context, sdk client.Client, statefulSetName, namespace, currentRevision string) ([]v1.Pod, error) {
	var podList v1.PodList
	if err := sdk.List(ctx, &podList, client.InNamespace(namespace), client.MatchingLabels{
		"nodeSpecUniqueStr": statefulSetName,
	}); err != nil {
		return nil, fmt.Errorf("failed to list MiddleManager pods: %w", err)
	}

	outdatedPods := make([]v1.Pod, 0, len(podList.Items))
	for _, pod := range podList.Items {
		if pod.Labels["controller-revision-hash"] == currentRevision {
			outdatedPods = append(outdatedPods, pod)
		}
	}
	return outdatedPods, nil
}

func sortPodsDescending(pods []v1.Pod) {
	sort.SliceStable(pods, func(i, j int) bool {
		return extractPodOrdinal(pods[i].Name) > extractPodOrdinal(pods[j].Name)
	})
}

func extractPodOrdinal(name string) int32 {
	re := regexp.MustCompile(`\d+$`)
	match := re.FindString(name)
	if match == "" {
		return -1
	}
	num, err := strconv.Atoi(match)
	if err != nil {
		return -1
	}
	return int32(num)
}

func isPodReady(pod v1.Pod) bool {
	if pod.Status.Phase != v1.PodRunning {
		return false
	}
	for _, condition := range pod.Status.Conditions {
		if condition.Type == v1.PodReady && condition.Status == v1.ConditionTrue {
			return true
		}
	}
	return false
}
