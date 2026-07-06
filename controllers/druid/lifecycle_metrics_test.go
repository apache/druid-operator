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
	"testing"
	"time"

	"github.com/apache/druid-operator/apis/druid/v1alpha1"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestDeploymentLifecycleMetricsRecordFailedWhenTimedOut(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := newDeploymentLifecycleMetrics(registry)
	startedAt := metav1.NewTime(time.Unix(100, 0))
	drd := lifecycleMetricDruid(v1alpha1.DeploymentLifecycleStatus{
		Phase:     v1alpha1.DeploymentLifecycleInProgress,
		Reason:    "Waiting for Deployment [example] rollout",
		StartedAt: &startedAt,
	})

	metrics.record(drd, lifecycleDependencies{
		now:     func() time.Time { return time.Unix(110, 0) },
		timeout: 5 * time.Second,
	})

	families, err := registry.Gather()
	require.NoError(t, err)
	assert.Equal(t, 1.0, metricValue(t, families, "druid_operator_deployment_lifecycle_failed", lifecycleMetricLabels()))
}

func TestDeploymentLifecycleMetricsRecordNotFailedWhenTimeoutReasonButConfiguredTimeoutNotExceeded(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := newDeploymentLifecycleMetrics(registry)
	startedAt := metav1.NewTime(time.Unix(100, 0))
	drd := lifecycleMetricDruid(v1alpha1.DeploymentLifecycleStatus{
		Phase:     v1alpha1.DeploymentLifecycleInProgress,
		Reason:    deploymentLifecycleTimeoutReasonPrefix + " of 5s; still waiting for Deployment [example] rollout",
		StartedAt: &startedAt,
	})

	metrics.record(drd, lifecycleDependencies{
		now:     func() time.Time { return time.Unix(110, 0) },
		timeout: 30 * time.Second,
	})

	families, err := registry.Gather()
	require.NoError(t, err)
	assert.Equal(t, 0.0, metricValue(t, families, "druid_operator_deployment_lifecycle_failed", lifecycleMetricLabels()))
}

func TestDeploymentLifecycleMetricsRecordNotFailedWhenTimeoutDisabled(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := newDeploymentLifecycleMetrics(registry)
	startedAt := metav1.NewTime(time.Unix(100, 0))
	drd := lifecycleMetricDruid(v1alpha1.DeploymentLifecycleStatus{
		Phase:     v1alpha1.DeploymentLifecycleInProgress,
		StartedAt: &startedAt,
	})

	metrics.record(drd, lifecycleDependencies{
		now:     func() time.Time { return time.Unix(110, 0) },
		timeout: 0,
	})

	families, err := registry.Gather()
	require.NoError(t, err)
	assert.Equal(t, 0.0, metricValue(t, families, "druid_operator_deployment_lifecycle_failed", lifecycleMetricLabels()))
}

func TestDeploymentLifecycleMetricsDeleteRemovesSeries(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := newDeploymentLifecycleMetrics(registry)
	startedAt := metav1.NewTime(time.Unix(100, 0))
	metrics.record(lifecycleMetricDruid(v1alpha1.DeploymentLifecycleStatus{
		Phase:     v1alpha1.DeploymentLifecycleInProgress,
		StartedAt: &startedAt,
	}), lifecycleDependencies{
		now:     func() time.Time { return time.Unix(110, 0) },
		timeout: 5 * time.Second,
	})

	metrics.delete("default", "example")

	families, err := registry.Gather()
	require.NoError(t, err)
	assert.Equal(t, 0.0, metricValue(t, families, "druid_operator_deployment_lifecycle_failed", lifecycleMetricLabels()))
}

func lifecycleMetricDruid(status v1alpha1.DeploymentLifecycleStatus) *v1alpha1.Druid {
	return &v1alpha1.Druid{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "example",
			Namespace: "default",
		},
		Status: v1alpha1.DruidClusterStatus{
			DeploymentLifecycle: status,
		},
	}
}

func lifecycleMetricLabels() map[string]string {
	return map[string]string{
		"namespace":      "default",
		"druid_instance": "example",
	}
}

func metricValue(t *testing.T, families []*dto.MetricFamily, name string, labels map[string]string) float64 {
	t.Helper()
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.Metric {
			if metricHasLabels(metric, labels) {
				if metric.Gauge != nil {
					return metric.Gauge.GetValue()
				}
				if metric.Counter != nil {
					return metric.Counter.GetValue()
				}
			}
		}
	}
	return 0
}

func metricHasLabels(metric *dto.Metric, labels map[string]string) bool {
	if len(labels) == 0 {
		return true
	}

	matched := 0
	for _, label := range metric.Label {
		expected, ok := labels[label.GetName()]
		if !ok {
			continue
		}
		if label.GetValue() != expected {
			return false
		}
		matched++
	}
	return matched == len(labels)
}
