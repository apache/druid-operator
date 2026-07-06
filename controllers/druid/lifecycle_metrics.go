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
	"time"

	"github.com/apache/druid-operator/apis/druid/v1alpha1"
	"github.com/prometheus/client_golang/prometheus"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

type deploymentLifecycleMetrics struct {
	failed *prometheus.GaugeVec
}

var defaultDeploymentLifecycleMetrics = newDeploymentLifecycleMetrics(ctrlmetrics.Registry)

func newDeploymentLifecycleMetrics(registerer prometheus.Registerer) *deploymentLifecycleMetrics {
	metrics := &deploymentLifecycleMetrics{
		failed: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "druid_operator_deployment_lifecycle_failed",
				Help: "Whether the current Druid deployment lifecycle is considered failed for metric consumers.",
			},
			[]string{"namespace", "druid_instance"},
		),
	}

	if registerer != nil {
		registerer.MustRegister(metrics.failed)
	}

	return metrics
}

func (m *deploymentLifecycleMetrics) record(drd *v1alpha1.Druid, deps lifecycleDependencies) {
	if m == nil || drd == nil {
		return
	}

	m.failed.WithLabelValues(drd.Namespace, drd.Name).Set(boolToFloat(deploymentLifecycleMetricFailed(drd.Status.DeploymentLifecycle, deps)))
}

func (m *deploymentLifecycleMetrics) delete(namespace, druidName string) {
	if m == nil {
		return
	}

	m.failed.DeleteLabelValues(namespace, druidName)
}

func deploymentLifecycleMetricFailed(status v1alpha1.DeploymentLifecycleStatus, deps lifecycleDependencies) bool {
	if status.Phase != v1alpha1.DeploymentLifecycleInProgress || status.StartedAt == nil || deps.timeout <= 0 {
		return false
	}

	now := time.Now
	if deps.now != nil {
		now = deps.now
	}
	return !now().Before(status.StartedAt.Time.Add(deps.timeout))
}

func boolToFloat(value bool) float64 {
	if value {
		return 1
	}
	return 0
}
