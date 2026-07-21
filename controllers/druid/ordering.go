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
	"sort"

	"github.com/apache/druid-operator/apis/druid/v1alpha1"
)

var (
	defaultDruidServicesOrder = []string{historical, overlord, middleManager, indexer, broker, coordinator, router}
)

type ServiceGroup struct {
	key      string
	nodeType string
	tier     string
	spec     v1alpha1.DruidNodeSpec
}

func getNodeSpecsByOrder(m *v1alpha1.Druid) []*ServiceGroup {
	nodeTypeOrder := defaultDruidServicesOrder
	if len(m.Spec.OrderOfUpgrade) > 0 {
		nodeTypeOrder = m.Spec.OrderOfUpgrade
	}

	groupsByNodeType := map[string][]*ServiceGroup{}
	for _, t := range nodeTypeOrder {
		groupsByNodeType[t] = []*ServiceGroup{}
	}

	for key, nodeSpec := range m.Spec.Nodes {
		sg := &ServiceGroup{
			key:      key,
			nodeType: nodeSpec.NodeType,
			tier:     nodeSpec.Tier,
			spec:     nodeSpec,
		}
		groupsByNodeType[nodeSpec.NodeType] = append(groupsByNodeType[nodeSpec.NodeType], sg)
	}

	for nodeType, groups := range groupsByNodeType {
		tierOrder := m.Spec.OrderOfUpgradeOfTiers[nodeType]
		sortServiceGroups(groups, tierOrder)
		groupsByNodeType[nodeType] = groups
	}

	result := make([]*ServiceGroup, 0, len(m.Spec.Nodes))
	for _, t := range nodeTypeOrder {
		result = append(result, groupsByNodeType[t]...)
	}

	return result
}

func sortServiceGroups(groups []*ServiceGroup, tierOrder []string) {
	if len(groups) <= 1 {
		return
	}

	tierRank := make(map[string]int, len(tierOrder))
	for i, t := range tierOrder {
		tierRank[t] = i
	}

	sort.SliceStable(groups, func(i, j int) bool {
		gi, gj := groups[i], groups[j]

		if len(tierOrder) > 0 {
			ri, okI := tierRank[gi.tier]
			rj, okJ := tierRank[gj.tier]

			switch {
			case okI && okJ && ri != rj:
				return ri < rj
			case okI && !okJ:
				return true
			case !okI && okJ:
				return false
			}
		}

		return gi.key < gj.key
	})
}
