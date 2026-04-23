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
	"reflect"
	"sort"
	"testing"
	"time"

	druidv1alpha1 "github.com/apache/druid-operator/apis/druid/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/types"
)

// +kubebuilder:docs-gen:collapse=Imports

/*
ordering_test
*/
var _ = Describe("Test ordering logic", func() {
	const (
		filePath = "testdata/ordering.yaml"
		timeout  = time.Second * 45
		interval = time.Millisecond * 250
	)

	var (
		druid = &druidv1alpha1.Druid{}
	)

	Context("When creating a druid cluster with multiple nodes", func() {
		It("Should create the druid object", func() {
			By("Creating a new druid")
			druidCR, err := readDruidClusterSpecFromFile(filePath)
			Expect(err).Should(BeNil())
			Expect(k8sClient.Create(ctx, druidCR)).To(Succeed())

			By("Getting a newly created druid")
			Eventually(func() bool {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: druidCR.Name, Namespace: druidCR.Namespace}, druid)
				return err == nil
			}, timeout, interval).Should(BeTrue())
		})
		It("Should return a deterministic, lexicographically ordered list of nodes within each NodeType", func() {
			orderedServiceGroups := getNodeSpecsByOrder(druid)
			// Three historical tiers (historicalstier1–3) all share NodeType
			// "historical" and must come back sorted by key so rollingDeploy
			// can never roll two of them out at the same time.
			Expect(orderedServiceGroups[0].key).Should(Equal("historicalstier1"))
			Expect(orderedServiceGroups[1].key).Should(Equal("historicalstier2"))
			Expect(orderedServiceGroups[2].key).Should(Equal("historicalstier3"))
			Expect(orderedServiceGroups[3].key).Should(Equal("overlords"))
			Expect(orderedServiceGroups[4].key).Should(Equal("middle-managers"))
			Expect(orderedServiceGroups[5].key).Should(Equal("indexers"))
			Expect(orderedServiceGroups[6].key).Should(Equal("brokers"))
			Expect(orderedServiceGroups[7].key).Should(Equal("coordinators"))
			Expect(orderedServiceGroups[8].key).Should(Equal("routers"))
		})
	})
})

// determinismCallCount is the number of times getNodeSpecsByOrder is invoked
// per test to surface map-iteration non-determinism. Without the sort step in
// getNodeSpecsByOrder, randomized map iteration over m.Spec.Nodes makes the
// intra-NodeType order flap. With many specs sharing one NodeType and many
// repeated calls, the probability of observing at least one differing order
// approaches 1, which is exactly what we want for a regression test.
const determinismCallCount = 200

// makeMultiHistoricalDruid returns a Druid CR with several node specs sharing
// the same "historical" NodeType, plus one spec per other NodeType. This is
// the shape that triggered the bug fixed here: multiple StatefulSets/
// Deployments belonging to a single NodeType.
func makeMultiHistoricalDruid() *druidv1alpha1.Druid {
	return &druidv1alpha1.Druid{
		Spec: druidv1alpha1.DruidSpec{
			Nodes: map[string]druidv1alpha1.DruidNodeSpec{
				"historicalstier1": {NodeType: historical},
				"historicalstier2": {NodeType: historical},
				"historicalstier3": {NodeType: historical},
				"historicalstier4": {NodeType: historical},
				"brokers":          {NodeType: broker},
				"coordinators":     {NodeType: coordinator},
				"overlords":        {NodeType: overlord},
				"middle-managers":  {NodeType: middleManager},
				"indexers":         {NodeType: indexer},
				"routers":          {NodeType: router},
			},
		},
	}
}

// keysOfServiceGroups extracts the ordered list of keys from a slice of
// *ServiceGroup so test assertions can compare orderings as plain strings.
func keysOfServiceGroups(specs []*ServiceGroup) []string {
	out := make([]string, len(specs))
	for i, s := range specs {
		out[i] = s.key
	}
	return out
}

// TestGetNodeSpecsByOrder_DeterministicAcrossCalls invokes getNodeSpecsByOrder
// many times on the same Druid CR and asserts every call returns the exact
// same ordering. Before the fix, Go's randomized map iteration over
// m.Spec.Nodes causes the order of specs sharing a NodeType (e.g. the four
// "historical" entries) to flap between calls, so this test fails. With the
// sort.Slice fix, all calls return the same ordering and the test passes.
func TestGetNodeSpecsByOrder_DeterministicAcrossCalls(t *testing.T) {
	druid := makeMultiHistoricalDruid()

	first := keysOfServiceGroups(getNodeSpecsByOrder(druid))

	for i := 1; i < determinismCallCount; i++ {
		got := keysOfServiceGroups(getNodeSpecsByOrder(druid))
		if !reflect.DeepEqual(first, got) {
			t.Fatalf(
				"getNodeSpecsByOrder is non-deterministic: call 0 returned %v, call %d returned %v",
				first, i, got,
			)
		}
	}
}

// TestGetNodeSpecsByOrder_LexicographicWithinNodeType asserts the contractual
// intra-NodeType ordering: ascending by spec key. This pins down the exact
// behavior the operator relies on for sequential rolling deploy.
func TestGetNodeSpecsByOrder_LexicographicWithinNodeType(t *testing.T) {
	druid := makeMultiHistoricalDruid()

	got := keysOfServiceGroups(getNodeSpecsByOrder(druid))

	wantHistoricals := []string{"historicalstier1", "historicalstier2", "historicalstier3", "historicalstier4"}
	gotHistoricals := got[:len(wantHistoricals)]

	if !sort.StringsAreSorted(gotHistoricals) {
		t.Errorf("historical specs must be sorted ascending by key, got %v", gotHistoricals)
	}
	if !reflect.DeepEqual(gotHistoricals, wantHistoricals) {
		t.Errorf("historical block ordering mismatch: want %v, got %v", wantHistoricals, gotHistoricals)
	}

	want := []string{
		"historicalstier1", "historicalstier2", "historicalstier3", "historicalstier4",
		"overlords",
		"middle-managers",
		"indexers",
		"brokers",
		"coordinators",
		"routers",
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("full ordering mismatch:\nwant %v\n got %v", want, got)
	}
}

// TestGetNodeSpecsByOrder_NodeTypeOrderPreserved guards against a regression
// in the cross-NodeType ordering defined by druidServicesOrder.
func TestGetNodeSpecsByOrder_NodeTypeOrderPreserved(t *testing.T) {
	druid := &druidv1alpha1.Druid{
		Spec: druidv1alpha1.DruidSpec{
			Nodes: map[string]druidv1alpha1.DruidNodeSpec{
				"routers":      {NodeType: router},
				"coordinators": {NodeType: coordinator},
				"brokers":      {NodeType: broker},
				"historicals":  {NodeType: historical},
				"overlords":    {NodeType: overlord},
			},
		},
	}

	got := keysOfServiceGroups(getNodeSpecsByOrder(druid))
	want := []string{"historicals", "overlords", "brokers", "coordinators", "routers"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("NodeType ordering broken: want %v, got %v", want, got)
	}
}
