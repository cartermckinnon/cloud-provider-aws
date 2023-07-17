/*
Copyright 2023 The Kubernetes Authors.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package autoscalinggroup

import (
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func Test_getNodeConditionChecksum(t *testing.T) {
	now := metav1.NewTime(time.Now())
	testcases := []struct {
		name           string
		conditionA     []v1.NodeCondition
		conditionB     []v1.NodeCondition
		expectEquality bool
	}{
		{
			name: "Identical conditions",
			conditionA: []v1.NodeCondition{
				{
					Type:               "Foo",
					Status:             v1.ConditionTrue,
					LastHeartbeatTime:  now,
					LastTransitionTime: now,
				},
				{
					Type:               "Bar",
					Status:             v1.ConditionTrue,
					LastHeartbeatTime:  now,
					LastTransitionTime: now,
				},
			},
			conditionB: []v1.NodeCondition{
				{
					Type:               "Foo",
					Status:             v1.ConditionTrue,
					LastHeartbeatTime:  now,
					LastTransitionTime: now,
				},
				{
					Type:               "Bar",
					Status:             v1.ConditionTrue,
					LastHeartbeatTime:  now,
					LastTransitionTime: now,
				},
			},
			expectEquality: true,
		},
		{
			name: "Identical conditions with different timestamps",
			conditionA: []v1.NodeCondition{
				{
					Type:               "Foo",
					Status:             v1.ConditionTrue,
					LastHeartbeatTime:  now,
					LastTransitionTime: now,
				},
				{
					Type:               "Bar",
					Status:             v1.ConditionTrue,
					LastHeartbeatTime:  now,
					LastTransitionTime: now,
				},
			},
			conditionB: []v1.NodeCondition{
				{
					Type:               "Foo",
					Status:             v1.ConditionTrue,
					LastHeartbeatTime:  metav1.NewTime(now.Add(time.Minute * 4)),
					LastTransitionTime: metav1.NewTime(now.Add(time.Minute * 3)),
				},
				{
					Type:               "Bar",
					Status:             v1.ConditionTrue,
					LastHeartbeatTime:  metav1.NewTime(now.Add(time.Minute * 2)),
					LastTransitionTime: metav1.NewTime(now.Add(time.Minute * 1)),
				},
			},
			expectEquality: true,
		},
		{
			name: "Identical conditions in different order",
			conditionA: []v1.NodeCondition{
				{
					Type:               "Foo",
					Status:             v1.ConditionTrue,
					LastHeartbeatTime:  now,
					LastTransitionTime: now,
				},
				{
					Type:               "Bar",
					Status:             v1.ConditionTrue,
					LastHeartbeatTime:  now,
					LastTransitionTime: now,
				},
			},
			conditionB: []v1.NodeCondition{
				{
					Type:               "Bar",
					Status:             v1.ConditionTrue,
					LastHeartbeatTime:  now,
					LastTransitionTime: now,
				},
				{
					Type:               "Foo",
					Status:             v1.ConditionTrue,
					LastHeartbeatTime:  now,
					LastTransitionTime: now,
				},
			},
			expectEquality: true,
		},
		{
			name: "Same condition type with different status",
			conditionA: []v1.NodeCondition{
				{
					Type:               "Foo",
					Status:             v1.ConditionTrue,
					LastHeartbeatTime:  now,
					LastTransitionTime: now,
				},
			},
			conditionB: []v1.NodeCondition{
				{
					Type:               "Foo",
					Status:             v1.ConditionFalse,
					LastHeartbeatTime:  now,
					LastTransitionTime: now,
				},
			},
			expectEquality: false,
		},
		{
			name: "Same condition status with different type",
			conditionA: []v1.NodeCondition{
				{
					Type:               "Foo",
					Status:             v1.ConditionTrue,
					LastHeartbeatTime:  now,
					LastTransitionTime: now,
				},
			},
			conditionB: []v1.NodeCondition{
				{
					Type:               "Bar",
					Status:             v1.ConditionTrue,
					LastHeartbeatTime:  now,
					LastTransitionTime: now,
				},
			},
			expectEquality: false,
		},
	}

	for _, testcase := range testcases {
		t.Run(testcase.name, func(t *testing.T) {
			node := v1.Node{}

			node.Status.Conditions = testcase.conditionA
			checksumA := getNodeConditionChecksum(&node)

			node.Status.Conditions = testcase.conditionB
			checksumB := getNodeConditionChecksum(&node)

			t.Logf("%s: checksumA=%v checksumB=%v", testcase.name, checksumA, checksumB)

			if (checksumA == checksumB) != testcase.expectEquality {
				t.Errorf("%s: expectEquality=%v checksumA=%v checksumB=%v", testcase.name, testcase.expectEquality, checksumA, checksumB)
			}
		})
	}
}
