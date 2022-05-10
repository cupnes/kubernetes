/*
Copyright 2021 The Kubernetes Authors.

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

package volumebinding

import (
	"math"

	v1 "k8s.io/api/core/v1"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/helper"
)

// classResourceMap holds a map of storage class to resource.
type classResourceMap map[string]*StorageResource

// volumeCapacityScorer calculates the score based on class storage resource information.
type volumeCapacityScorer func(classResourceMap) int64

type dynamicProvisionScorer func([]*DynamicProvision) int64

// buildScorerFunction builds volumeCapacityScorer from the scoring function shape.
func buildScorerFunction(scoringFunctionShape helper.FunctionShape) volumeCapacityScorer {
	rawScoringFunction := helper.BuildBrokenLinearFunction(scoringFunctionShape)
	f := func(requested, capacity int64) int64 {
		if capacity == 0 || requested > capacity {
			return rawScoringFunction(maxUtilization)
		}

		return rawScoringFunction(requested * maxUtilization / capacity)
	}
	return func(classResources classResourceMap) int64 {
		var nodeScore int64
		// in alpha stage, all classes have the same weight
		weightSum := len(classResources)
		if weightSum == 0 {
			return 0
		}
		for _, resource := range classResources {
			classScore := f(resource.Requested, resource.Capacity)
			nodeScore += classScore
		}
		return int64(math.Round(float64(nodeScore) / float64(weightSum)))
	}
}

func dynamicProvisionScorerImpl(provisions []*DynamicProvision) int64 {
	classResources := make(classResourceMap)
	for _, provision := range provisions {
		if provision.Capacity == nil {
			continue
		}
		class := *provision.PVC.Spec.StorageClassName
		if _, ok := classResources[class]; !ok {
			classResources[class] = &StorageResource{
				Requested: 0,
				Capacity:  0,
			}
		}
		if provision.Capacity.Capacity.Value() > classResources[class].Capacity {
			classResources[class].Capacity = provision.Capacity.Capacity.Value()
		}

		requestedQty := provision.PVC.Spec.Resources.Requests[v1.ResourceName(v1.ResourceStorage)]
		classResources[class].Requested += requestedQty.Value()
	}

	if len(classResources) == 0 {
		return 0
	}

	var score float64
	for _, resource := range classResources {
		score += (float64(resource.Requested) / float64(resource.Capacity)) * 100
	}
	return int64(math.Floor(score / float64(len(classResources))))
}
