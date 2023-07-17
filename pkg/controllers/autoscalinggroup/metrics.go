/*
Copyright 2020 The Kubernetes Authors.
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
	"sync"

	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

const (
	metricsSubsystem       = "autoscaling_group_controller"
	metricLabelLatencyType = "latency_type"
	metricLabelErrorType   = "error_type"
	metricLabelInstanceId  = "instance_id"
)

var (
	queuedNodeDuration = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Subsystem:      metricsSubsystem,
			Name:           "queued_node_duration_seconds",
			Help:           "Number of seconds that a single node existed in the queue",
			StabilityLevel: metrics.ALPHA,
			Buckets:        metrics.ExponentialBuckets(0.5, 1.5, 20),
		},
		[]string{metricLabelLatencyType})

	processNodeError = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      metricsSubsystem,
			Name:           "process_node_errors_total",
			Help:           "Number of errors when processing nodes",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{metricLabelErrorType, metricLabelInstanceId})
)

func recordQueuedNodeLatencyMetrics(latencyType string, timeTaken float64) {
	queuedNodeDuration.With(metrics.Labels{metricLabelLatencyType: latencyType}).Observe(timeTaken)
}

func recordProcessNodeErrorMetrics(errorType string, instanceID string) {
	processNodeError.With(metrics.Labels{metricLabelErrorType: errorType, metricLabelInstanceId: instanceID}).Inc()
}

var metricRegistration sync.Once

func registerMetrics() {
	metricRegistration.Do(func() {
		legacyregistry.MustRegister(queuedNodeDuration)
		legacyregistry.MustRegister(processNodeError)
	})
}
