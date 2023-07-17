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
	"fmt"
	"time"

	"github.com/spf13/pflag"
)

// AutoScalingGroupControllerOptions contains the inputs that can
// be used in the autoscaling group controller
type AutoScalingGroupControllerOptions struct {
	RateLimit                     float64
	BurstLimit                    int
	NewNodeGracePeriod            time.Duration
	AutoScalingGroupMonitorPeriod time.Duration
}

// AddFlags add the additional flags for the controller
func (o *AutoScalingGroupControllerOptions) AddFlags(fs *pflag.FlagSet) {
	fs.Float64Var(&o.RateLimit, "autoscaling-group-controller-rate-limit", o.RateLimit,
		"Steady-state rate limit (per sec) at which the controller processes items in its queue. A value of zero (default) disables rate limiting.")
	fs.IntVar(&o.BurstLimit, "autoscaling-group-controller-burst-limit", o.BurstLimit,
		"Burst limit at which the controller processes items in its queue. A value of zero (default) disables rate limiting.")
	fs.DurationVar(&o.NewNodeGracePeriod, "autoscaling-group-controller-new-node-grace-period", time.Minute*2,
		"Amount of time after Node creation or instance launch that health will not be reported to the cloud provider")
	fs.DurationVar(&o.AutoScalingGroupMonitorPeriod, "autoscaling-group-controller-monitor-period", time.Minute*2,
		"Interval at which AutoScaling groups are checked for participation in the cluster")
}

// Validate checks for errors from user input
func (o *AutoScalingGroupControllerOptions) Validate() error {
	if o.RateLimit < 0.0 {
		return fmt.Errorf("--autoscaling-group-controller-rate-limit should not be less than zero")
	}
	if o.BurstLimit < 0 {
		return fmt.Errorf("--autoscaling-group-controller-burst-limit should not be less than zero")
	}
	return nil
}
