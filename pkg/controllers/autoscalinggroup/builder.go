package autoscalinggroup

import (
	"context"

	"github.com/pkg/errors"
	cloudprovider "k8s.io/cloud-provider"
	"k8s.io/cloud-provider/app"
	cloudcontrollerconfig "k8s.io/cloud-provider/app/config"
	genericcontrollermanager "k8s.io/controller-manager/app"
	"k8s.io/controller-manager/controller"
	"k8s.io/klog/v2"
)

// AutoScalingGroupControllerBuilder is the builder for the autoscaling group controller
type AutoScalingGroupControllerBuilder struct {
	Options AutoScalingGroupControllerOptions
}

// Constructor returns a function to configure and run the autoscaling group controller
func (tc *AutoScalingGroupControllerBuilder) Constructor(initContext app.ControllerInitContext, completedConfig *cloudcontrollerconfig.CompletedConfig, cloud cloudprovider.Interface) app.InitFunc {
	return func(ctx context.Context, controllerContext genericcontrollermanager.ControllerContext) (controller.Interface, bool, error) {
		return tc.configureAndRun(ctx, initContext, completedConfig, cloud)
	}
}

func (tc *AutoScalingGroupControllerBuilder) configureAndRun(ctx context.Context, initContext app.ControllerInitContext, completedConfig *cloudcontrollerconfig.CompletedConfig, cloud cloudprovider.Interface) (controller.Interface, bool, error) {
	err := tc.Options.Validate()
	if err != nil {
		return nil, false, errors.Wrap(err, "AutoScaling group controller options are invalid")
	}
	nodeHealthController, err := NewAutoScalingGroupController(
		completedConfig.SharedInformers.Core().V1().Nodes(),
		completedConfig.ClientBuilder.ClientOrDie(initContext.ClientName),
		cloud,
		completedConfig.ComponentConfig.KubeCloudShared.NodeMonitorPeriod.Duration,
		tc.Options.AutoScalingGroupMonitorPeriod,
		tc.Options.RateLimit,
		tc.Options.BurstLimit,
		tc.Options.NewNodeGracePeriod,
		completedConfig.ComponentConfig.KubeCloudShared.ClusterName,
	)
	if err != nil {
		klog.Warningf("failed to create autoscaling group controller: %s", err)
		return nil, false, nil
	}
	go nodeHealthController.Run(ctx.Done())
	return nil, true, nil
}
