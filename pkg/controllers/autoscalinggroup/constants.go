package autoscalinggroup

const (
	// Tag key for AutoScaling groups to control whether the controller iteracts with it
	AutoScalingGroupControllerEnabledTagKey = "aws.cloudprovider.kubernetes.io/autoscaling-group-controller/enabled"

	// AutoScalingGroupControllerClientName is the name of the autoscaling group controller
	AutoScalingGroupControllerClientName = "autoscaling-group-controller"

	// AutoScalingGroupControllerKey is the key used to register this controller
	AutoScalingGroupControllerKey = "autoscaling-group"
)
