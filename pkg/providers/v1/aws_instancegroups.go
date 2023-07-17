/*
Copyright 2014 The Kubernetes Authors.

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

package aws

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/autoscaling"
	"github.com/aws/aws-sdk-go/service/ec2"
	ec2api "github.com/aws/aws-sdk-go/service/ec2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
)

// AWSCloud implements InstanceGroups
var _ InstanceGroups = &Cloud{}

// ResizeInstanceGroup sets the size of the specificed instancegroup Exported
// so it can be used by the e2e tests, which don't want to instantiate a full
// cloudprovider.
func ResizeInstanceGroup(asg ASG, instanceGroupName string, size int) error {
	request := &autoscaling.UpdateAutoScalingGroupInput{
		AutoScalingGroupName: aws.String(instanceGroupName),
		DesiredCapacity:      aws.Int64(int64(size)),
	}
	if _, err := asg.UpdateAutoScalingGroup(request); err != nil {
		return fmt.Errorf("error resizing AWS autoscaling group: %q", err)
	}
	return nil
}

// ResizeInstanceGroup implements InstanceGroups.ResizeInstanceGroup
// Set the size to the fixed size
func (c *Cloud) ResizeInstanceGroup(instanceGroupName string, size int) error {
	return ResizeInstanceGroup(c.asg, instanceGroupName, size)
}

// DescribeInstanceGroup gets info about the specified instancegroup
// Exported so it can be used by the e2e tests,
// which don't want to instantiate a full cloudprovider.
func DescribeInstanceGroup(asg ASG, instanceGroupName string) (InstanceGroupInfo, error) {
	request := &autoscaling.DescribeAutoScalingGroupsInput{
		AutoScalingGroupNames: []*string{aws.String(instanceGroupName)},
	}
	response, err := asg.DescribeAutoScalingGroups(request)
	if err != nil {
		return nil, fmt.Errorf("error listing AWS autoscaling group (%s): %q", instanceGroupName, err)
	}

	if len(response.AutoScalingGroups) == 0 {
		return nil, nil
	}
	if len(response.AutoScalingGroups) > 1 {
		klog.Warning("AWS returned multiple autoscaling groups with name ", instanceGroupName)
	}
	group := response.AutoScalingGroups[0]
	return &awsInstanceGroup{group: group}, nil
}

// DescribeInstanceGroup implements InstanceGroups.DescribeInstanceGroup
// Queries the cloud provider for information about the specified instance group
func (c *Cloud) DescribeInstanceGroup(instanceGroupName string) (InstanceGroupInfo, error) {
	return DescribeInstanceGroup(c.asg, instanceGroupName)
}

// SetHealthOfInstanceInGroup implements InstanceGroups.SetHealthOfInstanceInGroup
// Reports the instance health to the cloud provider's instance group control plane.
func SetHealthOfInstanceInGroup(asg ASG, instanceId string, healthy bool) error {
	input := &autoscaling.SetInstanceHealthInput{
		InstanceId:   aws.String(instanceId),
		HealthStatus: aws.String("Healthy"),
	}
	if !healthy {
		input.HealthStatus = aws.String("Unhealthy")
	}
	_, err := asg.SetInstanceHealth(input)
	return err
}

// SetHealthOfInstanceInGroup implements InstanceGroups.SetHealthOfInstanceInGroup
// Reports the instance health to the cloud provider's instance group control plane.
func (c *Cloud) SetHealthOfInstanceInGroup(instanceId string, healthy bool) error {
	return SetHealthOfInstanceInGroup(c.asg, instanceId, healthy)
}

// DescribeInstanceGroupsForCluster implements InstanceGroups.DescribeInstanceGroupsForCluster
// Reports the instance health to the cloud provider's instance group control plane.
func DescribeInstanceGroupsForCluster(asg ASG, clusterName string) ([]InstanceGroupInfo, error) {
	req := &autoscaling.DescribeAutoScalingGroupsInput{
		Filters: []*autoscaling.Filter{
			{
				Name:   aws.String("tag-key"),
				Values: []*string{aws.String(fmt.Sprintf("kubernetes.io/cluster/%s", clusterName))},
			},
		},
	}
	res, err := asg.DescribeAutoScalingGroups(req)
	if err != nil {
		return nil, err
	}
	var instanceGroups []InstanceGroupInfo
	for _, group := range res.AutoScalingGroups {
		instanceGroups = append(instanceGroups, &awsInstanceGroup{
			group: group,
		})
	}
	return instanceGroups, nil
}

// DescribeInstanceGroupsForCluster implements InstanceGroups.DescribeInstanceGroupsForCluster
// Reports the instance health to the cloud provider's instance group control plane.
func (c *Cloud) DescribeInstanceGroupsForCluster(clusterName string) ([]InstanceGroupInfo, error) {
	return DescribeInstanceGroupsForCluster(c.asg, clusterName)
}

func DescribeInstancesForGroup(ec2 EC2, instanceGroupName string) ([]InstanceInfo, error) {
	req := &ec2api.DescribeInstancesInput{
		Filters: []*ec2api.Filter{
			{
				Name:   aws.String("tag:aws:autoscaling:groupName"),
				Values: []*string{aws.String(instanceGroupName)},
			},
			{
				Name:   aws.String("instance-state-name"),
				Values: []*string{aws.String("running")},
			},
		},
	}
	res, err := ec2.DescribeInstances(req)
	if err != nil {
		return nil, err
	}
	var instances []InstanceInfo
	for _, instance := range res {
		instances = append(instances, &awsInstanceInfo{
			instance: instance,
		})
	}
	return instances, nil
}

func (c *Cloud) DescribeInstancesForGroup(instanceGroupName string) ([]InstanceInfo, error) {
	return DescribeInstancesForGroup(c.ec2, instanceGroupName)
}

func GetGroupNameForInstance(asg ASG, instanceID InstanceID) (*string, error) {
	request := &autoscaling.DescribeAutoScalingInstancesInput{
		InstanceIds: []*string{aws.String(string(instanceID))},
	}
	response, err := asg.DescribeAutoScalingInstances(request)
	if err != nil {
		return nil, fmt.Errorf("error describing AWS autoscaling instance (%s): %q", instanceID, err)
	}

	if len(response.AutoScalingInstances) == 0 {
		return nil, nil
	}
	instance := response.AutoScalingInstances[0]
	return instance.AutoScalingGroupName, nil
}

func (c *Cloud) GetGroupNameForInstance(instanceID InstanceID) (*string, error) {
	return GetGroupNameForInstance(c.asg, instanceID)
}

// awsInstanceGroup implements InstanceGroupInfo
var _ InstanceGroupInfo = &awsInstanceGroup{}

type awsInstanceGroup struct {
	group *autoscaling.Group
}

// Implement InstanceGroupInfo.CurrentSize
// The number of instances currently running under control of this group
func (g *awsInstanceGroup) CurrentSize() (int, error) {
	return len(g.group.Instances), nil
}

// Implement InstanceGroupInfo.Name
// The name of the autoscaling group
func (g *awsInstanceGroup) Name() string {
	return aws.StringValue(g.group.AutoScalingGroupName)
}

// Implement InstanceGroupInfo.GetProperty
func (g *awsInstanceGroup) GetProperty(name string) (string, bool) {
	for _, tag := range g.group.Tags {
		if aws.StringValue(tag.Key) == name {
			return aws.StringValue(tag.Value), true
		}
	}
	return "", false
}

var _ InstanceInfo = &awsInstanceInfo{}

type awsInstanceInfo struct {
	instance *ec2.Instance
}

func (i *awsInstanceInfo) ID() InstanceID {
	return InstanceID(aws.StringValue(i.instance.InstanceId))
}

func (i *awsInstanceInfo) CreationTimestamp() (metav1.Time, error) {
	launchTime := i.instance.LaunchTime
	if launchTime == nil {
		return metav1.Time{}, fmt.Errorf("instance %s has no launch time", i.ID())
	}
	return metav1.NewTime(*launchTime), nil
}
