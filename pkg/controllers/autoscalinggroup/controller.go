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
	"hash/adler32"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"golang.org/x/time/rate"
	v1 "k8s.io/api/core/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	coreinformers "k8s.io/client-go/informers/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	cloudprovider "k8s.io/cloud-provider"
	awsv1 "k8s.io/cloud-provider-aws/pkg/providers/v1"
	"k8s.io/klog/v2"
)

func init() {
	registerMetrics()
}

// queuedNode contains the node and details about its time in the queue
type queuedNode struct {
	node           *v1.Node
	requeuingCount int
	enqueueTime    time.Time
}

type cachedInstanceInfo struct {
	autoScalingGroupName      string
	lastNodeConditionChecksum uint32
}

func (w queuedNode) String() string {
	return fmt.Sprintf("[Node: %s, RequeuingCount: %d, EnqueueTime: %s]", w.node.GetName(), w.requeuingCount, w.enqueueTime)
}

const (
	maxRequeuingCount = 9

	// The label for depicting total number of errors a queued node encounter and succeed
	totalErrorsWorkItemErrorMetric = "total_errors"

	// The label for depicting total time when queued node gets queued to processed
	workItemProcessingTimeWorkItemMetric = "work_item_processing_time"

	// The label for depicting total time when queued node gets queued to dequeued
	workItemDequeuingTimeWorkItemMetric = "work_item_dequeuing_time"

	// The label for depicting total number of errors a queued node encounter and fail
	errorsAfterRetriesExhaustedWorkItemErrorMetric = "errors_after_retries_exhausted"
)

type AutoScalingGroupController interface {
	Run(<-chan struct{})
}

type autoScalingGroupController struct {
	nodeInformer  coreinformers.NodeInformer
	kubeClient    clientset.Interface
	cloud         *awsv1.Cloud
	nodeQueue     workqueue.RateLimitingInterface
	nodesSynced   cache.InformerSynced
	clusterName   string
	clusterTagKey string

	// Value controlling monitoring period, i.e. how often does controller
	// check node list. This value should be lower than nodeMonitorGracePeriod
	// set in controller-manager
	nodeMonitorPeriod time.Duration

	autoScalingGroupMonitorPeriod time.Duration

	rateLimitEnabled bool

	newNodeGracePeriod time.Duration

	nodeInfoCache     map[string]cachedInstanceInfo
	nodeInfoCacheLock *sync.RWMutex

	autoScalingGroupStatus map[string]bool
}

// NewAutoScalingGroupController creates a new autoscaling group controller.
func NewAutoScalingGroupController(
	nodeInformer coreinformers.NodeInformer,
	kubeClient clientset.Interface,
	cloud cloudprovider.Interface,
	nodeMonitorPeriod time.Duration,
	autoScalingGroupMonitorPeriod time.Duration,
	rateLimit float64,
	burstLimit int,
	newNodeGracePeriod time.Duration,
	clusterName string) (AutoScalingGroupController, error) {

	awsCloud, ok := cloud.(*awsv1.Cloud)
	if !ok {
		err := fmt.Errorf("autoscaling group controller does not support %v provider", cloud.ProviderName())
		return nil, err
	}

	var rateLimiter workqueue.RateLimiter
	var rateLimitEnabled bool
	if rateLimit > 0.0 && burstLimit > 0 {
		//klog.Infof("Rate limit enabled on controller with rate %f and burst %d.", rateLimit, burstLimit)
		// This is the workqueue.DefaultControllerRateLimiter() but in case where throttling is enabled on the controller,
		// the rate and burst values are set to the provided values.
		rateLimiter = workqueue.NewMaxOfRateLimiter(
			workqueue.NewItemExponentialFailureRateLimiter(5*time.Millisecond, 1000*time.Second),
			&workqueue.BucketRateLimiter{Limiter: rate.NewLimiter(rate.Limit(rateLimit), burstLimit)},
		)
		rateLimitEnabled = true
	} else {
		//klog.Infof("Rate limit disabled on controller.")
		rateLimiter = workqueue.DefaultControllerRateLimiter()
		rateLimitEnabled = false
	}

	agc := &autoScalingGroupController{
		nodeInformer:                  nodeInformer,
		kubeClient:                    kubeClient,
		cloud:                         awsCloud,
		clusterName:                   clusterName,
		nodeQueue:                     workqueue.NewNamedRateLimitingQueue(rateLimiter, "AutoScaling group"),
		nodesSynced:                   nodeInformer.Informer().HasSynced,
		nodeMonitorPeriod:             nodeMonitorPeriod,
		autoScalingGroupMonitorPeriod: autoScalingGroupMonitorPeriod,
		rateLimitEnabled:              rateLimitEnabled,
		newNodeGracePeriod:            newNodeGracePeriod,
		nodeInfoCache:                 make(map[string]cachedInstanceInfo),
		nodeInfoCacheLock:             &sync.RWMutex{},
		autoScalingGroupStatus:        make(map[string]bool),
	}

	return agc, nil
}

// Run will start the controller.
func (agc *autoScalingGroupController) Run(stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer agc.nodeQueue.ShutDown()

	// Use shared informer to listen to update/delete of nodes. Note that any nodes
	// that exist before controller starts will show up in the update method
	agc.nodeInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		UpdateFunc: func(oldObj, newObj interface{}) {
			node := newObj.(*v1.Node)
			if time.Since(node.ObjectMeta.CreationTimestamp.Time) < agc.newNodeGracePeriod {
				klog.Infof("Skip processing the node %s since it was created less than %v ago", node.Name, agc.newNodeGracePeriod)
			} else {
				agc.enqueueNode(node)
			}
		},
		DeleteFunc: func(deletedObj interface{}) {
			node := deletedObj.(*v1.Node)
			agc.nodeInfoCacheLock.Lock()
			defer agc.nodeInfoCacheLock.Unlock()
			delete(agc.nodeInfoCache, node.Name)
		},
	})

	// Wait for the caches to be synced before starting workers
	klog.Info("Waiting for informer caches to sync")
	if ok := cache.WaitForCacheSync(stopCh, agc.nodesSynced); !ok {
		klog.Errorf("Failed to wait for caches to sync")
		return
	}

	klog.Infof("Starting the autoscaling group controller")

	agc.refreshAutoScalingGroups()

	go wait.Until(func() {
		for agc.pollNodeQueue() {
		}
	}, agc.nodeMonitorPeriod, stopCh)

	go wait.Until(func() {
		agc.refreshAutoScalingGroups()
		agc.checkAutoScalingGroups()
	}, agc.autoScalingGroupMonitorPeriod, stopCh)

	<-stopCh
}

// pollNodeQueue reads one queued node from the queue and processes it.
func (agc *autoScalingGroupController) pollNodeQueue() bool {
	obj, shutdown := agc.nodeQueue.Get()
	if shutdown {
		return false
	}
	//klog.Infof("Starting to process %s", obj)

	err := func(obj interface{}) error {
		defer agc.nodeQueue.Done(obj)
		queuedNode, ok := obj.(*queuedNode)
		if !ok {
			agc.nodeQueue.Forget(obj)
			err := fmt.Errorf("expected queuedNode in workqueue but got %s", obj)
			utilruntime.HandleError(err)
			return nil
		}

		timeTaken := time.Since(queuedNode.enqueueTime).Seconds()
		recordQueuedNodeLatencyMetrics(workItemDequeuingTimeWorkItemMetric, timeTaken)

		instanceID, err := awsv1.KubernetesInstanceID(queuedNode.node.Spec.ProviderID).MapToAWSInstanceID()
		if err != nil {
			err = fmt.Errorf("Error in getting instanceID for node %s, error: %v", queuedNode.node.GetName(), err)
			utilruntime.HandleError(err)
			return nil
		}
		//klog.Infof("Instance ID of queued node %s is %s", queuedNode, instanceID)

		if awsv1.IsFargateNode(string(instanceID)) {
			klog.Infof("Skip processing the node %s since it is a Fargate node", instanceID)
			agc.nodeQueue.Forget(obj)
			return nil
		}

		err = agc.processNode(queuedNode.node)

		if err != nil {
			if queuedNode.requeuingCount < maxRequeuingCount {
				// Put the item back on the workqueue to handle any transient errors.
				queuedNode.requeuingCount++
				agc.nodeQueue.AddRateLimited(queuedNode)

				recordProcessNodeErrorMetrics(totalErrorsWorkItemErrorMetric, string(instanceID))
				return fmt.Errorf("error processing queued node '%v': %s, requeuing count %d", queuedNode, err.Error(), queuedNode.requeuingCount)
			}

			klog.Errorf("Error processing queued node %s: %s, requeuing count exceeded", queuedNode, err.Error())
			recordProcessNodeErrorMetrics(errorsAfterRetriesExhaustedWorkItemErrorMetric, string(instanceID))
		} else {
			//klog.Infof("Finished processing %s", queuedNode)
			timeTaken = time.Since(queuedNode.enqueueTime).Seconds()
			recordQueuedNodeLatencyMetrics(workItemProcessingTimeWorkItemMetric, timeTaken)
			//klog.Infof("Processing latency %s", timeTaken)
		}

		agc.nodeQueue.Forget(obj)
		return nil
	}(obj)

	if err != nil {
		klog.Errorf("Error occurred while processing %s", obj)
		utilruntime.HandleError(err)
	}

	return true
}

func (agc *autoScalingGroupController) processNode(node *v1.Node) error {
	instanceID, _ := awsv1.KubernetesInstanceID(node.Spec.ProviderID).MapToAWSInstanceID()

	nodeInfo, exists := agc.nodeInfoCache[node.Name]
	if !exists {
		// figure out which ASG the node is assigned to
		groupName, err := agc.cloud.GetGroupNameForInstance(instanceID)
		if err != nil {
			return err
		}
		nodeInfo = cachedInstanceInfo{
			autoScalingGroupName: aws.StringValue(groupName),
		}
		agc.nodeInfoCache[node.Name] = nodeInfo
	}
	if nodeInfo.autoScalingGroupName == "" {
		klog.Infof("Skip processing the node %s since it is not attached to an autoscaling group", node.Name)
		return nil
	}
	groupEnabled, groupStatusExists := agc.autoScalingGroupStatus[nodeInfo.autoScalingGroupName]
	if !groupStatusExists {
		return fmt.Errorf("no status found for autoscaling group %s", nodeInfo.autoScalingGroupName)
	}
	if !groupEnabled {
		klog.Infof("Skip processing the node %s since its autoscaling group is not managed by the controller", node.Name)
		return nil
	}
	nodeConditionChecksum := getNodeConditionChecksum(node)
	//if exists && nodeConditionChecksum != nodeInfo.lastNodeConditionChecksum {
	//	klog.Infof("Condition of node %s has not changed", node.Name)
	//	return nil
	//}
	//klog.Infof("Condition of node %s has changed: %v", node.Name, node.Status.Conditions)
	nodeInfo.lastNodeConditionChecksum = nodeConditionChecksum

	isHealthy := isNodeHealthy(node)
	err := agc.cloud.SetHealthOfInstanceInGroup(string(instanceID), isHealthy)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == "ValidationError" && strings.HasPrefix(awsErr.Message(), "Instance Id not found") {
				klog.Infof("Cannot set health to %v of EC2 instance %s for node %s. Instance is not attached to an autoscaling group", isHealthy, instanceID, node.Name)
				// TODO add instance ID to a cache (with TTL) so that the SetInstanceHealth call is skipped
				// when we know the instance is not attached to an ASG.
				// The instance may be attached to an ASG at any time, so we need to retry the call every so often.
				return nil
			}
		}
		klog.Errorf("Error in setting health to %v of EC2 instance %s for node %s, error: %v", isHealthy, instanceID, node.GetName(), err)
		return err
	}
	klog.Infof("Successfully set health to %v of EC2 instance %s for node %s", isHealthy, instanceID, node.Name)
	return nil
}

func isNodeHealthy(node *v1.Node) bool {
	ready := findNodeCondition(node.Status.Conditions, v1.NodeReady)
	if ready == nil {
		// assume unhealthy if no Ready condition exists
		return false
	}
	switch ready.Status {
	case v1.ConditionTrue:
		return true
	default:
		return false
	}
}

func findNodeCondition(conditions []v1.NodeCondition, conditionType v1.NodeConditionType) *v1.NodeCondition {
	for _, condition := range conditions {
		if condition.Type == conditionType {
			return &condition
		}
	}
	return nil
}

// enqueueNode takes in the object and an
// action for the object for a queuedNode and enqueue to the workqueue
func (agc *autoScalingGroupController) enqueueNode(node *v1.Node) {
	item := &queuedNode{
		node:           node,
		requeuingCount: 0,
		enqueueTime:    time.Now(),
	}
	if agc.rateLimitEnabled {
		agc.nodeQueue.AddRateLimited(item)
		//klog.Infof("Added %s to the workqueue (rate-limited)", item)
	} else {
		agc.nodeQueue.Add(item)
		//klog.Infof("Added %s to the workqueue (without any rate-limit)", item)
	}
}

// getNodeConditionChecksum returns a 32-bit checksum of the node's conditions.
// Only the type and status of the conditions are included.
func getNodeConditionChecksum(node *v1.Node) uint32 {
	var conditions []string
	for _, nc := range node.Status.Conditions {
		condition := string(nc.Type) + string(nc.Status)
		conditions = append(conditions, condition)
	}
	sort.Strings(conditions)
	var bytes []byte
	for _, condition := range conditions {
		bytes = append(bytes, []byte(condition)...)
	}
	return adler32.Checksum(bytes)
}

func (agc *autoScalingGroupController) refreshAutoScalingGroups() error {
	klog.Infof("Refreshing autoscaling groups that are managed by the controller...")
	groups, err := agc.cloud.DescribeInstanceGroupsForCluster(agc.clusterName)
	if err != nil {
		return err
	}
	agc.autoScalingGroupStatus = make(map[string]bool)
	enabledCount := 0
	for _, group := range groups {
		enabled := false
		value, exists := group.GetProperty(AutoScalingGroupControllerEnabledTagKey)
		if exists {
			tagValue, err := strconv.ParseBool(value)
			if err != nil {
				klog.Warningf("Autoscaling group %s has an invalid value for %s: '%s'", group.Name(), AutoScalingGroupControllerEnabledTagKey, value)
			} else {
				enabled = tagValue
			}
		}
		agc.autoScalingGroupStatus[group.Name()] = enabled
		if enabled {
			enabledCount++
		}
	}
	klog.Infof("Discovered %d autoscaling groups, %d of which are managed by the controller", len(agc.autoScalingGroupStatus), enabledCount)
	return nil
}

// checkAutoScalingGroups determines the instances that are associated with
// autoscaling groups that have management by the controller enabled, and
// marks the instances as unhealthy if they don't have a corresponding Node
// (i.e. aren't participating in the cluster)
func (agc *autoScalingGroupController) checkAutoScalingGroups() {
	klog.Infof("Checking autoscaling groups...")
	for group, enabled := range agc.autoScalingGroupStatus {
		if !enabled {
			continue
		}
		klog.Infof("Checking autoscaling group %s", group)
		instances, err := agc.cloud.DescribeInstancesForGroup(group)
		if err != nil {
			klog.Errorf("Failed to describe instances for group %s: %v", group, err)
			continue
		}
		for _, instance := range instances {
			nodeName, err := agc.cloud.InstanceIDToNodeName(instance.ID())
			if err != nil {
				if strings.HasPrefix(err.Error(), "node not found for InstanceID") {
					creationTimestamp, err := instance.CreationTimestamp()
					if err != nil {
						klog.Errorf("Failed to get creation timestamp for instance %s: %v", instance.ID(), err)
					} else if time.Since(creationTimestamp.Time) > agc.newNodeGracePeriod {
						agc.cloud.SetHealthOfInstanceInGroup(string(instance.ID()), false)
						klog.Errorf("Set instance %s in group %s as unhealthy since it is not participating in the cluster", instance.ID(), group)
					}
				} else {
					klog.Errorf("Failed to get node name for instance %s in group %s: %v", instance.ID(), group, err)
				}
			} else {
				klog.Infof("Instance %s in group %s is mapped to node %s", instance.ID(), group, nodeName)
			}
		}
		klog.Infof("Finished checking autoscaling group %s", group)
	}
	klog.Infof("Finished checking autoscaling groups!")
}
