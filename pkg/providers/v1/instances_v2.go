/*
Copyright 2024 The Kubernetes Authors.

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

// This file implements the InstancesV2 interface.
// InstancesV2 is an abstract, pluggable interface for cloud provider instances.
// Unlike the Instances interface, it is designed for external cloud providers and should only be used by them.

package aws

import (
	"context"
	"fmt"
	"strconv"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	cloudprovider "k8s.io/cloud-provider"
	"k8s.io/klog/v2"
)

func (c *Cloud) getProviderID(ctx context.Context, node *v1.Node) (string, error) {
	if node.Spec.ProviderID != "" {
		return node.Spec.ProviderID, nil
	}

	instanceID, err := c.InstanceID(ctx, types.NodeName(node.Name))
	if err != nil {
		return "", err
	}

	return c.ProviderName() + "://" + instanceID, nil
}

// InstanceExists returns true if the instance for the given node exists according to the cloud provider.
// Use the node.name or node.spec.providerID field to find the node in the cloud provider.
func (c *Cloud) InstanceExists(ctx context.Context, node *v1.Node) (bool, error) {
	providerID, err := c.getProviderID(ctx, node)
	if err != nil {
		return false, err
	}

	exists, err := c.instanceExistsByProviderID(ctx, providerID)
	if err != nil {
		if IsAWSErrorInstanceNotFound(err) {
			if time.Since(node.CreationTimestamp.Time) < c.nodeEventualConsistencyGracePeriod {
				// recently-launched EC2 instances may not appear in `ec2:DescribeInstances`
				// we return an error if we're within the eventual-consistency grace period
				// e.g. to cause the cloud-node-lifecycle-controller to ignore this node
				return false, fmt.Errorf("node is within eventual-consistency grace period (%v): %v", c.nodeEventualConsistencyGracePeriod, err)
			}
			return false, nil
		}

	}
	return exists, nil
}

// InstanceShutdown returns true if the instance is shutdown according to the cloud provider.
// Use the node.name or node.spec.providerID field to find the node in the cloud provider.
func (c *Cloud) InstanceShutdown(ctx context.Context, node *v1.Node) (bool, error) {
	providerID, err := c.getProviderID(ctx, node)
	if err != nil {
		return false, err
	}

	return c.InstanceShutdownByProviderID(ctx, providerID)
}

func (c *Cloud) getAdditionalLabels(ctx context.Context, zoneName string, instanceID string, instanceType string,
	region string, existingLabels map[string]string) (map[string]string, error) {
	additionalLabels := map[string]string{}

	// If zone ID label is already set, skip.
	if _, ok := existingLabels[LabelZoneID]; !ok {
		// Add the zone ID to the additional labels
		zoneID, err := c.zoneCache.getZoneIDByZoneName(zoneName)
		if err != nil {
			return nil, err
		}

		additionalLabels[LabelZoneID] = zoneID
	}

	// If topology labels are already set, skip.
	if _, ok := existingLabels[LabelNetworkNodePrefix+"1"]; !ok {
		nodeTopology, err := c.instanceTopologyManager.GetNodeTopology(ctx, instanceType, region, instanceID)
		// We've seen some edge cases where this functionality is problematic, so swallowing errors and logging
		// to avoid short-circuiting syncing nodes. If it's an intermittent issue, the labels will be added
		// on subsequent attempts.
		if err != nil {
			klog.Warningf("Failed to get node topology. Moving on without setting labels: %q", err)
		} else if nodeTopology != nil {
			for index, networkNode := range nodeTopology.NetworkNodes {
				layer := index + 1
				label := LabelNetworkNodePrefix + strconv.Itoa(layer)
				additionalLabels[label] = networkNode
			}
		}
	}

	return additionalLabels, nil
}

// InstanceMetadata returns the instance's metadata. The values returned in InstanceMetadata are
// translated into specific fields and labels in the Node object on registration.
// Implementations should always check node.spec.providerID first when trying to discover the instance
// for a given node. In cases where node.spec.providerID is empty, implementations can use other
// properties of the node like its name, labels and annotations.
func (c *Cloud) InstanceMetadata(ctx context.Context, node *v1.Node) (*cloudprovider.InstanceMetadata, error) {
	providerID, err := c.getProviderID(ctx, node)
	if err != nil {
		return nil, err
	}

	instanceType, err := c.InstanceTypeByProviderID(ctx, providerID)
	if err != nil {
		return nil, err
	}

	zone, err := c.GetZoneByProviderID(ctx, providerID)
	if err != nil {
		return nil, err
	}

	nodeAddresses, err := c.NodeAddressesByProviderID(ctx, providerID)
	if err != nil {
		return nil, err
	}

	instanceID, err := KubernetesInstanceID(providerID).MapToAWSInstanceID()
	if err != nil {
		return nil, err
	}

	additionalLabels, err := c.getAdditionalLabels(ctx, zone.FailureDomain, string(instanceID), instanceType, zone.Region, node.Labels)
	if err != nil {
		return nil, err
	}

	return &cloudprovider.InstanceMetadata{
		ProviderID:       providerID,
		InstanceType:     instanceType,
		NodeAddresses:    nodeAddresses,
		Zone:             zone.FailureDomain,
		Region:           zone.Region,
		AdditionalLabels: additionalLabels,
	}, nil
}
