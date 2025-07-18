/*
Copyright 2017 The Kubernetes Authors.

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
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"k8s.io/klog/v2"

	"k8s.io/apimachinery/pkg/util/wait"

	"k8s.io/cloud-provider-aws/pkg/providers/v1/iface"
)

// TagNameKubernetesClusterPrefix is the tag name we use to differentiate multiple
// logically independent clusters running in the same AZ.
// The tag key = TagNameKubernetesClusterPrefix + clusterID
// The tag value is an ownership value
const TagNameKubernetesClusterPrefix = "kubernetes.io/cluster/"

// TagNameKubernetesClusterLegacy is the legacy tag name we use to differentiate multiple
// logically independent clusters running in the same AZ.  The problem with it was that it
// did not allow shared resources.
const TagNameKubernetesClusterLegacy = "KubernetesCluster"

// ResourceLifecycle is the cluster lifecycle state used in tagging
type ResourceLifecycle string

const (
	// ResourceLifecycleOwned is the value we use when tagging resources to indicate
	// that the resource is considered owned and managed by the cluster,
	// and in particular that the lifecycle is tied to the lifecycle of the cluster.
	ResourceLifecycleOwned = "owned"
	// ResourceLifecycleShared is the value we use when tagging resources to indicate
	// that the resource is shared between multiple clusters, and should not be destroyed
	// if the cluster is destroyed.
	ResourceLifecycleShared = "shared"
)

type awsTagging struct {
	// ClusterID is our cluster identifier: we tag AWS resources with this value,
	// and thus we can run two independent clusters in the same VPC or subnets.
	// This gives us similar functionality to GCE projects.
	ClusterID string

	// usesLegacyTags is true if we are using the legacy TagNameKubernetesClusterLegacy tags
	usesLegacyTags bool
}

func (t *awsTagging) init(legacyClusterID string, clusterID string) error {
	if legacyClusterID != "" {
		if clusterID != "" && legacyClusterID != clusterID {
			return fmt.Errorf("clusterID tags did not match: %q vs %q", clusterID, legacyClusterID)
		}
		t.usesLegacyTags = true
		clusterID = legacyClusterID
	}

	t.ClusterID = clusterID

	if clusterID != "" {
		klog.Infof("AWS cloud filtering on ClusterID: %v", clusterID)
	} else {
		return fmt.Errorf("AWS cloud failed to find ClusterID")
	}

	return nil
}

// Extracts a clusterID from the given tags, if one is present
// If no clusterID is found, returns "", nil
// If multiple (different) clusterIDs are found, returns an error
func (t *awsTagging) initFromTags(tags []ec2types.Tag) error {
	legacyClusterID, newClusterID, err := findClusterIDs(tags)
	if err != nil {
		return err
	}

	if legacyClusterID == "" && newClusterID == "" {
		klog.Errorf("Tag %q nor %q not found; Kubernetes may behave unexpectedly.", TagNameKubernetesClusterLegacy, TagNameKubernetesClusterPrefix+"...")
	}

	return t.init(legacyClusterID, newClusterID)
}

// Extracts the legacy & new cluster ids from the given tags, if they are present
// If duplicate tags are found, returns an error
func findClusterIDs(tags []ec2types.Tag) (string, string, error) {
	legacyClusterID := ""
	newClusterID := ""

	for _, tag := range tags {
		tagKey := aws.ToString(tag.Key)
		if strings.HasPrefix(tagKey, TagNameKubernetesClusterPrefix) {
			id := strings.TrimPrefix(tagKey, TagNameKubernetesClusterPrefix)
			if newClusterID != "" {
				return "", "", fmt.Errorf("Found multiple cluster tags with prefix %s (%q and %q)", TagNameKubernetesClusterPrefix, newClusterID, id)
			}
			newClusterID = id
		}

		if tagKey == TagNameKubernetesClusterLegacy {
			id := aws.ToString(tag.Value)
			if legacyClusterID != "" {
				return "", "", fmt.Errorf("Found multiple %s tags (%q and %q)", TagNameKubernetesClusterLegacy, legacyClusterID, id)
			}
			legacyClusterID = id
		}
	}

	return legacyClusterID, newClusterID, nil
}

func (t *awsTagging) clusterTagKey() string {
	return TagNameKubernetesClusterPrefix + t.ClusterID
}

func (t *awsTagging) hasClusterTag(tags []ec2types.Tag) bool {
	// if the clusterID is not configured -- we consider all instances.
	if len(t.ClusterID) == 0 {
		return true
	}
	clusterTagKey := t.clusterTagKey()
	for _, tag := range tags {
		tagKey := aws.ToString(tag.Key)
		// For 1.6, we continue to recognize the legacy tags, for the 1.5 -> 1.6 upgrade
		// Note that we want to continue traversing tag list if we see a legacy tag with value != ClusterID
		if (tagKey == TagNameKubernetesClusterLegacy) && (aws.ToString(tag.Value) == t.ClusterID) {
			return true
		}
		if tagKey == clusterTagKey {
			return true
		}
	}
	return false
}

func (t *awsTagging) hasNoClusterPrefixTag(tags []ec2types.Tag) bool {
	for _, tag := range tags {
		if strings.HasPrefix(aws.ToString(tag.Key), TagNameKubernetesClusterPrefix) {
			return false
		}
	}
	return true
}

// Ensure that a resource has the correct tags
// If it has no tags, we assume that this was a problem caused by an error in between creation and tagging,
// and we add the tags.  If it has a different cluster's tags, that is an error.
func (t *awsTagging) readRepairClusterTags(ctx context.Context, client iface.EC2, resourceID string, lifecycle ResourceLifecycle, additionalTags map[string]string, observedTags []ec2types.Tag) error {
	actualTagMap := make(map[string]string)
	for _, tag := range observedTags {
		actualTagMap[aws.ToString(tag.Key)] = aws.ToString(tag.Value)
	}

	expectedTags := t.buildTags(lifecycle, additionalTags)

	addTags := make(map[string]string)
	for k, expected := range expectedTags {
		actual := actualTagMap[k]
		if actual == expected {
			continue
		}
		if actual == "" {
			klog.Warningf("Resource %q was missing expected cluster tag %q.  Will add (with value %q)", resourceID, k, expected)
			addTags[k] = expected
		} else {
			return fmt.Errorf("resource %q has tag belonging to another cluster: %q=%q (expected %q)", resourceID, k, actual, expected)
		}
	}

	if len(addTags) == 0 {
		return nil
	}

	if err := t.createTags(ctx, client, resourceID, lifecycle, addTags); err != nil {
		return fmt.Errorf("error adding missing tags to resource %q: %q", resourceID, err)
	}

	return nil
}

// createTags calls EC2 CreateTags, but adds retry-on-failure logic
// We retry mainly because if we create an object, we cannot tag it until it is "fully created" (eventual consistency)
// The error code varies though (depending on what we are tagging), so we simply retry on all errors
func (t *awsTagging) createTags(ctx context.Context, client iface.EC2, resourceID string, lifecycle ResourceLifecycle, additionalTags map[string]string) error {
	tags := t.buildTags(lifecycle, additionalTags)

	if tags == nil || len(tags) == 0 {
		return nil
	}

	var awsTags []ec2types.Tag
	for k, v := range tags {
		tag := ec2types.Tag{
			Key:   aws.String(k),
			Value: aws.String(v),
		}
		awsTags = append(awsTags, tag)
	}

	backoff := wait.Backoff{
		Duration: createTagInitialDelay,
		Factor:   createTagFactor,
		Steps:    createTagSteps,
	}
	request := &ec2.CreateTagsInput{}
	request.Resources = []string{resourceID}
	request.Tags = awsTags

	var lastErr error
	err := wait.ExponentialBackoff(backoff, func() (bool, error) {
		_, err := client.CreateTags(ctx, request)
		if err == nil {
			return true, nil
		}

		// We could check that the error is retryable, but the error code changes based on what we are tagging
		// SecurityGroup: InvalidGroup.NotFound
		klog.V(2).Infof("Failed to create tags; will retry.  Error was %q", err)
		lastErr = err
		return false, nil
	})
	if err == wait.ErrWaitTimeout {
		// return real CreateTags error instead of timeout
		err = lastErr
	}
	return err
}

// Add additional filters, to match on our tags
// This lets us run multiple k8s clusters in a single EC2 AZ
func (t *awsTagging) addFilters(filters []*ec2types.Filter) []*ec2types.Filter {
	// if there are no clusterID configured - no filtering by special tag names
	// should be applied to revert to legacy behaviour.
	if len(t.ClusterID) == 0 {
		if len(filters) == 0 {
			// We can't pass a zero-length Filters to AWS (it's an error)
			// So if we end up with no filters; just return nil
			return nil
		}
		return filters
	}

	f := newEc2Filter("tag-key", t.clusterTagKey())
	filters = append(filters, &f)
	return filters
}

// Add additional filters, to match on our tags. This uses the tag for legacy
// 1.5 -> 1.6 clusters and exists for backwards compatibility
//
// This lets us run multiple k8s clusters in a single EC2 AZ
func (t *awsTagging) addLegacyFilters(filters []*ec2types.Filter) []*ec2types.Filter {
	// if there are no clusterID configured - no filtering by special tag names
	// should be applied to revert to legacy behaviour.
	if len(t.ClusterID) == 0 {
		if len(filters) == 0 {
			// We can't pass a zero-length Filters to AWS (it's an error)
			// So if we end up with no filters; just return nil
			return nil
		}
		return filters
	}

	f := newEc2Filter(fmt.Sprintf("tag:%s", TagNameKubernetesClusterLegacy), t.ClusterID)

	// We can't pass a zero-length Filters to AWS (it's an error)
	// So if we end up with no filters; we need to return nil
	filters = append(filters, &f)
	return filters
}

func (t *awsTagging) buildTags(lifecycle ResourceLifecycle, additionalTags map[string]string) map[string]string {
	tags := make(map[string]string)
	for k, v := range additionalTags {
		tags[k] = v
	}

	// no clusterID is a sign of misconfigured cluster, but we can't be tagging the resources with empty
	// strings
	if len(t.ClusterID) == 0 {
		return tags
	}

	// We only create legacy tags if we are using legacy tags, i.e. if we have seen a legacy tag on our instance
	if t.usesLegacyTags {
		tags[TagNameKubernetesClusterLegacy] = t.ClusterID
	}
	tags[t.clusterTagKey()] = string(lifecycle)

	return tags
}

func (t *awsTagging) clusterID() string {
	return t.ClusterID
}

// TagResource calls EC2 and tag the resource associated to resourceID
// with the supplied tags
func (c *Cloud) TagResource(ctx context.Context, resourceID string, tags map[string]string) error {
	request := &ec2.CreateTagsInput{
		Resources: []string{resourceID},
		Tags:      buildAwsTags(tags),
	}

	output, err := c.ec2.CreateTags(ctx, request)

	if err != nil {
		klog.Errorf("Error occurred trying to tag resources, %v", err)
		return err
	}

	klog.Infof("Done calling create-tags to EC2: %v", output)

	return nil
}

// TagResourceBatch calls EC2 and tag the resource associated to resourceID with the supplied tags
// calls are batched based on batcher configuration.
func (c *Cloud) TagResourceBatch(ctx context.Context, resourceID string, tags map[string]string) error {
	request := &ec2.CreateTagsInput{
		Resources: []string{resourceID},
		Tags:      buildAwsTags(tags),
	}

	output, err := c.createTagsBatcher.createTags(ctx, request)

	if err != nil {
		klog.Errorf("Error occurred trying to tag resources, %v", err)
		return err
	}

	klog.Infof("Done calling create-tags to EC2: %v", *output)

	return nil
}

// UntagResource calls EC2 and tag the resource associated to resourceID
// with the supplied tags
func (c *Cloud) UntagResource(ctx context.Context, resourceID string, tags map[string]string) error {
	request := &ec2.DeleteTagsInput{
		Resources: []string{resourceID},
		Tags:      buildAwsTags(tags),
	}

	output, err := c.ec2.DeleteTags(ctx, request)

	if err != nil {
		// An instance not found should not fail the untagging workflow as it
		// would for tagging, since the target state is already reached.
		if IsAWSErrorInstanceNotFound(err) {
			klog.Infof("Couldn't find resource when trying to untag it hence skipping it, %v", err)
			return nil
		}
		klog.Errorf("Error occurred trying to untag resources, %v", err)
		return err
	}

	klog.Infof("Done calling delete-tags to EC2: %v", output)

	return nil
}

// UntagResourceBatch calls EC2 and tag the resource associated to resourceID with the supplied tags
// calls are batched based on batcher configuration.
func (c *Cloud) UntagResourceBatch(ctx context.Context, resourceID string, tags map[string]string) error {
	request := &ec2.DeleteTagsInput{
		Resources: []string{resourceID},
		Tags:      buildAwsTags(tags),
	}

	output, err := c.deleteTagsBatcher.deleteTags(ctx, request)

	if err != nil {
		// An instance not found should not fail the untagging workflow as it
		// would for tagging, since the target state is already reached.
		if IsAWSErrorInstanceNotFound(err) {
			klog.Infof("Couldn't find resource when trying to untag it hence skipping it, %v", err)
			return nil
		}
		klog.Errorf("Error occurred trying to untag resources, %v", err)
		return err
	}

	klog.Infof("Done calling delete-tags to EC2: %v", output)

	return nil
}

func buildAwsTags(tags map[string]string) []ec2types.Tag {
	var awsTags []ec2types.Tag
	for k, v := range tags {
		newTag := ec2types.Tag{
			Key:   aws.String(k),
			Value: aws.String(v),
		}
		awsTags = append(awsTags, newTag)
	}

	return awsTags
}
