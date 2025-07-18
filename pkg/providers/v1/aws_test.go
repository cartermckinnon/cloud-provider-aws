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
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	elb "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancing"
	elbtypes "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancing/types"
	elbv2 "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2"
	elbv2types "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2/types"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"

	"k8s.io/cloud-provider-aws/pkg/providers/v1/config"
)

const TestClusterID = "clusterid.test"
const TestClusterName = "testCluster"

type MockedFakeEC2 struct {
	*FakeEC2Impl
	mock.Mock
}

func (m *MockedFakeEC2) expectDescribeSecurityGroups(clusterID, groupName string) {
	tags := []ec2types.Tag{
		{Key: aws.String(TagNameKubernetesClusterLegacy), Value: aws.String(clusterID)},
		{Key: aws.String(fmt.Sprintf("%s%s", TagNameKubernetesClusterPrefix, clusterID)), Value: aws.String(ResourceLifecycleOwned)},
	}

	m.On("DescribeSecurityGroups", &ec2.DescribeSecurityGroupsInput{Filters: []ec2types.Filter{
		newEc2Filter("group-name", groupName),
		newEc2Filter("vpc-id", ""),
	}}).Return([]ec2types.SecurityGroup{{Tags: tags}})
}

func (m *MockedFakeEC2) expectDescribeSecurityGroupsAll(clusterID string) {
	tags := []ec2types.Tag{
		{Key: aws.String(TagNameKubernetesClusterLegacy), Value: aws.String(clusterID)},
		{Key: aws.String(fmt.Sprintf("%s%s", TagNameKubernetesClusterPrefix, clusterID)), Value: aws.String(ResourceLifecycleOwned)},
	}

	m.On("DescribeSecurityGroups", &ec2.DescribeSecurityGroupsInput{}).Return([]ec2types.SecurityGroup{{
		GroupId: aws.String("sg-123456"),
		Tags:    tags,
	}})
}

func (m *MockedFakeEC2) expectDescribeSecurityGroupsByFilter(clusterID, filterName string, filterValues ...string) {
	tags := []ec2types.Tag{
		{Key: aws.String(TagNameKubernetesClusterLegacy), Value: aws.String(clusterID)},
		{Key: aws.String(fmt.Sprintf("%s%s", TagNameKubernetesClusterPrefix, clusterID)), Value: aws.String(ResourceLifecycleOwned)},
	}

	m.On("DescribeSecurityGroups", &ec2.DescribeSecurityGroupsInput{Filters: []ec2types.Filter{
		newEc2Filter(filterName, filterValues...),
	}}).Return([]ec2types.SecurityGroup{{Tags: tags}})
}

func (m *MockedFakeEC2) DescribeSecurityGroups(ctx context.Context, request *ec2.DescribeSecurityGroupsInput, optFns ...func(*ec2.Options)) ([]ec2types.SecurityGroup, error) {
	args := m.Called(request)
	return args.Get(0).([]ec2types.SecurityGroup), nil
}

func (m *MockedFakeEC2) DescribeInstanceTopology(ctx context.Context, request *ec2.DescribeInstanceTopologyInput, optFns ...func(*ec2.Options)) ([]ec2types.InstanceTopology, error) {
	args := m.Called(ctx, request)
	if args.Get(1) != nil {
		return nil, args.Get(1).(error)
	}
	return args.Get(0).([]ec2types.InstanceTopology), nil
}

type MockedFakeELB struct {
	*FakeELB
	mock.Mock
}

func (m *MockedFakeELB) DescribeLoadBalancers(ctx context.Context, input *elb.DescribeLoadBalancersInput, optFns ...func(*elb.Options)) (*elb.DescribeLoadBalancersOutput, error) {
	args := m.Called(input)
	return args.Get(0).(*elb.DescribeLoadBalancersOutput), nil
}

func (m *MockedFakeELB) expectDescribeLoadBalancers(loadBalancerName string) {
	m.On("DescribeLoadBalancers", &elb.DescribeLoadBalancersInput{LoadBalancerNames: []string{loadBalancerName}}).Return(&elb.DescribeLoadBalancersOutput{
		LoadBalancerDescriptions: []elbtypes.LoadBalancerDescription{
			{
				SecurityGroups: []string{"sg-123456"},
			},
		},
	})
}

func (m *MockedFakeELB) AddTags(ctx context.Context, input *elb.AddTagsInput, optFns ...func(*elb.Options)) (*elb.AddTagsOutput, error) {
	args := m.Called(input)
	return args.Get(0).(*elb.AddTagsOutput), nil
}

func (m *MockedFakeELB) ConfigureHealthCheck(ctx context.Context, input *elb.ConfigureHealthCheckInput, optFns ...func(*elb.Options)) (*elb.ConfigureHealthCheckOutput, error) {
	args := m.Called(input)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*elb.ConfigureHealthCheckOutput), args.Error(1)
}

func (m *MockedFakeELB) expectConfigureHealthCheck(loadBalancerName *string, expectedHC *elbtypes.HealthCheck, returnErr error) {
	expected := &elb.ConfigureHealthCheckInput{HealthCheck: expectedHC, LoadBalancerName: loadBalancerName}
	call := m.On("ConfigureHealthCheck", expected)
	if returnErr != nil {
		call.Return(nil, returnErr)
	} else {
		call.Return(&elb.ConfigureHealthCheckOutput{}, nil)
	}
}

func TestReadAWSCloudConfigNodeIPFamilies(t *testing.T) {
	tests := []struct {
		name string

		reader io.Reader
		aws    Services

		expectError    bool
		nodeIPFamilies []string
	}{
		{
			"Single IP family",
			strings.NewReader("[global]\nNodeIPFamilies = ipv6"), nil,
			false, []string{"ipv6"},
		},
		{
			"Multiple IP families",
			strings.NewReader("[global]\nNodeIPFamilies = ipv6\nNodeIPFamilies = ipv4"), nil,
			false, []string{"ipv6", "ipv4"},
		},
	}

	for _, test := range tests {
		t.Logf("Running test case %s", test.name)
		cfg, err := readAWSCloudConfig(test.reader)

		if test.expectError {
			if err == nil {
				t.Errorf("Should error for case %s (cfg=%v)", test.name, cfg)
			}
		} else {
			if err != nil {
				t.Errorf("Should succeed for case: %s", test.name)
			}
			if !reflect.DeepEqual(cfg.Global.NodeIPFamilies, test.nodeIPFamilies) {
				t.Errorf("Incorrect ip family value (%s vs %v) for case: %s",
					cfg.Global.NodeIPFamilies, test.nodeIPFamilies, test.name)
			}
		}
	}
}

type ServiceDescriptor struct {
	name                         string
	region                       string
	signingRegion, signingMethod string
	signingName                  string
}

func TestValidateOverridesActiveConfig(t *testing.T) {
	tests := []struct {
		name string

		reader io.Reader
		aws    Services

		expectError bool
		active      bool
	}{
		{
			"No overrides",
			strings.NewReader(`
				[global]
				`),
			nil,
			false, false,
		},
		{
			"Missing Service Name",
			strings.NewReader(`
                [global]

                [ServiceOverride "1"]
                 Region=sregion
                 URL=https://s3.foo.bar
                 SigningRegion=sregion
                 SigningMethod = sign
                `),
			nil,
			true, false,
		},
		{
			"Missing Service Region",
			strings.NewReader(`
                [global]

                [ServiceOverride "1"]
                 Service=s3
                 URL=https://s3.foo.bar
                 SigningRegion=sregion
                 SigningMethod = sign
                 `),
			nil,
			true, false,
		},
		{
			"Missing URL",
			strings.NewReader(`
                  [global]

                  [ServiceOverride "1"]
                   Service="s3"
                   Region=sregion
                   SigningRegion=sregion
                   SigningMethod = sign
                  `),
			nil,
			true, false,
		},
		{
			"Missing Signing Region",
			strings.NewReader(`
                [global]

                [ServiceOverride "1"]
                 Service=s3
                 Region=sregion
                 URL=https://s3.foo.bar
                 SigningMethod = sign
                 `),
			nil,
			true, false,
		},
		{
			"Active Overrides",
			strings.NewReader(`
                [Global]

               [ServiceOverride "1"]
                Service = "s3      "
                Region = sregion
                URL = https://s3.foo.bar
                SigningRegion = sregion
                SigningMethod = v4
                `),
			nil,
			false, true,
		},
		{
			"Multiple Overridden Services",
			strings.NewReader(`
                [Global]
                 vpc = vpc-abc1234567

				[ServiceOverride "1"]
                  Service=s3
                  Region=sregion1
                  URL=https://s3.foo.bar
                  SigningRegion=sregion1
                  SigningMethod = v4

				[ServiceOverride "2"]
                  Service=ec2
                  Region=sregion2
                  URL=https://ec2.foo.bar
                  SigningRegion=sregion2
                  SigningMethod = v4`),
			nil,
			false, true,
		},
		{
			"Duplicate Services",
			strings.NewReader(`
                [Global]
                 vpc = vpc-abc1234567

				[ServiceOverride "1"]
                  Service=s3
                  Region=sregion1
                  URL=https://s3.foo.bar
                  SigningRegion=sregion
                  SigningMethod = sign

				[ServiceOverride "2"]
                  Service=s3
                  Region=sregion1
                  URL=https://s3.foo.bar
                  SigningRegion=sregion
                  SigningMethod = sign`),
			nil,
			true, false,
		},
		{
			"Multiple Overridden Services in Multiple regions",
			strings.NewReader(`
                 [global]

				[ServiceOverride "1"]
                 Service=s3
                 Region=region1
                 URL=https://s3.foo.bar
                 SigningRegion=sregion1

				[ServiceOverride "2"]
                 Service=ec2
                 Region=region2
                 URL=https://ec2.foo.bar
                 SigningRegion=sregion
                 SigningMethod = v4
                 `),
			nil,
			false, true,
		},
		{
			"Multiple regions, Same Service",
			strings.NewReader(`
                 [global]

				[ServiceOverride "1"]
                Service=s3
                Region=region1
                URL=https://s3.foo.bar
                SigningRegion=sregion1
                SigningMethod = v3

				[ServiceOverride "2"]
                 Service=s3
                 Region=region2
                 URL=https://s3.foo.bar
                 SigningRegion=sregion1
				 SigningMethod = v4
                 SigningName = "name"
                 `),
			nil,
			false, true,
		},
	}

	for _, test := range tests {
		t.Logf("Running test case %s", test.name)
		cfg, err := readAWSCloudConfig(test.reader)
		if err == nil {
			err = cfg.ValidateOverrides()
		}
		if test.expectError {
			if err == nil {
				t.Errorf("Should error for case %s (cfg=%v)", test.name, cfg)
			}
		} else {
			if err != nil {
				t.Errorf("Should succeed for case: %s, got %v", test.name, err)
			}
		}
	}
}

func TestNewAWSCloud(t *testing.T) {
	tests := []struct {
		name string

		reader      io.Reader
		awsServices Services

		expectError bool
		region      string
	}{
		{
			"Config specifies valid zone",
			strings.NewReader("[global]\nzone = eu-west-1a"), newMockedFakeAWSServices(TestClusterID),
			false, "eu-west-1",
		},
		{
			"Gets zone from metadata when not in config",
			strings.NewReader("[global]\n"),
			newMockedFakeAWSServices(TestClusterID),
			false, "us-west-2",
		},
	}

	for _, test := range tests {
		t.Logf("Running test case %s", test.name)
		cfg, err := readAWSCloudConfig(test.reader)
		var c *Cloud
		if err == nil {
			c, err = newAWSCloud(*cfg, test.awsServices)
		}
		if test.expectError {
			if err == nil {
				t.Errorf("Should error for case %s", test.name)
			}
		} else {
			if err != nil {
				t.Errorf("Should succeed for case: %s, got %v", test.name, err)
			} else if c.region != test.region {
				t.Errorf("Incorrect region value (%s vs %s) for case: %s",
					c.region, test.region, test.name)
			}
		}
	}
}

func mockInstancesResp(selfInstance *ec2types.Instance, instances []*ec2types.Instance) (*Cloud, *FakeAWSServices) {
	awsServices := newMockedFakeAWSServices(TestClusterID)
	awsServices.instances = instances
	awsServices.selfInstance = selfInstance
	awsCloud, err := newAWSCloud(config.CloudConfig{}, awsServices)
	if err != nil {
		panic(err)
	}
	awsCloud.kubeClient = fake.NewSimpleClientset()
	fakeInformerFactory := informers.NewSharedInformerFactory(awsCloud.kubeClient, 0)
	awsCloud.SetInformers(fakeInformerFactory)
	for _, instance := range instances {
		node := &v1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name: *instance.PrivateDnsName,
			},
			Spec: v1.NodeSpec{
				ProviderID: *instance.InstanceId,
			},
		}
		awsCloud.nodeInformer.Informer().GetStore().Add(node)
	}
	awsCloud.nodeInformerHasSynced = informerSynced
	return awsCloud, awsServices
}

func mockZone(region, availabilityZone string) *Cloud {
	awsServices := newMockedFakeAWSServices(TestClusterID).WithAz(availabilityZone).WithRegion(region)
	awsCloud, err := newAWSCloud(config.CloudConfig{}, awsServices)
	if err != nil {
		panic(err)
	}
	awsCloud.kubeClient = fake.NewSimpleClientset()
	return awsCloud
}

func testHasNodeAddress(t *testing.T, addrs []v1.NodeAddress, addressType v1.NodeAddressType, address string) {
	for _, addr := range addrs {
		if addr.Type == addressType && addr.Address == address {
			return
		}
	}
	t.Errorf("Did not find expected address: %s:%s in %v", addressType, address, addrs)
}

func makeMinimalInstance(instanceID string) ec2types.Instance {
	return makeInstance(instanceID, "", "", "", "", nil, false)
}

func makeInstance(instanceID string, privateIP, publicIP, privateDNSName, publicDNSName string, ipv6s []string, setNetInterface bool) ec2types.Instance {
	var tag ec2types.Tag
	tag.Key = aws.String(TagNameKubernetesClusterLegacy)
	tag.Value = aws.String(TestClusterID)
	tags := []ec2types.Tag{tag}

	instance := ec2types.Instance{
		InstanceId:       &instanceID,
		PrivateDnsName:   aws.String(privateDNSName),
		PrivateIpAddress: aws.String(privateIP),
		PublicDnsName:    aws.String(publicDNSName),
		PublicIpAddress:  aws.String(publicIP),
		InstanceType:     ec2types.InstanceTypeC3Large,
		Tags:             tags,
		Placement:        &ec2types.Placement{AvailabilityZone: aws.String("us-west-2a")},
		State: &ec2types.InstanceState{
			Name: ec2types.InstanceStateNameRunning,
		},
	}
	if setNetInterface == true {
		instance.NetworkInterfaces = []ec2types.InstanceNetworkInterface{
			{
				Status: ec2types.NetworkInterfaceStatusInUse,
				PrivateIpAddresses: []ec2types.InstancePrivateIpAddress{
					{
						PrivateIpAddress: aws.String(privateIP),
					},
				},
			},
		}
		if len(ipv6s) > 0 {
			instance.NetworkInterfaces[0].Ipv6Addresses = []ec2types.InstanceIpv6Address{
				{
					Ipv6Address: aws.String(ipv6s[0]),
				},
			}
		}
	}
	return instance
}

func TestNodeAddressesByProviderID(t *testing.T) {
	for _, tc := range []struct {
		Name            string
		InstanceID      string
		PrivateIP       string
		PublicIP        string
		PrivateDNSName  string
		PublicDNSName   string
		Ipv6s           []string
		SetNetInterface bool
		NodeName        string
		Ipv6Only        bool

		ExpectedNumAddresses int
	}{
		{
			Name:                 "ipv4 w/public IP",
			InstanceID:           "i-00000000000000000",
			PrivateIP:            "192.168.0.1",
			PublicIP:             "1.2.3.4",
			PrivateDNSName:       "instance-same.ec2.internal",
			PublicDNSName:        "instance-same.ec2.external",
			SetNetInterface:      true,
			ExpectedNumAddresses: 5,
		},
		{
			Name:                 "ipv4 w/private IP only",
			InstanceID:           "i-00000000000000001",
			PrivateIP:            "192.168.0.2",
			PrivateDNSName:       "instance-same.ec2.internal",
			ExpectedNumAddresses: 2,
		},
		{
			Name:                 "ipv4 w/public IP and no public DNS",
			InstanceID:           "i-00000000000000002",
			PrivateIP:            "192.168.0.1",
			PublicIP:             "1.2.3.4",
			PrivateDNSName:       "instance-other.ec2.internal",
			ExpectedNumAddresses: 3,
		},
		{
			Name:                 "ipv6 only",
			InstanceID:           "i-00000000000000003",
			PrivateIP:            "192.168.0.3",
			PrivateDNSName:       "instance-ipv6.ec2.internal",
			Ipv6s:                []string{"2a05:d014:aa7:911:fc7e:1600:fc4d:ab2", "2a05:d014:aa7:911:9f44:e737:1aa0:6489"},
			SetNetInterface:      true,
			ExpectedNumAddresses: 1,
			NodeName:             "foo",
			Ipv6Only:             true,
		},
	} {
		t.Run(tc.Name, func(t *testing.T) {
			instance := makeInstance(tc.InstanceID, tc.PrivateIP, tc.PublicIP, tc.PrivateDNSName, tc.PublicDNSName, tc.Ipv6s, tc.SetNetInterface)
			aws1, _ := mockInstancesResp(&instance, []*ec2types.Instance{&instance})
			_, err := aws1.NodeAddressesByProviderID(context.TODO(), "i-xxx")
			if err == nil {
				t.Errorf("Should error when no instance found")
			}
			if tc.Ipv6Only {
				aws1.cfg.Global.NodeIPFamilies = []string{"ipv6"}
			}
			if tc.NodeName != "" {
				aws1.selfAWSInstance.nodeName = types.NodeName(tc.NodeName)
			}
			addrs, err := aws1.NodeAddressesByProviderID(context.TODO(), tc.InstanceID)
			if err != nil {
				t.Errorf("Should not error when instance found, %s", err)
			}
			if len(addrs) != tc.ExpectedNumAddresses {
				t.Errorf("Should return exactly %d NodeAddresses, got %d (%v)", tc.ExpectedNumAddresses, len(addrs), addrs)
			}

			if tc.SetNetInterface && !tc.Ipv6Only {
				testHasNodeAddress(t, addrs, v1.NodeInternalIP, tc.PrivateIP)
			}
			if tc.PublicIP != "" {
				testHasNodeAddress(t, addrs, v1.NodeExternalIP, tc.PublicIP)
			}
			if tc.PublicDNSName != "" {
				testHasNodeAddress(t, addrs, v1.NodeExternalDNS, tc.PublicDNSName)
			}
			if tc.PrivateDNSName != "" && !tc.Ipv6Only {
				testHasNodeAddress(t, addrs, v1.NodeInternalDNS, tc.PrivateDNSName)
				testHasNodeAddress(t, addrs, v1.NodeHostName, tc.PrivateDNSName)
			}
			if tc.Ipv6Only {
				testHasNodeAddress(t, addrs, v1.NodeInternalIP, tc.Ipv6s[0])
			}
		})
	}
}

func TestNodeAddresses(t *testing.T) {
	for _, tc := range []struct {
		Name            string
		InstanceID      string
		PrivateIP       string
		PublicIP        string
		PrivateDNSName  string
		PublicDNSName   string
		Ipv6s           []string
		SetNetInterface bool
		NodeName        string
		Ipv6Only        bool

		ExpectedNumAddresses int
	}{
		{
			Name:                 "ipv4 w/public IP",
			InstanceID:           "i-00000000000000000",
			PrivateIP:            "192.168.0.1",
			PublicIP:             "1.2.3.4",
			PrivateDNSName:       "instance-same.ec2.internal",
			PublicDNSName:        "instance-same.ec2.external",
			SetNetInterface:      true,
			NodeName:             "foo",
			ExpectedNumAddresses: 5,
		},
		{
			Name:                 "ipv4 w/private IP only",
			InstanceID:           "i-00000000000000002",
			PrivateIP:            "192.168.0.1",
			PublicIP:             "1.2.3.4",
			PrivateDNSName:       "instance-other.ec2.internal",
			ExpectedNumAddresses: 3,
		},
		{
			Name:                 "ipv6 only",
			InstanceID:           "i-00000000000000003",
			PrivateIP:            "192.168.0.3",
			PrivateDNSName:       "instance-ipv6.ec2.internal",
			PublicDNSName:        "instance-same.ec2.external",
			Ipv6s:                []string{"2a05:d014:aa7:911:fc7e:1600:fc4d:ab2", "2a05:d014:aa7:911:9f44:e737:1aa0:6489"},
			SetNetInterface:      true,
			Ipv6Only:             true,
			NodeName:             "foo",
			ExpectedNumAddresses: 1,
		},
		{
			Name:                 "resource based naming using FQDN",
			InstanceID:           "i-00000000000000004",
			PrivateIP:            "192.168.0.4",
			PublicIP:             "1.2.3.4",
			PrivateDNSName:       "i-00000000000000004.ec2.internal",
			SetNetInterface:      true,
			ExpectedNumAddresses: 4,
		},
		{
			Name:                 "resource based naming using hostname only",
			InstanceID:           "i-00000000000000005",
			PrivateIP:            "192.168.0.5",
			PublicIP:             "1.2.3.4",
			PrivateDNSName:       "i-00000000000000005",
			SetNetInterface:      true,
			ExpectedNumAddresses: 4,
		},
	} {
		t.Run(tc.Name, func(t *testing.T) {
			instance := makeInstance(tc.InstanceID, tc.PrivateIP, tc.PublicIP, tc.PrivateDNSName, tc.PublicDNSName, tc.Ipv6s, tc.SetNetInterface)
			aws1, _ := mockInstancesResp(&instance, []*ec2types.Instance{&instance})
			_, err := aws1.NodeAddresses(context.TODO(), "instance-mismatch.ec2.internal")
			if err == nil {
				t.Errorf("Should error when no instance found")
			}
			if tc.Ipv6Only {
				aws1.cfg.Global.NodeIPFamilies = []string{"ipv6"}
			}
			if tc.NodeName != "" {
				aws1.selfAWSInstance.nodeName = types.NodeName(tc.NodeName)
			}
			addrs, err := aws1.NodeAddresses(context.TODO(), types.NodeName(tc.PrivateDNSName))
			if err != nil {
				t.Errorf("Should not error when instance found, %s", err)
			}
			if len(addrs) != tc.ExpectedNumAddresses {
				t.Errorf("Should return exactly %d NodeAddresses, got %d (%v)", tc.ExpectedNumAddresses, len(addrs), addrs)
			}

			if tc.SetNetInterface && !tc.Ipv6Only {
				testHasNodeAddress(t, addrs, v1.NodeInternalIP, tc.PrivateIP)
			}
			if tc.PublicIP != "" && !tc.Ipv6Only {
				testHasNodeAddress(t, addrs, v1.NodeExternalIP, tc.PublicIP)
			}
			if tc.PublicDNSName != "" && !tc.Ipv6Only {
				testHasNodeAddress(t, addrs, v1.NodeExternalDNS, tc.PublicDNSName)
			}
			if tc.PrivateDNSName != "" && !tc.Ipv6Only {
				testHasNodeAddress(t, addrs, v1.NodeInternalDNS, tc.PrivateDNSName)
				testHasNodeAddress(t, addrs, v1.NodeHostName, tc.PrivateDNSName)
			}
			if tc.Ipv6Only {
				testHasNodeAddress(t, addrs, v1.NodeInternalIP, tc.Ipv6s[0])
			}
		})
	}
}

func TestGetRegion(t *testing.T) {
	aws := mockZone("us-west-2", "us-west-2e")
	zones, ok := aws.Zones()
	if !ok {
		t.Fatalf("Unexpected missing zones impl")
	}
	zone, err := zones.GetZone(context.TODO())
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if zone.Region != "us-west-2" {
		t.Errorf("Unexpected region: %s", zone.Region)
	}
	if zone.FailureDomain != "us-west-2e" {
		t.Errorf("Unexpected FailureDomain: %s", zone.FailureDomain)
	}
}

func TestFindVPCID(t *testing.T) {
	awsServices := newMockedFakeAWSServices(TestClusterID)
	c, err := newAWSCloud(config.CloudConfig{}, awsServices)
	if err != nil {
		t.Errorf("Error building aws cloud: %v", err)
		return
	}
	vpcID, err := c.findVPCID(context.TODO())
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if vpcID != "vpc-mac0" {
		t.Errorf("Unexpected vpcID: %s", vpcID)
	}
}

func constructSubnets(subnetsIn map[int]map[string]string) (subnetsOut []*ec2types.Subnet) {
	for i := range subnetsIn {
		subnetsOut = append(
			subnetsOut,
			constructSubnet(
				subnetsIn[i]["id"],
				subnetsIn[i]["az"],
			),
		)
	}
	return
}

func constructSubnet(id string, az string) *ec2types.Subnet {
	return &ec2types.Subnet{
		SubnetId:         &id,
		AvailabilityZone: &az,
	}
}

func constructRouteTables(routeTablesIn map[string]bool) (routeTablesOut []*ec2types.RouteTable) {
	routeTablesOut = append(routeTablesOut,
		&ec2types.RouteTable{
			Associations: []ec2types.RouteTableAssociation{{Main: aws.Bool(true)}},
			Routes: []ec2types.Route{{
				DestinationCidrBlock: aws.String("0.0.0.0/0"),
				GatewayId:            aws.String("igw-main"),
			}},
		})

	for subnetID := range routeTablesIn {
		routeTablesOut = append(
			routeTablesOut,
			constructRouteTable(
				subnetID,
				routeTablesIn[subnetID],
			),
		)
	}
	return
}

func constructRouteTable(subnetID string, public bool) *ec2types.RouteTable {
	var gatewayID string
	if public {
		gatewayID = "igw-" + subnetID[len(subnetID)-8:8]
	} else {
		gatewayID = "vgw-" + subnetID[len(subnetID)-8:8]
	}
	return &ec2types.RouteTable{
		Associations: []ec2types.RouteTableAssociation{{SubnetId: aws.String(subnetID)}},
		Routes: []ec2types.Route{{
			DestinationCidrBlock: aws.String("0.0.0.0/0"),
			GatewayId:            aws.String(gatewayID),
		}},
	}
}

func Test_findELBSubnets(t *testing.T) {
	awsServices := newMockedFakeAWSServices(TestClusterID)
	c, err := newAWSCloud(config.CloudConfig{}, awsServices)
	if err != nil {
		t.Errorf("Error building aws cloud: %v", err)
		return
	}
	subnetA0000001 := &ec2types.Subnet{
		AvailabilityZone: aws.String("us-west-2a"),
		SubnetId:         aws.String("subnet-a0000001"),
		Tags: []ec2types.Tag{
			{
				Key:   aws.String(TagNameSubnetPublicELB),
				Value: aws.String("1"),
			},
		},
	}
	subnetA0000002 := &ec2types.Subnet{
		AvailabilityZone: aws.String("us-west-2a"),
		SubnetId:         aws.String("subnet-a0000002"),
		Tags: []ec2types.Tag{
			{
				Key:   aws.String(TagNameSubnetPublicELB),
				Value: aws.String("1"),
			},
		},
	}
	subnetA0000003 := &ec2types.Subnet{
		AvailabilityZone: aws.String("us-west-2a"),
		SubnetId:         aws.String("subnet-a0000003"),
		Tags: []ec2types.Tag{
			{
				Key:   aws.String(c.tagging.clusterTagKey()),
				Value: aws.String("owned"),
			},
			{
				Key:   aws.String(TagNameSubnetInternalELB),
				Value: aws.String("1"),
			},
		},
	}
	subnetB0000001 := &ec2types.Subnet{
		AvailabilityZone: aws.String("us-west-2b"),
		SubnetId:         aws.String("subnet-b0000001"),
		Tags: []ec2types.Tag{
			{
				Key:   aws.String(c.tagging.clusterTagKey()),
				Value: aws.String("owned"),
			},
			{
				Key:   aws.String(TagNameSubnetPublicELB),
				Value: aws.String("1"),
			},
		},
	}
	subnetB0000002 := &ec2types.Subnet{
		AvailabilityZone: aws.String("us-west-2b"),
		SubnetId:         aws.String("subnet-b0000002"),
		Tags: []ec2types.Tag{
			{
				Key:   aws.String(c.tagging.clusterTagKey()),
				Value: aws.String("owned"),
			},
			{
				Key:   aws.String(TagNameSubnetInternalELB),
				Value: aws.String("1"),
			},
		},
	}
	subnetC0000001 := &ec2types.Subnet{
		AvailabilityZone: aws.String("us-west-2c"),
		SubnetId:         aws.String("subnet-c0000001"),
		Tags: []ec2types.Tag{
			{
				Key:   aws.String(c.tagging.clusterTagKey()),
				Value: aws.String("owned"),
			},
			{
				Key:   aws.String(TagNameSubnetInternalELB),
				Value: aws.String("1"),
			},
		},
	}
	subnetOther := &ec2types.Subnet{
		AvailabilityZone: aws.String("us-west-2c"),
		SubnetId:         aws.String("subnet-other"),
		Tags: []ec2types.Tag{
			{
				Key:   aws.String(TagNameKubernetesClusterPrefix + "clusterid.other"),
				Value: aws.String("owned"),
			},
			{
				Key:   aws.String(TagNameSubnetInternalELB),
				Value: aws.String("1"),
			},
		},
	}
	subnetNoTag := &ec2types.Subnet{
		AvailabilityZone: aws.String("us-west-2c"),
		SubnetId:         aws.String("subnet-notag"),
	}
	subnetLocalZone := &ec2types.Subnet{
		AvailabilityZone: aws.String("az-local"),
		SubnetId:         aws.String("subnet-in-local-zone"),
		Tags: []ec2types.Tag{
			{
				Key:   aws.String(c.tagging.clusterTagKey()),
				Value: aws.String("owned"),
			},
		},
	}
	subnetWavelengthZone := &ec2types.Subnet{
		AvailabilityZone: aws.String("az-wavelength"),
		SubnetId:         aws.String("subnet-in-wavelength-zone"),
		Tags: []ec2types.Tag{
			{
				Key:   aws.String(c.tagging.clusterTagKey()),
				Value: aws.String("owned"),
			},
		},
	}

	tests := []struct {
		name        string
		subnets     []*ec2types.Subnet
		routeTables map[string]bool
		internal    bool
		want        []string
	}{
		{
			name: "no subnets",
		},
		{
			name: "single tagged subnet",
			subnets: []*ec2types.Subnet{
				subnetA0000001,
			},
			routeTables: map[string]bool{
				"subnet-a0000001": true,
			},
			internal: false,
			want:     []string{"subnet-a0000001"},
		},
		{
			name: "no matching public subnet",
			subnets: []*ec2types.Subnet{
				subnetA0000002,
			},
			routeTables: map[string]bool{
				"subnet-a0000002": false,
			},
			want: nil,
		},
		{
			name: "prefer role over cluster tag",
			subnets: []*ec2types.Subnet{
				subnetA0000001,
				subnetA0000003,
			},
			routeTables: map[string]bool{
				"subnet-a0000001": true,
				"subnet-a0000003": true,
			},
			want: []string{"subnet-a0000001"},
		},
		{
			name: "prefer cluster tag",
			subnets: []*ec2types.Subnet{
				subnetC0000001,
				subnetNoTag,
			},
			want: []string{"subnet-c0000001"},
		},
		{
			name: "include untagged",
			subnets: []*ec2types.Subnet{
				subnetA0000001,
				subnetNoTag,
			},
			routeTables: map[string]bool{
				"subnet-a0000001": true,
				"subnet-notag":    true,
			},
			want: []string{"subnet-a0000001", "subnet-notag"},
		},
		{
			name: "ignore some other cluster owned subnet",
			subnets: []*ec2types.Subnet{
				subnetB0000001,
				subnetOther,
			},
			routeTables: map[string]bool{
				"subnet-b0000001": true,
				"subnet-other":    true,
			},
			want: []string{"subnet-b0000001"},
		},
		{
			name: "prefer matching role",
			subnets: []*ec2types.Subnet{
				subnetB0000001,
				subnetB0000002,
			},
			routeTables: map[string]bool{
				"subnet-b0000001": false,
				"subnet-b0000002": false,
			},
			want:     []string{"subnet-b0000002"},
			internal: true,
		},
		{
			name: "choose lexicographic order",
			subnets: []*ec2types.Subnet{
				subnetA0000001,
				subnetA0000002,
			},
			routeTables: map[string]bool{
				"subnet-a0000001": true,
				"subnet-a0000002": true,
			},
			want: []string{"subnet-a0000001"},
		},
		{
			name: "everything",
			subnets: []*ec2types.Subnet{
				subnetA0000001,
				subnetA0000002,
				subnetB0000001,
				subnetB0000002,
				subnetC0000001,
				subnetNoTag,
				subnetOther,
			},
			routeTables: map[string]bool{
				"subnet-a0000001": true,
				"subnet-a0000002": true,
				"subnet-b0000001": true,
				"subnet-b0000002": true,
				"subnet-c0000001": true,
				"subnet-notag":    true,
				"subnet-other":    true,
			},
			want: []string{"subnet-a0000001", "subnet-b0000001", "subnet-c0000001"},
		},
		{
			name: "exclude subnets from local and wavelenght zones",
			subnets: []*ec2types.Subnet{
				subnetA0000001,
				subnetB0000001,
				subnetC0000001,
				subnetLocalZone,
				subnetWavelengthZone,
			},
			want: []string{"subnet-a0000001", "subnet-b0000001", "subnet-c0000001"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			awsServices.ec2.RemoveSubnets()
			awsServices.ec2.RemoveRouteTables()
			for _, subnet := range tt.subnets {
				awsServices.ec2.CreateSubnet(subnet)
			}
			routeTables := constructRouteTables(tt.routeTables)
			for _, rt := range routeTables {
				awsServices.ec2.CreateRouteTable(rt)
			}
			got, _ := c.findELBSubnets(context.TODO(), tt.internal)
			sort.Strings(tt.want)
			sort.Strings(got)
			assert.Equal(t, tt.want, got)
		})
	}
}

func Test_getLoadBalancerSubnets(t *testing.T) {
	awsServices := newMockedFakeAWSServices(TestClusterID)
	c, err := newAWSCloud(config.CloudConfig{}, awsServices)
	if err != nil {
		t.Errorf("Error building aws cloud: %v", err)
		return
	}
	tests := []struct {
		name        string
		service     *v1.Service
		subnets     []*ec2types.Subnet
		internalELB bool
		want        []string
		wantErr     error
	}{
		{
			name:    "no annotation",
			service: &v1.Service{},
		},
		{
			name: "annotation with no subnets",
			service: &v1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						"service.beta.kubernetes.io/aws-load-balancer-subnets": "\t",
					},
				},
			},
			wantErr: errors.New("unable to resolve empty subnet slice"),
		},
		{
			name: "subnet ids",
			subnets: []*ec2types.Subnet{
				{
					AvailabilityZone: aws.String("us-west-2c"),
					SubnetId:         aws.String("subnet-a000001"),
				},
				{
					AvailabilityZone: aws.String("us-west-2b"),
					SubnetId:         aws.String("subnet-a000002"),
				},
			},
			service: &v1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						"service.beta.kubernetes.io/aws-load-balancer-subnets": "subnet-a000001, subnet-a000002",
					},
				},
			},
			want: []string{"subnet-a000001", "subnet-a000002"},
		},
		{
			name: "subnet names",
			subnets: []*ec2types.Subnet{
				{
					AvailabilityZone: aws.String("us-west-2c"),
					SubnetId:         aws.String("subnet-a000001"),
				},
				{
					AvailabilityZone: aws.String("us-west-2b"),
					SubnetId:         aws.String("subnet-a000002"),
				},
			},
			service: &v1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						"service.beta.kubernetes.io/aws-load-balancer-subnets": "My Subnet 1, My Subnet 2 ",
					},
				},
			},
			want: []string{"subnet-a000001", "subnet-a000002"},
		},
		{
			name: "unable to find all subnets",
			subnets: []*ec2types.Subnet{
				{
					AvailabilityZone: aws.String("us-west-2c"),
					SubnetId:         aws.String("subnet-a000001"),
				},
			},
			service: &v1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						"service.beta.kubernetes.io/aws-load-balancer-subnets": "My Subnet 1, My Subnet 2, Test Subnet ",
					},
				},
			},
			wantErr: errors.New("expected to find 3, but found 1 subnets"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			awsServices.ec2.RemoveSubnets()
			for _, subnet := range tt.subnets {
				awsServices.ec2.CreateSubnet(subnet)
			}
			got, err := c.getLoadBalancerSubnets(context.TODO(), tt.service, tt.internalELB)
			if tt.wantErr != nil {
				assert.EqualError(t, err, tt.wantErr.Error())
			} else {
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestSubnetIDsinVPC(t *testing.T) {
	awsServices := newMockedFakeAWSServices(TestClusterID)
	c, err := newAWSCloud(config.CloudConfig{}, awsServices)
	if err != nil {
		t.Errorf("Error building aws cloud: %v", err)
		return
	}

	// test with 3 subnets from 3 different AZs
	subnets := make(map[int]map[string]string)
	subnets[0] = make(map[string]string)
	subnets[0]["id"] = "subnet-a0000001"
	subnets[0]["az"] = "af-south-1a"
	subnets[1] = make(map[string]string)
	subnets[1]["id"] = "subnet-b0000001"
	subnets[1]["az"] = "af-south-1b"
	subnets[2] = make(map[string]string)
	subnets[2]["id"] = "subnet-c0000001"
	subnets[2]["az"] = "af-south-1c"
	constructedSubnets := constructSubnets(subnets)
	awsServices.ec2.RemoveSubnets()
	for _, subnet := range constructedSubnets {
		awsServices.ec2.CreateSubnet(subnet)
	}

	routeTables := map[string]bool{
		"subnet-a0000001": true,
		"subnet-b0000001": true,
		"subnet-c0000001": true,
	}
	constructedRouteTables := constructRouteTables(routeTables)
	awsServices.ec2.RemoveRouteTables()
	for _, rt := range constructedRouteTables {
		awsServices.ec2.CreateRouteTable(rt)
	}

	result, err := c.findELBSubnets(context.TODO(), false)
	if err != nil {
		t.Errorf("Error listing subnets: %v", err)
		return
	}

	if len(result) != 3 {
		t.Errorf("Expected 3 subnets but got %d", len(result))
		return
	}

	resultSet := make(map[string]bool)
	for _, v := range result {
		resultSet[v] = true
	}

	for i := range subnets {
		if !resultSet[subnets[i]["id"]] {
			t.Errorf("Expected subnet%d '%s' in result: %v", i, subnets[i]["id"], result)
			return
		}
	}

	// test implicit routing table - when subnets are not explicitly linked to a table they should use main
	constructedRouteTables = constructRouteTables(map[string]bool{})
	awsServices.ec2.RemoveRouteTables()
	for _, rt := range constructedRouteTables {
		awsServices.ec2.CreateRouteTable(rt)
	}

	result, err = c.findELBSubnets(context.TODO(), false)
	if err != nil {
		t.Errorf("Error listing subnets: %v", err)
		return
	}

	if len(result) != 3 {
		t.Errorf("Expected 3 subnets but got %d", len(result))
		return
	}

	resultSet = make(map[string]bool)
	for _, v := range result {
		resultSet[v] = true
	}

	for i := range subnets {
		if !resultSet[subnets[i]["id"]] {
			t.Errorf("Expected subnet%d '%s' in result: %v", i, subnets[i]["id"], result)
			return
		}
	}

	// Test with 5 subnets from 3 different AZs.
	// Add 2 duplicate AZ subnets lexicographically chosen one is the middle element in array to
	// check that we both choose the correct entry when it comes after and before another element
	// in the same AZ.
	subnets[3] = make(map[string]string)
	subnets[3]["id"] = "subnet-c0000000"
	subnets[3]["az"] = "af-south-1c"
	subnets[4] = make(map[string]string)
	subnets[4]["id"] = "subnet-c0000002"
	subnets[4]["az"] = "af-south-1c"
	constructedSubnets = constructSubnets(subnets)
	awsServices.ec2.RemoveSubnets()
	for _, subnet := range constructedSubnets {
		awsServices.ec2.CreateSubnet(subnet)
	}
	routeTables["subnet-c0000000"] = true
	routeTables["subnet-c0000002"] = true
	constructedRouteTables = constructRouteTables(routeTables)
	awsServices.ec2.RemoveRouteTables()
	for _, rt := range constructedRouteTables {
		awsServices.ec2.CreateRouteTable(rt)
	}

	result, err = c.findELBSubnets(context.TODO(), false)
	if err != nil {
		t.Errorf("Error listing subnets: %v", err)
		return
	}

	if len(result) != 3 {
		t.Errorf("Expected 3 subnets but got %d", len(result))
		return
	}

	expected := []string{"subnet-a0000001", "subnet-b0000001", "subnet-c0000000"}
	for _, s := range result {
		if !contains(expected, s) {
			t.Errorf("Unexpected subnet '%s' found", s)
			return
		}
	}

	delete(routeTables, "subnet-c0000002")

	// test with 6 subnets from 3 different AZs
	// with 3 private subnets
	subnets[4] = make(map[string]string)
	subnets[4]["id"] = "subnet-d0000001"
	subnets[4]["az"] = "af-south-1a"
	subnets[5] = make(map[string]string)
	subnets[5]["id"] = "subnet-d0000002"
	subnets[5]["az"] = "af-south-1b"

	constructedSubnets = constructSubnets(subnets)
	awsServices.ec2.RemoveSubnets()
	for _, subnet := range constructedSubnets {
		awsServices.ec2.CreateSubnet(subnet)
	}

	routeTables["subnet-a0000001"] = false
	routeTables["subnet-b0000001"] = false
	routeTables["subnet-c0000001"] = false
	routeTables["subnet-c0000000"] = true
	routeTables["subnet-d0000001"] = true
	routeTables["subnet-d0000002"] = true
	constructedRouteTables = constructRouteTables(routeTables)
	awsServices.ec2.RemoveRouteTables()
	for _, rt := range constructedRouteTables {
		awsServices.ec2.CreateRouteTable(rt)
	}
	result, err = c.findELBSubnets(context.TODO(), false)
	if err != nil {
		t.Errorf("Error listing subnets: %v", err)
		return
	}

	if len(result) != 3 {
		t.Errorf("Expected 3 subnets but got %d", len(result))
		return
	}

	expected = []string{"subnet-c0000000", "subnet-d0000001", "subnet-d0000002"}
	for _, s := range result {
		if !contains(expected, s) {
			t.Errorf("Unexpected subnet '%s' found", s)
			return
		}
	}
}

func TestIpPermissionExistsHandlesMultipleGroupIds(t *testing.T) {
	oldIPPermission := ec2types.IpPermission{
		UserIdGroupPairs: []ec2types.UserIdGroupPair{
			{GroupId: aws.String("firstGroupId")},
			{GroupId: aws.String("secondGroupId")},
			{GroupId: aws.String("thirdGroupId")},
		},
	}

	existingIPPermission := ec2types.IpPermission{
		UserIdGroupPairs: []ec2types.UserIdGroupPair{
			{GroupId: aws.String("secondGroupId")},
		},
	}

	newIPPermission := ec2types.IpPermission{
		UserIdGroupPairs: []ec2types.UserIdGroupPair{
			{GroupId: aws.String("fourthGroupId")},
		},
	}

	equals := ipPermissionExists(&existingIPPermission, &oldIPPermission, false)
	if !equals {
		t.Errorf("Should have been considered equal since first is in the second array of groups")
	}

	equals = ipPermissionExists(&newIPPermission, &oldIPPermission, false)
	if equals {
		t.Errorf("Should have not been considered equal since first is not in the second array of groups")
	}

	// The first pair matches, but the second does not
	newIPPermission2 := ec2types.IpPermission{
		UserIdGroupPairs: []ec2types.UserIdGroupPair{
			{GroupId: aws.String("firstGroupId")},
			{GroupId: aws.String("fourthGroupId")},
		},
	}
	equals = ipPermissionExists(&newIPPermission2, &oldIPPermission, false)
	if equals {
		t.Errorf("Should have not been considered equal since first is not in the second array of groups")
	}
}

func TestIpPermissionExistsHandlesRangeSubsets(t *testing.T) {
	// Two existing scenarios we'll test against
	emptyIPPermission := ec2types.IpPermission{}

	oldIPPermission := ec2types.IpPermission{
		IpRanges: []ec2types.IpRange{
			{CidrIp: aws.String("10.0.0.0/8")},
			{CidrIp: aws.String("192.168.1.0/24")},
		},
	}

	// Two already existing ranges and a new one
	existingIPPermission := ec2types.IpPermission{
		IpRanges: []ec2types.IpRange{
			{CidrIp: aws.String("10.0.0.0/8")},
		},
	}
	existingIPPermission2 := ec2types.IpPermission{
		IpRanges: []ec2types.IpRange{
			{CidrIp: aws.String("192.168.1.0/24")},
		},
	}

	newIPPermission := ec2types.IpPermission{
		IpRanges: []ec2types.IpRange{
			{CidrIp: aws.String("172.16.0.0/16")},
		},
	}

	exists := ipPermissionExists(&emptyIPPermission, &emptyIPPermission, false)
	if !exists {
		t.Errorf("Should have been considered existing since we're comparing a range array against itself")
	}
	exists = ipPermissionExists(&oldIPPermission, &oldIPPermission, false)
	if !exists {
		t.Errorf("Should have been considered existing since we're comparing a range array against itself")
	}

	exists = ipPermissionExists(&existingIPPermission, &oldIPPermission, false)
	if !exists {
		t.Errorf("Should have been considered existing since 10.* is in oldIPPermission's array of ranges")
	}
	exists = ipPermissionExists(&existingIPPermission2, &oldIPPermission, false)
	if !exists {
		t.Errorf("Should have been considered existing since 192.* is in oldIpPermission2's array of ranges")
	}

	exists = ipPermissionExists(&newIPPermission, &emptyIPPermission, false)
	if exists {
		t.Errorf("Should have not been considered existing since we compared against a missing array of ranges")
	}
	exists = ipPermissionExists(&newIPPermission, &oldIPPermission, false)
	if exists {
		t.Errorf("Should have not been considered existing since 172.* is not in oldIPPermission's array of ranges")
	}
}

func TestIpPermissionExistsHandlesMultipleGroupIdsWithUserIds(t *testing.T) {
	oldIPPermission := ec2types.IpPermission{
		UserIdGroupPairs: []ec2types.UserIdGroupPair{
			{GroupId: aws.String("firstGroupId"), UserId: aws.String("firstUserId")},
			{GroupId: aws.String("secondGroupId"), UserId: aws.String("secondUserId")},
			{GroupId: aws.String("thirdGroupId"), UserId: aws.String("thirdUserId")},
		},
	}

	existingIPPermission := ec2types.IpPermission{
		UserIdGroupPairs: []ec2types.UserIdGroupPair{
			{GroupId: aws.String("secondGroupId"), UserId: aws.String("secondUserId")},
		},
	}

	newIPPermission := ec2types.IpPermission{
		UserIdGroupPairs: []ec2types.UserIdGroupPair{
			{GroupId: aws.String("secondGroupId"), UserId: aws.String("anotherUserId")},
		},
	}

	equals := ipPermissionExists(&existingIPPermission, &oldIPPermission, true)
	if !equals {
		t.Errorf("Should have been considered equal since first is in the second array of groups")
	}

	equals = ipPermissionExists(&newIPPermission, &oldIPPermission, true)
	if equals {
		t.Errorf("Should have not been considered equal since first is not in the second array of groups")
	}
}

func TestFindInstanceByNodeNameExcludesTerminatedInstances(t *testing.T) {
	awsStates := []struct {
		id       int32
		state    ec2types.InstanceStateName
		expected bool
	}{
		{0, ec2types.InstanceStateNamePending, true},
		{16, ec2types.InstanceStateNameRunning, true},
		{32, ec2types.InstanceStateNameShuttingDown, true},
		{48, ec2types.InstanceStateNameTerminated, false},
		{64, ec2types.InstanceStateNameStopping, true},
		{80, ec2types.InstanceStateNameStopped, true},
	}
	awsServices := newMockedFakeAWSServices(TestClusterID)

	nodeName := types.NodeName("my-dns.internal")

	var tag ec2types.Tag
	tag.Key = aws.String(TagNameKubernetesClusterLegacy)
	tag.Value = aws.String(TestClusterID)
	tags := []ec2types.Tag{tag}

	var testInstance ec2types.Instance
	testInstance.PrivateDnsName = aws.String(string(nodeName))
	testInstance.Tags = tags

	awsDefaultInstances := awsServices.instances
	for _, awsState := range awsStates {
		id := string("i-" + awsState.state)
		testInstance.InstanceId = aws.String(id)
		testInstance.State = &ec2types.InstanceState{Code: aws.Int32(awsState.id), Name: awsState.state}

		awsServices.instances = append(awsDefaultInstances, &testInstance)

		c, err := newAWSCloud(config.CloudConfig{}, awsServices)
		if err != nil {
			t.Errorf("Error building aws cloud: %v", err)
			return
		}

		resultInstance, err := c.findInstanceByNodeName(context.TODO(), nodeName)

		if awsState.expected {
			if err != nil || resultInstance == nil {
				t.Errorf("Expected to find instance %v", *testInstance.InstanceId)
				return
			}
			if *resultInstance.InstanceId != *testInstance.InstanceId {
				t.Errorf("Wrong instance returned by findInstanceByNodeName() expected: %v, actual: %v", *testInstance.InstanceId, *resultInstance.InstanceId)
				return
			}
		} else {
			if err == nil && resultInstance != nil {
				t.Errorf("Did not expect to find instance %v", *resultInstance.InstanceId)
				return
			}
		}
	}
}

func TestGetInstanceByNodeNameBatching(t *testing.T) {
	awsServices := newMockedFakeAWSServices(TestClusterID)
	c, err := newAWSCloud(config.CloudConfig{}, awsServices)
	assert.Nil(t, err, "Error building aws cloud: %v", err)
	var tag ec2types.Tag
	tag.Key = aws.String(TagNameKubernetesClusterPrefix + TestClusterID)
	tag.Value = aws.String("")
	tags := []ec2types.Tag{tag}
	nodeNames := []string{}
	for i := 0; i < 200; i++ {
		nodeName := fmt.Sprintf("ip-171-20-42-%d.ec2.internal", i)
		nodeNames = append(nodeNames, nodeName)
		ec2Instance := &ec2types.Instance{}
		instanceID := fmt.Sprintf("i-abcedf%d", i)
		ec2Instance.InstanceId = aws.String(instanceID)
		ec2Instance.PrivateDnsName = aws.String(nodeName)
		ec2Instance.State = &ec2types.InstanceState{Code: aws.Int32(48), Name: ec2types.InstanceStateNameRunning}
		ec2Instance.Tags = tags
		awsServices.instances = append(awsServices.instances, ec2Instance)
	}

	instances, err := c.getInstancesByNodeNames(context.TODO(), nodeNames)
	assert.Nil(t, err, "Error getting instances by nodeNames %v: %v", nodeNames, err)
	assert.NotEmpty(t, instances)
	assert.Equal(t, 200, len(instances), "Expected 200 but got less")
}

func TestDescribeLoadBalancerOnDelete(t *testing.T) {
	awsServices := newMockedFakeAWSServices(TestClusterID)
	c, _ := newAWSCloud(config.CloudConfig{}, awsServices)
	awsServices.elb.(*MockedFakeELB).expectDescribeLoadBalancers("aid")
	awsServices.ec2.(*MockedFakeEC2).expectDescribeSecurityGroupsByFilter(TestClusterID, "group-id", "sg-123456")
	awsServices.ec2.(*MockedFakeEC2).expectDescribeSecurityGroupsAll(TestClusterID)
	awsServices.ec2.(*MockedFakeEC2).expectDescribeSecurityGroupsByFilter(TestClusterID, "ip-permission.group-id", "sg-123456")

	c.EnsureLoadBalancerDeleted(context.TODO(), TestClusterName, &v1.Service{ObjectMeta: metav1.ObjectMeta{Name: "myservice", UID: "id"}})
}

func TestDescribeLoadBalancerOnUpdate(t *testing.T) {
	awsServices := newMockedFakeAWSServices(TestClusterID)
	c, _ := newAWSCloud(config.CloudConfig{}, awsServices)
	awsServices.elb.(*MockedFakeELB).expectDescribeLoadBalancers("aid")
	awsServices.ec2.(*MockedFakeEC2).expectDescribeSecurityGroupsAll(TestClusterID)
	awsServices.ec2.(*MockedFakeEC2).expectDescribeSecurityGroupsByFilter(TestClusterID, "ip-permission.group-id", "sg-123456")

	c.UpdateLoadBalancer(context.TODO(), TestClusterName, &v1.Service{ObjectMeta: metav1.ObjectMeta{Name: "myservice", UID: "id"}}, []*v1.Node{})
}

func TestDescribeLoadBalancerOnGet(t *testing.T) {
	awsServices := newMockedFakeAWSServices(TestClusterID)
	c, _ := newAWSCloud(config.CloudConfig{}, awsServices)
	awsServices.elb.(*MockedFakeELB).expectDescribeLoadBalancers("aid")

	c.GetLoadBalancer(context.TODO(), TestClusterName, &v1.Service{ObjectMeta: metav1.ObjectMeta{Name: "myservice", UID: "id"}})
}

func TestDescribeLoadBalancerOnEnsure(t *testing.T) {
	awsServices := newMockedFakeAWSServices(TestClusterID)
	c, _ := newAWSCloud(config.CloudConfig{}, awsServices)
	awsServices.elb.(*MockedFakeELB).expectDescribeLoadBalancers("aid")

	c.EnsureLoadBalancer(context.TODO(), TestClusterName, &v1.Service{ObjectMeta: metav1.ObjectMeta{Name: "myservice", UID: "id"}}, []*v1.Node{})
}

func TestCheckMixedProtocol(t *testing.T) {
	tests := []struct {
		name        string
		annotations map[string]string
		ports       []v1.ServicePort
		wantErr     error
	}{
		{
			name:        "TCP",
			annotations: make(map[string]string),
			ports: []v1.ServicePort{
				{
					Protocol: v1.ProtocolTCP,
					Port:     int32(8080),
				},
			},
			wantErr: nil,
		},
		{
			name:        "UDP",
			annotations: map[string]string{ServiceAnnotationLoadBalancerType: "nlb"},
			ports: []v1.ServicePort{
				{
					Protocol: v1.ProtocolUDP,
					Port:     int32(8080),
				},
			},
			wantErr: nil,
		},
		{
			name:        "TCP and UDP",
			annotations: map[string]string{ServiceAnnotationLoadBalancerType: "nlb"},
			ports: []v1.ServicePort{
				{
					Protocol: v1.ProtocolUDP,
					Port:     int32(53),
				},
				{
					Protocol: v1.ProtocolTCP,
					Port:     int32(53),
				},
			},
			wantErr: errors.New("mixed protocol is not supported for LoadBalancer"),
		},
	}
	for _, test := range tests {
		tt := test
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := checkMixedProtocol(tt.ports)
			if tt.wantErr != nil {
				assert.EqualError(t, err, tt.wantErr.Error())
			} else {
				assert.Equal(t, err, nil)
			}
		})
	}
}

func TestCheckProtocol(t *testing.T) {
	tests := []struct {
		name        string
		annotations map[string]string
		port        v1.ServicePort
		wantErr     error
	}{
		{
			name:        "TCP with ELB",
			annotations: make(map[string]string),
			port:        v1.ServicePort{Protocol: v1.ProtocolTCP, Port: int32(8080)},
			wantErr:     nil,
		},
		{
			name:        "TCP with NLB",
			annotations: map[string]string{ServiceAnnotationLoadBalancerType: "nlb"},
			port:        v1.ServicePort{Protocol: v1.ProtocolTCP, Port: int32(8080)},
			wantErr:     nil,
		},
		{
			name:        "UDP with ELB",
			annotations: make(map[string]string),
			port:        v1.ServicePort{Protocol: v1.ProtocolUDP, Port: int32(8080)},
			wantErr:     fmt.Errorf("Protocol UDP not supported by load balancer"),
		},
		{
			name:        "UDP with NLB",
			annotations: map[string]string{ServiceAnnotationLoadBalancerType: "nlb"},
			port:        v1.ServicePort{Protocol: v1.ProtocolUDP, Port: int32(8080)},
			wantErr:     nil,
		},
	}
	for _, test := range tests {
		tt := test
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := checkProtocol(tt.port, tt.annotations)
			if tt.wantErr != nil && err == nil {
				t.Errorf("Expected error: want=%s got =%s", tt.wantErr, err)
			}
			if tt.wantErr == nil && err != nil {
				t.Errorf("Unexpected error: want=%s got =%s", tt.wantErr, err)
			}
		})
	}
}

func TestBuildListener(t *testing.T) {
	tests := []struct {
		name string

		lbPort                    int32
		portName                  string
		instancePort              int32
		backendProtocolAnnotation string
		certAnnotation            string
		sslPortAnnotation         string

		expectError      bool
		lbProtocol       string
		instanceProtocol string
		certID           string
	}{
		{
			"No cert or BE protocol annotation, passthrough",
			80, "", 7999, "", "", "",
			false, "tcp", "tcp", "",
		},
		{
			"Cert annotation without BE protocol specified, SSL->TCP",
			80, "", 8000, "", "cert", "",
			false, "ssl", "tcp", "cert",
		},
		{
			"BE protocol without cert annotation, passthrough",
			443, "", 8001, "https", "", "",
			false, "tcp", "tcp", "",
		},
		{
			"Invalid cert annotation, bogus backend protocol",
			443, "", 8002, "bacon", "foo", "",
			true, "tcp", "tcp", "",
		},
		{
			"Invalid cert annotation, protocol followed by equal sign",
			443, "", 8003, "http=", "=", "",
			true, "tcp", "tcp", "",
		},
		{
			"HTTPS->HTTPS",
			443, "", 8004, "https", "cert", "",
			false, "https", "https", "cert",
		},
		{
			"HTTPS->HTTP",
			443, "", 8005, "http", "cert", "",
			false, "https", "http", "cert",
		},
		{
			"SSL->SSL",
			443, "", 8006, "ssl", "cert", "",
			false, "ssl", "ssl", "cert",
		},
		{
			"SSL->TCP",
			443, "", 8007, "tcp", "cert", "",
			false, "ssl", "tcp", "cert",
		},
		{
			"Port in whitelist",
			1234, "", 8008, "tcp", "cert", "1234,5678",
			false, "ssl", "tcp", "cert",
		},
		{
			"Port not in whitelist, passthrough",
			443, "", 8009, "tcp", "cert", "1234,5678",
			false, "tcp", "tcp", "",
		},
		{
			"Named port in whitelist",
			1234, "bar", 8010, "tcp", "cert", "foo,bar",
			false, "ssl", "tcp", "cert",
		},
		{
			"Named port not in whitelist, passthrough",
			443, "", 8011, "tcp", "cert", "foo,bar",
			false, "tcp", "tcp", "",
		},
		{
			"HTTP->HTTP",
			80, "", 8012, "http", "", "",
			false, "http", "http", "",
		},
	}

	for _, test := range tests {
		t.Logf("Running test case %s", test.name)
		annotations := make(map[string]string)
		if test.backendProtocolAnnotation != "" {
			annotations[ServiceAnnotationLoadBalancerBEProtocol] = test.backendProtocolAnnotation
		}
		if test.certAnnotation != "" {
			annotations[ServiceAnnotationLoadBalancerCertificate] = test.certAnnotation
		}
		ports := getPortSets(test.sslPortAnnotation)
		l, err := buildListener(v1.ServicePort{
			NodePort: int32(test.instancePort),
			Port:     int32(test.lbPort),
			Name:     test.portName,
			Protocol: v1.Protocol("tcp"),
		}, annotations, ports)
		if test.expectError {
			if err == nil {
				t.Errorf("Should error for case %s", test.name)
			}
		} else {
			if err != nil {
				t.Errorf("Should succeed for case: %s, got %v", test.name, err)
			} else {
				var cert *string
				if test.certID != "" {
					cert = &test.certID
				}
				expected := elbtypes.Listener{
					InstancePort:     &test.instancePort,
					InstanceProtocol: &test.instanceProtocol,
					LoadBalancerPort: test.lbPort,
					Protocol:         &test.lbProtocol,
					SSLCertificateId: cert,
				}
				if !reflect.DeepEqual(l, expected) {
					t.Errorf("Incorrect listener (%v vs expected %v) for case: %s",
						l, expected, test.name)
				}
			}
		}
	}
}

func TestProxyProtocolEnabled(t *testing.T) {
	policies := []string{ProxyProtocolPolicyName, "FooBarFoo"}
	fakeBackend := elbtypes.BackendServerDescription{
		InstancePort: aws.Int32(80),
		PolicyNames:  policies,
	}
	result := proxyProtocolEnabled(fakeBackend)
	assert.True(t, result, "expected to find %s in %s", ProxyProtocolPolicyName, policies)

	policies = []string{"FooBarFoo"}
	fakeBackend = elbtypes.BackendServerDescription{
		InstancePort: aws.Int32(80),
		PolicyNames:  []string{"FooBarFoo"},
	}
	result = proxyProtocolEnabled(fakeBackend)
	assert.False(t, result, "did not expect to find %s in %s", ProxyProtocolPolicyName, policies)

	policies = []string{}
	fakeBackend = elbtypes.BackendServerDescription{
		InstancePort: aws.Int32(80),
	}
	result = proxyProtocolEnabled(fakeBackend)
	assert.False(t, result, "did not expect to find %s in %s", ProxyProtocolPolicyName, policies)
}

func TestGetKeyValuePropertiesFromAnnotation(t *testing.T) {
	tagTests := []struct {
		Annotations map[string]string
		Tags        map[string]string
	}{
		{
			Annotations: map[string]string{
				ServiceAnnotationLoadBalancerAdditionalTags: "Key=Val",
			},
			Tags: map[string]string{
				"Key": "Val",
			},
		},
		{
			Annotations: map[string]string{
				ServiceAnnotationLoadBalancerAdditionalTags: "Key1=Val1, Key2=Val2",
			},
			Tags: map[string]string{
				"Key1": "Val1",
				"Key2": "Val2",
			},
		},
		{
			Annotations: map[string]string{
				ServiceAnnotationLoadBalancerAdditionalTags: "Key1=, Key2=Val2",
				"anotherKey": "anotherValue",
			},
			Tags: map[string]string{
				"Key1": "",
				"Key2": "Val2",
			},
		},
		{
			Annotations: map[string]string{
				"Nothing": "Key1=, Key2=Val2, Key3",
			},
			Tags: map[string]string{},
		},
		{
			Annotations: map[string]string{
				ServiceAnnotationLoadBalancerAdditionalTags: "K=V K1=V2,Key1========, =====, ======Val, =Val, , 234,",
			},
			Tags: map[string]string{
				"K":    "V K1",
				"Key1": "",
				"234":  "",
			},
		},
	}

	for _, tagTest := range tagTests {
		result := getKeyValuePropertiesFromAnnotation(tagTest.Annotations, ServiceAnnotationLoadBalancerAdditionalTags)
		for k, v := range result {
			if len(result) != len(tagTest.Tags) {
				t.Errorf("incorrect expected length: %v != %v", result, tagTest.Tags)
				continue
			}
			if tagTest.Tags[k] != v {
				t.Errorf("%s != %s", tagTest.Tags[k], v)
				continue
			}
		}
	}
}

func TestLBExtraSecurityGroupsAnnotation(t *testing.T) {
	awsServices := newMockedFakeAWSServices(TestClusterID)
	c, _ := newAWSCloud(config.CloudConfig{}, awsServices)

	sg1 := map[string]string{ServiceAnnotationLoadBalancerExtraSecurityGroups: "sg-000001"}
	sg2 := map[string]string{ServiceAnnotationLoadBalancerExtraSecurityGroups: "sg-000002"}
	sg3 := map[string]string{ServiceAnnotationLoadBalancerExtraSecurityGroups: "sg-000001, sg-000002"}

	tests := []struct {
		name string

		annotations map[string]string
		expectedSGs []string
	}{
		{"No extra SG annotation", map[string]string{}, []string{}},
		{"Empty extra SGs specified", map[string]string{ServiceAnnotationLoadBalancerExtraSecurityGroups: ", ,,"}, []string{}},
		{"SG specified", sg1, []string{sg1[ServiceAnnotationLoadBalancerExtraSecurityGroups]}},
		{"Multiple SGs specified", sg3, []string{sg1[ServiceAnnotationLoadBalancerExtraSecurityGroups], sg2[ServiceAnnotationLoadBalancerExtraSecurityGroups]}},
	}

	awsServices.ec2.(*MockedFakeEC2).expectDescribeSecurityGroups(TestClusterID, "k8s-elb-aid")

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			serviceName := types.NamespacedName{Namespace: "default", Name: "myservice"}

			sgList, setupSg, err := c.buildELBSecurityGroupList(context.TODO(), serviceName, "aid", test.annotations)
			assert.NoError(t, err, "buildELBSecurityGroupList failed")
			extraSGs := sgList[1:]
			assert.True(t, sets.NewString(test.expectedSGs...).Equal(sets.NewString(extraSGs...)),
				"Security Groups expected=%q , returned=%q", test.expectedSGs, extraSGs)
			assert.True(t, setupSg, "Security Groups Setup Permissions Flag expected=%t , returned=%t", true, setupSg)
		})
	}
}

func TestLBSecurityGroupsAnnotation(t *testing.T) {
	awsServices := newMockedFakeAWSServices(TestClusterID)
	c, _ := newAWSCloud(config.CloudConfig{}, awsServices)

	sg1 := map[string]string{ServiceAnnotationLoadBalancerSecurityGroups: "sg-000001"}
	sg2 := map[string]string{ServiceAnnotationLoadBalancerSecurityGroups: "sg-000002"}
	sg3 := map[string]string{ServiceAnnotationLoadBalancerSecurityGroups: "sg-000001, sg-000002"}

	tests := []struct {
		name string

		annotations map[string]string
		expectedSGs []string
	}{
		{"SG specified", sg1, []string{sg1[ServiceAnnotationLoadBalancerSecurityGroups]}},
		{"Multiple SGs specified", sg3, []string{sg1[ServiceAnnotationLoadBalancerSecurityGroups], sg2[ServiceAnnotationLoadBalancerSecurityGroups]}},
	}

	awsServices.ec2.(*MockedFakeEC2).expectDescribeSecurityGroups(TestClusterID, "k8s-elb-aid")

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			serviceName := types.NamespacedName{Namespace: "default", Name: "myservice"}

			sgList, setupSg, err := c.buildELBSecurityGroupList(context.TODO(), serviceName, "aid", test.annotations)
			assert.NoError(t, err, "buildELBSecurityGroupList failed")
			assert.True(t, sets.NewString(test.expectedSGs...).Equal(sets.NewString(sgList...)),
				"Security Groups expected=%q , returned=%q", test.expectedSGs, sgList)
			assert.False(t, setupSg, "Security Groups Setup Permissions Flag expected=%t , returned=%t", false, setupSg)
		})
	}
}

// Test that we can add a load balancer tag
func TestAddLoadBalancerTags(t *testing.T) {
	loadBalancerName := "test-elb"
	awsServices := newMockedFakeAWSServices(TestClusterID)
	c, _ := newAWSCloud(config.CloudConfig{}, awsServices)

	want := make(map[string]string)
	want["tag1"] = "val1"

	expectedAddTagsRequest := &elb.AddTagsInput{
		LoadBalancerNames: []string{loadBalancerName},
		Tags: []elbtypes.Tag{
			{
				Key:   aws.String("tag1"),
				Value: aws.String("val1"),
			},
		},
	}
	awsServices.elb.(*MockedFakeELB).On("AddTags", expectedAddTagsRequest).Return(&elb.AddTagsOutput{})

	err := c.addLoadBalancerTags(context.TODO(), loadBalancerName, want)
	assert.Nil(t, err, "Error adding load balancer tags: %v", err)
	awsServices.elb.(*MockedFakeELB).AssertExpectations(t)
}

func TestEnsureLoadBalancerHealthCheck(t *testing.T) {
	tests := []struct {
		name        string
		annotations map[string]string
		want        elbtypes.HealthCheck
	}{
		{
			name:        "falls back to HC defaults",
			annotations: map[string]string{},
			want: elbtypes.HealthCheck{
				HealthyThreshold:   aws.Int32(2),
				UnhealthyThreshold: aws.Int32(6),
				Timeout:            aws.Int32(5),
				Interval:           aws.Int32(10),
				Target:             aws.String("TCP:8080"),
			},
		},
		{
			name:        "healthy threshold override",
			annotations: map[string]string{ServiceAnnotationLoadBalancerHCHealthyThreshold: "7"},
			want: elbtypes.HealthCheck{
				HealthyThreshold:   aws.Int32(7),
				UnhealthyThreshold: aws.Int32(6),
				Timeout:            aws.Int32(5),
				Interval:           aws.Int32(10),
				Target:             aws.String("TCP:8080"),
			},
		},
		{
			name:        "unhealthy threshold override",
			annotations: map[string]string{ServiceAnnotationLoadBalancerHCUnhealthyThreshold: "7"},
			want: elbtypes.HealthCheck{
				HealthyThreshold:   aws.Int32(2),
				UnhealthyThreshold: aws.Int32(7),
				Timeout:            aws.Int32(5),
				Interval:           aws.Int32(10),
				Target:             aws.String("TCP:8080"),
			},
		},
		{
			name:        "timeout override",
			annotations: map[string]string{ServiceAnnotationLoadBalancerHCTimeout: "7"},
			want: elbtypes.HealthCheck{
				HealthyThreshold:   aws.Int32(2),
				UnhealthyThreshold: aws.Int32(6),
				Timeout:            aws.Int32(7),
				Interval:           aws.Int32(10),
				Target:             aws.String("TCP:8080"),
			},
		},
		{
			name:        "interval override",
			annotations: map[string]string{ServiceAnnotationLoadBalancerHCInterval: "7"},
			want: elbtypes.HealthCheck{
				HealthyThreshold:   aws.Int32(2),
				UnhealthyThreshold: aws.Int32(6),
				Timeout:            aws.Int32(5),
				Interval:           aws.Int32(7),
				Target:             aws.String("TCP:8080"),
			},
		},
		{
			name: "healthcheck port override",
			annotations: map[string]string{
				ServiceAnnotationLoadBalancerHealthCheckPort: "2122",
			},
			want: elbtypes.HealthCheck{
				HealthyThreshold:   aws.Int32(2),
				UnhealthyThreshold: aws.Int32(6),
				Timeout:            aws.Int32(5),
				Interval:           aws.Int32(10),
				Target:             aws.String("TCP:2122"),
			},
		},
		{
			name: "healthcheck protocol override",
			annotations: map[string]string{
				ServiceAnnotationLoadBalancerHealthCheckProtocol: "HTTP",
			},
			want: elbtypes.HealthCheck{
				HealthyThreshold:   aws.Int32(2),
				UnhealthyThreshold: aws.Int32(6),
				Timeout:            aws.Int32(5),
				Interval:           aws.Int32(10),
				Target:             aws.String("HTTP:8080/"),
			},
		},
		{
			name: "healthcheck protocol, port, path override",
			annotations: map[string]string{
				ServiceAnnotationLoadBalancerHealthCheckProtocol: "HTTPS",
				ServiceAnnotationLoadBalancerHealthCheckPath:     "/healthz",
				ServiceAnnotationLoadBalancerHealthCheckPort:     "31224",
			},
			want: elbtypes.HealthCheck{
				HealthyThreshold:   aws.Int32(2),
				UnhealthyThreshold: aws.Int32(6),
				Timeout:            aws.Int32(5),
				Interval:           aws.Int32(10),
				Target:             aws.String("HTTPS:31224/healthz"),
			},
		},
		{
			name: "healthcheck protocol SSL",
			annotations: map[string]string{
				ServiceAnnotationLoadBalancerHealthCheckProtocol: "SSL",
				ServiceAnnotationLoadBalancerHealthCheckPath:     "/healthz",
				ServiceAnnotationLoadBalancerHealthCheckPort:     "3124",
			},
			want: elbtypes.HealthCheck{
				HealthyThreshold:   aws.Int32(2),
				UnhealthyThreshold: aws.Int32(6),
				Timeout:            aws.Int32(5),
				Interval:           aws.Int32(10),
				Target:             aws.String("SSL:3124"),
			},
		},
		{
			name: "healthcheck port annotation traffic-port",
			annotations: map[string]string{
				ServiceAnnotationLoadBalancerHealthCheckProtocol: "TCP",
				ServiceAnnotationLoadBalancerHealthCheckPort:     "traffic-port",
			},
			want: elbtypes.HealthCheck{
				HealthyThreshold:   aws.Int32(2),
				UnhealthyThreshold: aws.Int32(6),
				Timeout:            aws.Int32(5),
				Interval:           aws.Int32(10),
				Target:             aws.String("TCP:8080"),
			},
		},
	}
	lbName := "myLB"
	// this HC will always differ from the expected HC and thus it is expected an
	// API call will be made to update it
	currentHC := &elbtypes.HealthCheck{}
	elbDesc := &elbtypes.LoadBalancerDescription{LoadBalancerName: &lbName, HealthCheck: currentHC}
	defaultHealthyThreshold := int32(2)
	defaultUnhealthyThreshold := int32(6)
	defaultTimeout := int32(5)
	defaultInterval := int32(10)
	protocol, path, port := "TCP", "", int32(8080)
	target := "TCP:8080"
	defaultHC := &elbtypes.HealthCheck{
		HealthyThreshold:   &defaultHealthyThreshold,
		UnhealthyThreshold: &defaultUnhealthyThreshold,
		Timeout:            &defaultTimeout,
		Interval:           &defaultInterval,
		Target:             &target,
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			awsServices := newMockedFakeAWSServices(TestClusterID)
			c, err := newAWSCloud(config.CloudConfig{}, awsServices)
			assert.Nil(t, err, "Error building aws cloud: %v", err)
			expectedHC := test.want
			awsServices.elb.(*MockedFakeELB).expectConfigureHealthCheck(&lbName, &expectedHC, nil)

			err = c.ensureLoadBalancerHealthCheck(context.TODO(), elbDesc, protocol, port, path, test.annotations)

			require.NoError(t, err)
			awsServices.elb.(*MockedFakeELB).AssertExpectations(t)
		})
	}

	t.Run("does not make an API call if the current health check is the same", func(t *testing.T) {
		awsServices := newMockedFakeAWSServices(TestClusterID)
		c, err := newAWSCloud(config.CloudConfig{}, awsServices)
		assert.Nil(t, err, "Error building aws cloud: %v", err)
		expectedHC := *defaultHC
		timeout := int32(3)
		expectedHC.Timeout = aws.Int32(timeout)
		annotations := map[string]string{ServiceAnnotationLoadBalancerHCTimeout: "3"}
		var currentHC elbtypes.HealthCheck
		currentHC = expectedHC

		// NOTE no call expectations are set on the ELB mock
		// test default HC
		elbDesc := &elbtypes.LoadBalancerDescription{LoadBalancerName: &lbName, HealthCheck: defaultHC}
		err = c.ensureLoadBalancerHealthCheck(context.TODO(), elbDesc, protocol, port, path, map[string]string{})
		assert.NoError(t, err)
		// test HC with override
		elbDesc = &elbtypes.LoadBalancerDescription{LoadBalancerName: &lbName, HealthCheck: &currentHC}
		err = c.ensureLoadBalancerHealthCheck(context.TODO(), elbDesc, protocol, port, path, annotations)
		assert.NoError(t, err)
	})

	t.Run("validates resulting expected health check before making an API call", func(t *testing.T) {
		awsServices := newMockedFakeAWSServices(TestClusterID)
		c, err := newAWSCloud(config.CloudConfig{}, awsServices)
		assert.Nil(t, err, "Error building aws cloud: %v", err)
		expectedHC := *defaultHC
		invalidThreshold := int32(1)
		expectedHC.HealthyThreshold = aws.Int32(invalidThreshold)
		require.Error(t, ValidateHealthCheck(&expectedHC)) // confirm test precondition
		annotations := map[string]string{ServiceAnnotationLoadBalancerHCTimeout: "1"}

		// NOTE no call expectations are set on the ELB mock
		err = c.ensureLoadBalancerHealthCheck(context.TODO(), elbDesc, protocol, port, path, annotations)

		require.Error(t, err)
	})

	t.Run("handles invalid override values", func(t *testing.T) {
		awsServices := newMockedFakeAWSServices(TestClusterID)
		c, err := newAWSCloud(config.CloudConfig{}, awsServices)
		assert.Nil(t, err, "Error building aws cloud: %v", err)
		annotations := map[string]string{ServiceAnnotationLoadBalancerHCTimeout: "3.3"}

		// NOTE no call expectations are set on the ELB mock
		err = c.ensureLoadBalancerHealthCheck(context.TODO(), elbDesc, protocol, port, path, annotations)

		require.Error(t, err)
	})

	t.Run("returns error when updating the health check fails", func(t *testing.T) {
		awsServices := newMockedFakeAWSServices(TestClusterID)
		c, err := newAWSCloud(config.CloudConfig{}, awsServices)
		assert.Nil(t, err, "Error building aws cloud: %v", err)
		returnErr := fmt.Errorf("throttling error")
		awsServices.elb.(*MockedFakeELB).expectConfigureHealthCheck(&lbName, defaultHC, returnErr)

		err = c.ensureLoadBalancerHealthCheck(context.TODO(), elbDesc, protocol, port, path, map[string]string{})

		require.Error(t, err)
		awsServices.elb.(*MockedFakeELB).AssertExpectations(t)
	})
}

func TestFindSecurityGroupForInstance(t *testing.T) {
	groups := map[string]*ec2types.SecurityGroup{"sg123": {GroupId: aws.String("sg123")}}
	id, err := findSecurityGroupForInstance(&ec2types.Instance{SecurityGroups: []ec2types.GroupIdentifier{{GroupId: aws.String("sg123"), GroupName: aws.String("my_group")}}}, groups)
	if err != nil {
		t.Error()
	}
	assert.Equal(t, *id.GroupId, "sg123")
	assert.Equal(t, *id.GroupName, "my_group")
}

func TestFindSecurityGroupForInstanceMultipleTagged(t *testing.T) {
	groups := map[string]*ec2types.SecurityGroup{"sg123": {GroupId: aws.String("sg123")}}
	_, err := findSecurityGroupForInstance(&ec2types.Instance{
		SecurityGroups: []ec2types.GroupIdentifier{
			{GroupId: aws.String("sg123"), GroupName: aws.String("my_group")},
			{GroupId: aws.String("sg123"), GroupName: aws.String("another_group")},
		},
	}, groups)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "sg123(my_group)")
	assert.Contains(t, err.Error(), "sg123(another_group)")
}

const (
	testNodeName           = types.NodeName("ip-10-0-0-1.ec2.internal")
	testInstanceIDNodeName = types.NodeName("i-02bce90670bb0c7cd")
	testOverriddenNodeName = types.NodeName("foo")
	testProviderID         = "aws:///us-west-2c/i-02bce90670bb0c7cd"
	testInstanceID         = "i-02bce90670bb0c7cd"
)

func TestNodeNameToInstanceID(t *testing.T) {
	fakeAWS := newMockedFakeAWSServices(TestClusterID)
	c, err := newAWSCloud(config.CloudConfig{}, fakeAWS)
	assert.NoError(t, err)

	fakeClient := &fake.Clientset{}
	fakeInformerFactory := informers.NewSharedInformerFactory(fakeClient, 0)
	c.SetInformers(fakeInformerFactory)

	// no node name
	_, err = c.nodeNameToInstanceID("")
	assert.Error(t, err)

	// informer has not synced
	c.nodeInformerHasSynced = informerNotSynced
	_, err = c.nodeNameToInstanceID(testNodeName)
	assert.Error(t, err)

	// informer has synced but node not found
	c.nodeInformerHasSynced = informerSynced
	_, err = c.nodeNameToInstanceID(testNodeName)
	assert.Error(t, err)

	// we are able to find the node in cache
	err = c.nodeInformer.Informer().GetStore().Add(&v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: string(testNodeName),
		},
		Spec: v1.NodeSpec{
			ProviderID: testProviderID,
		},
	})
	assert.NoError(t, err)
	_, err = c.nodeNameToInstanceID(testNodeName)
	assert.NoError(t, err)
}

func TestInstanceIDToNodeName(t *testing.T) {
	testCases := []struct {
		name             string
		instanceID       InstanceID
		node             *v1.Node
		expectedNodeName types.NodeName
		expectedErr      error
	}{
		{
			name:       "success: node with private DNS name",
			instanceID: testInstanceID,
			node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: string(testNodeName),
				},
				Spec: v1.NodeSpec{
					ProviderID: testProviderID,
				},
			},
			expectedNodeName: testNodeName,
			expectedErr:      nil,
		},
		{
			name:       "success: node with instance ID name",
			instanceID: testInstanceID,
			node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: string(testInstanceIDNodeName),
				},
				Spec: v1.NodeSpec{
					ProviderID: testProviderID,
				},
			},
			expectedNodeName: testInstanceIDNodeName,
			expectedErr:      nil,
		},
		{
			name:       "success: node with overridden name",
			instanceID: testInstanceID,
			node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: string(testOverriddenNodeName),
				},
				Spec: v1.NodeSpec{
					ProviderID: testProviderID,
				},
			},
			expectedNodeName: testOverriddenNodeName,
			expectedErr:      nil,
		},
		{
			name:       "fail: no node with matching instance ID",
			instanceID: testInstanceID,
			node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: string(testOverriddenNodeName),
				},
				Spec: v1.NodeSpec{
					ProviderID: "aws:///us-west-2c/i-foo",
				},
			},
			expectedNodeName: types.NodeName(""),
			expectedErr:      fmt.Errorf("node with instanceID \"i-02bce90670bb0c7cd\" not found"),
		},
		{
			name:             "fail: no node at all",
			instanceID:       testInstanceID,
			node:             nil,
			expectedNodeName: types.NodeName(""),
			expectedErr:      fmt.Errorf("node with instanceID \"i-02bce90670bb0c7cd\" not found"),
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			awsServices := newMockedFakeAWSServices(TestClusterID)
			awsCloud, err := newAWSCloud(config.CloudConfig{}, awsServices)
			if err != nil {
				t.Fatalf("error creating mock cloud: %v", err)
			}
			awsCloud.kubeClient = fake.NewSimpleClientset()
			fakeInformerFactory := informers.NewSharedInformerFactory(awsCloud.kubeClient, 0)
			awsCloud.SetInformers(fakeInformerFactory)
			if testCase.node != nil {
				awsCloud.nodeInformer.Informer().GetStore().Add(testCase.node)
			}
			awsCloud.nodeInformerHasSynced = informerSynced
			nodeName, err := awsCloud.instanceIDToNodeName(testCase.instanceID)
			assert.Equal(t, testCase.expectedNodeName, nodeName)
			assert.Equal(t, testCase.expectedErr, err)
		})
	}
}

func informerSynced() bool {
	return true
}

func informerNotSynced() bool {
	return false
}

type MockedFakeELBV2 struct {
	LoadBalancers []elbv2types.LoadBalancer
	TargetGroups  []elbv2types.TargetGroup
	Listeners     []elbv2types.Listener

	// keys on all of these maps are ARNs
	LoadBalancerAttributes map[string]map[string]string
	Tags                   map[string][]elbv2types.Tag
	RegisteredInstances    map[string][]string // value is list of instance IDs
}

func (m *MockedFakeELBV2) AddTags(ctx context.Context, input *elbv2.AddTagsInput, optFns ...func(*elbv2.Options)) (*elbv2.AddTagsOutput, error) {
	for _, arn := range input.ResourceArns {
		for _, tag := range input.Tags {
			m.Tags[arn] = append(m.Tags[arn], tag)
		}
	}

	return &elbv2.AddTagsOutput{}, nil
}

func (m *MockedFakeELBV2) CreateLoadBalancer(ctx context.Context, input *elbv2.CreateLoadBalancerInput, optFns ...func(*elbv2.Options)) (*elbv2.CreateLoadBalancerOutput, error) {
	accountID := 123456789
	arn := fmt.Sprintf("arn:aws:elasticloadbalancing:us-west-2:%d:loadbalancer/net/%x/%x",
		accountID,
		rand.Uint64(),
		rand.Uint32())

	newLB := elbv2types.LoadBalancer{
		LoadBalancerArn:  aws.String(arn),
		LoadBalancerName: input.Name,
		Type:             elbv2types.LoadBalancerTypeEnumNetwork,
		VpcId:            aws.String("vpc-abc123def456abc78"),
		AvailabilityZones: []elbv2types.AvailabilityZone{
			{
				ZoneName: aws.String("us-west-2a"),
				SubnetId: aws.String("subnet-abc123de"),
			},
		},
	}
	m.LoadBalancers = append(m.LoadBalancers, newLB)

	return &elbv2.CreateLoadBalancerOutput{
		LoadBalancers: []elbv2types.LoadBalancer{newLB},
	}, nil
}

func (m *MockedFakeELBV2) DescribeLoadBalancers(ctx context.Context, input *elbv2.DescribeLoadBalancersInput, optFns ...func(*elbv2.Options)) (*elbv2.DescribeLoadBalancersOutput, error) {
	findMeNames := make(map[string]bool)
	for _, name := range input.Names {
		findMeNames[name] = true
	}

	findMeARNs := make(map[string]bool)
	for _, arn := range input.LoadBalancerArns {
		findMeARNs[arn] = true
	}

	result := []elbv2types.LoadBalancer{}

	for _, lb := range m.LoadBalancers {
		if _, present := findMeNames[aws.ToString(lb.LoadBalancerName)]; present {
			result = append(result, lb)
			delete(findMeNames, aws.ToString(lb.LoadBalancerName))
		} else if _, present := findMeARNs[aws.ToString(lb.LoadBalancerArn)]; present {
			result = append(result, lb)
			delete(findMeARNs, aws.ToString(lb.LoadBalancerArn))
		}
	}

	if len(findMeNames) > 0 || len(findMeARNs) > 0 {
		return nil, &elbv2types.LoadBalancerNotFoundException{Message: aws.String("not found")}
	}

	return &elbv2.DescribeLoadBalancersOutput{
		LoadBalancers: result,
	}, nil
}

func (m *MockedFakeELBV2) DeleteLoadBalancer(ctx context.Context, input *elbv2.DeleteLoadBalancerInput, optFns ...func(*elbv2.Options)) (*elbv2.DeleteLoadBalancerOutput, error) {
	panic("Not implemented")
}

func (m *MockedFakeELBV2) ModifyLoadBalancerAttributes(ctx context.Context, input *elbv2.ModifyLoadBalancerAttributesInput, optFns ...func(*elbv2.Options)) (*elbv2.ModifyLoadBalancerAttributesOutput, error) {
	attrMap, present := m.LoadBalancerAttributes[aws.ToString(input.LoadBalancerArn)]

	if !present {
		attrMap = make(map[string]string)
		m.LoadBalancerAttributes[aws.ToString(input.LoadBalancerArn)] = attrMap
	}

	for _, attr := range input.Attributes {
		attrMap[aws.ToString(attr.Key)] = aws.ToString(attr.Value)
	}

	return &elbv2.ModifyLoadBalancerAttributesOutput{
		Attributes: input.Attributes,
	}, nil
}

func (m *MockedFakeELBV2) DescribeLoadBalancerAttributes(ctx context.Context, input *elbv2.DescribeLoadBalancerAttributesInput, optFns ...func(*elbv2.Options)) (*elbv2.DescribeLoadBalancerAttributesOutput, error) {
	attrs := []elbv2types.LoadBalancerAttribute{}

	if lbAttrs, present := m.LoadBalancerAttributes[aws.ToString(input.LoadBalancerArn)]; present {
		for key, value := range lbAttrs {
			attrs = append(attrs, elbv2types.LoadBalancerAttribute{
				Key:   aws.String(key),
				Value: aws.String(value),
			})
		}
	}

	return &elbv2.DescribeLoadBalancerAttributesOutput{
		Attributes: attrs,
	}, nil
}

func (m *MockedFakeELBV2) CreateTargetGroup(ctx context.Context, input *elbv2.CreateTargetGroupInput, optFns ...func(*elbv2.Options)) (*elbv2.CreateTargetGroupOutput, error) {
	accountID := 123456789
	arn := fmt.Sprintf("arn:aws:elasticloadbalancing:us-west-2:%d:targetgroup/%x/%x",
		accountID,
		rand.Uint64(),
		rand.Uint32())

	newTG := elbv2types.TargetGroup{
		TargetGroupArn:             aws.String(arn),
		TargetGroupName:            input.Name,
		Port:                       input.Port,
		Protocol:                   input.Protocol,
		HealthCheckProtocol:        input.HealthCheckProtocol,
		HealthCheckPath:            input.HealthCheckPath,
		HealthCheckPort:            input.HealthCheckPort,
		HealthCheckTimeoutSeconds:  input.HealthCheckTimeoutSeconds,
		HealthCheckIntervalSeconds: input.HealthCheckIntervalSeconds,
		HealthyThresholdCount:      input.HealthyThresholdCount,
		UnhealthyThresholdCount:    input.UnhealthyThresholdCount,
	}

	m.TargetGroups = append(m.TargetGroups, newTG)

	return &elbv2.CreateTargetGroupOutput{
		TargetGroups: []elbv2types.TargetGroup{newTG},
	}, nil
}

func (m *MockedFakeELBV2) DescribeTargetGroups(ctx context.Context, input *elbv2.DescribeTargetGroupsInput, optFns ...func(*elbv2.Options)) (*elbv2.DescribeTargetGroupsOutput, error) {
	var targetGroups []elbv2types.TargetGroup

	if input.LoadBalancerArn != nil {
		targetGroups = []elbv2types.TargetGroup{}

		for _, tg := range m.TargetGroups {
			for _, lbArn := range tg.LoadBalancerArns {
				if lbArn == aws.ToString(input.LoadBalancerArn) {
					targetGroups = append(targetGroups, tg)
					break
				}
			}
		}
	} else if len(input.Names) != 0 {
		targetGroups = []elbv2types.TargetGroup{}

		for _, tg := range m.TargetGroups {
			for _, name := range input.Names {
				if aws.ToString(tg.TargetGroupName) == name {
					targetGroups = append(targetGroups, tg)
					break
				}
			}
		}
	} else if len(input.TargetGroupArns) != 0 {
		targetGroups = []elbv2types.TargetGroup{}

		for _, tg := range m.TargetGroups {
			for _, arn := range input.TargetGroupArns {
				if aws.ToString(tg.TargetGroupArn) == arn {
					targetGroups = append(targetGroups, tg)
					break
				}
			}
		}
	} else {
		targetGroups = m.TargetGroups
	}

	return &elbv2.DescribeTargetGroupsOutput{
		TargetGroups: targetGroups,
	}, nil
}

func (m *MockedFakeELBV2) ModifyTargetGroup(ctx context.Context, input *elbv2.ModifyTargetGroupInput, optFns ...func(*elbv2.Options)) (*elbv2.ModifyTargetGroupOutput, error) {
	var matchingTargetGroup *elbv2types.TargetGroup
	dirtyGroups := []elbv2types.TargetGroup{}

	for _, tg := range m.TargetGroups {
		if aws.ToString(tg.TargetGroupArn) == aws.ToString(input.TargetGroupArn) {
			matchingTargetGroup = &tg
			break
		}
	}

	if matchingTargetGroup != nil {
		dirtyGroups = append(dirtyGroups, *matchingTargetGroup)

		if input.HealthCheckEnabled != nil {
			matchingTargetGroup.HealthCheckEnabled = input.HealthCheckEnabled
		}
		if input.HealthCheckIntervalSeconds != nil {
			matchingTargetGroup.HealthCheckIntervalSeconds = input.HealthCheckIntervalSeconds
		}
		if input.HealthCheckPath != nil {
			matchingTargetGroup.HealthCheckPath = input.HealthCheckPath
		}
		if input.HealthCheckPort != nil {
			matchingTargetGroup.HealthCheckPort = input.HealthCheckPort
		}
		if string(input.HealthCheckProtocol) != "" {
			matchingTargetGroup.HealthCheckProtocol = input.HealthCheckProtocol
		}
		if input.HealthCheckTimeoutSeconds != nil {
			matchingTargetGroup.HealthCheckTimeoutSeconds = input.HealthCheckTimeoutSeconds
		}
		if input.HealthyThresholdCount != nil {
			matchingTargetGroup.HealthyThresholdCount = input.HealthyThresholdCount
		}
		if input.Matcher != nil {
			matchingTargetGroup.Matcher = input.Matcher
		}
		if input.UnhealthyThresholdCount != nil {
			matchingTargetGroup.UnhealthyThresholdCount = input.UnhealthyThresholdCount
		}
	}

	return &elbv2.ModifyTargetGroupOutput{
		TargetGroups: dirtyGroups,
	}, nil
}

func (m *MockedFakeELBV2) DeleteTargetGroup(ctx context.Context, input *elbv2.DeleteTargetGroupInput, optFns ...func(*elbv2.Options)) (*elbv2.DeleteTargetGroupOutput, error) {
	newTargetGroups := []elbv2types.TargetGroup{}

	for _, tg := range m.TargetGroups {
		if aws.ToString(tg.TargetGroupArn) != aws.ToString(input.TargetGroupArn) {
			newTargetGroups = append(newTargetGroups, tg)
		}
	}

	m.TargetGroups = newTargetGroups

	delete(m.RegisteredInstances, aws.ToString(input.TargetGroupArn))

	return &elbv2.DeleteTargetGroupOutput{}, nil
}

func (m *MockedFakeELBV2) DescribeTargetHealth(ctx context.Context, input *elbv2.DescribeTargetHealthInput, optFns ...func(*elbv2.Options)) (*elbv2.DescribeTargetHealthOutput, error) {
	healthDescriptions := []elbv2types.TargetHealthDescription{}

	var matchingTargetGroup elbv2types.TargetGroup

	for _, tg := range m.TargetGroups {
		if aws.ToString(tg.TargetGroupArn) == aws.ToString(input.TargetGroupArn) {
			matchingTargetGroup = tg
			break
		}
	}

	if registeredTargets, present := m.RegisteredInstances[aws.ToString(input.TargetGroupArn)]; present {
		for _, target := range registeredTargets {
			healthDescriptions = append(healthDescriptions, elbv2types.TargetHealthDescription{
				HealthCheckPort: matchingTargetGroup.HealthCheckPort,
				Target: &elbv2types.TargetDescription{
					Id:   aws.String(target),
					Port: matchingTargetGroup.Port,
				},
				TargetHealth: &elbv2types.TargetHealth{
					State: elbv2types.TargetHealthStateEnumHealthy,
				},
			})
		}
	}

	return &elbv2.DescribeTargetHealthOutput{
		TargetHealthDescriptions: healthDescriptions,
	}, nil
}

func (m *MockedFakeELBV2) DescribeTargetGroupAttributes(ctx context.Context, input *elbv2.DescribeTargetGroupAttributesInput, optFns ...func(*elbv2.Options)) (*elbv2.DescribeTargetGroupAttributesOutput, error) {
	panic("Not implemented")
}

func (m *MockedFakeELBV2) ModifyTargetGroupAttributes(ctx context.Context, input *elbv2.ModifyTargetGroupAttributesInput, optFns ...func(*elbv2.Options)) (*elbv2.ModifyTargetGroupAttributesOutput, error) {
	panic("Not implemented")
}

func (m *MockedFakeELBV2) RegisterTargets(ctx context.Context, input *elbv2.RegisterTargetsInput, optFns ...func(*elbv2.Options)) (*elbv2.RegisterTargetsOutput, error) {
	arn := aws.ToString(input.TargetGroupArn)
	alreadyExists := make(map[string]bool)
	for _, targetID := range m.RegisteredInstances[arn] {
		alreadyExists[targetID] = true
	}
	for _, target := range input.Targets {
		if !alreadyExists[aws.ToString(target.Id)] {
			m.RegisteredInstances[arn] = append(m.RegisteredInstances[arn], aws.ToString(target.Id))
		}
	}
	return &elbv2.RegisterTargetsOutput{}, nil
}

func (m *MockedFakeELBV2) DeregisterTargets(ctx context.Context, input *elbv2.DeregisterTargetsInput, optFns ...func(*elbv2.Options)) (*elbv2.DeregisterTargetsOutput, error) {
	removeMe := make(map[string]bool)

	for _, target := range input.Targets {
		removeMe[aws.ToString(target.Id)] = true
	}
	newRegisteredInstancesForArn := []string{}
	for _, targetID := range m.RegisteredInstances[aws.ToString(input.TargetGroupArn)] {
		if !removeMe[targetID] {
			newRegisteredInstancesForArn = append(newRegisteredInstancesForArn, targetID)
		}
	}
	m.RegisteredInstances[aws.ToString(input.TargetGroupArn)] = newRegisteredInstancesForArn

	return &elbv2.DeregisterTargetsOutput{}, nil
}

func (m *MockedFakeELBV2) CreateListener(ctx context.Context, input *elbv2.CreateListenerInput, optFns ...func(*elbv2.Options)) (*elbv2.CreateListenerOutput, error) {
	accountID := 123456789
	arn := fmt.Sprintf("arn:aws:elasticloadbalancing:us-west-2:%d:listener/net/%x/%x/%x",
		accountID,
		rand.Uint64(),
		rand.Uint32(),
		rand.Uint32())

	newListener := elbv2types.Listener{
		ListenerArn:     aws.String(arn),
		Port:            input.Port,
		Protocol:        input.Protocol,
		DefaultActions:  input.DefaultActions,
		LoadBalancerArn: input.LoadBalancerArn,
	}

	m.Listeners = append(m.Listeners, newListener)

	for _, tg := range m.TargetGroups {
		for _, action := range input.DefaultActions {
			if aws.ToString(action.TargetGroupArn) == aws.ToString(tg.TargetGroupArn) {
				tg.LoadBalancerArns = append(tg.LoadBalancerArns, aws.ToString(input.LoadBalancerArn))
				break
			}
		}
	}

	return &elbv2.CreateListenerOutput{
		Listeners: []elbv2types.Listener{newListener},
	}, nil
}

func (m *MockedFakeELBV2) DescribeListeners(ctx context.Context, input *elbv2.DescribeListenersInput, optFns ...func(*elbv2.Options)) (*elbv2.DescribeListenersOutput, error) {
	if len(input.ListenerArns) == 0 && input.LoadBalancerArn == nil {
		return &elbv2.DescribeListenersOutput{
			Listeners: m.Listeners,
		}, nil
	} else if len(input.ListenerArns) == 0 {
		listeners := []elbv2types.Listener{}

		for _, lb := range m.Listeners {
			if aws.ToString(lb.LoadBalancerArn) == aws.ToString(input.LoadBalancerArn) {
				listeners = append(listeners, lb)
			}
		}

		return &elbv2.DescribeListenersOutput{
			Listeners: listeners,
		}, nil
	}
	panic("Not implemented")
}

func (m *MockedFakeELBV2) DeleteListener(ctx context.Context, input *elbv2.DeleteListenerInput, optFns ...func(*elbv2.Options)) (*elbv2.DeleteListenerOutput, error) {
	panic("Not implemented")
}

func (m *MockedFakeELBV2) ModifyListener(ctx context.Context, input *elbv2.ModifyListenerInput, optFns ...func(*elbv2.Options)) (*elbv2.ModifyListenerOutput, error) {

	modifiedListeners := []elbv2types.Listener{}
	for i := range m.Listeners {
		listener := &m.Listeners[i]
		if aws.ToString(listener.ListenerArn) == aws.ToString(input.ListenerArn) {
			if input.DefaultActions != nil {
				// for each old action, find the corresponding target group, and remove the listener's LB ARN from its list
				for _, action := range listener.DefaultActions {
					var targetGroupForAction *elbv2types.TargetGroup

					for _, tg := range m.TargetGroups {
						if aws.ToString(action.TargetGroupArn) == aws.ToString(tg.TargetGroupArn) {
							targetGroupForAction = &tg
							break
						}
					}

					if targetGroupForAction != nil {
						newLoadBalancerARNs := []string{}
						for _, lbArn := range targetGroupForAction.LoadBalancerArns {
							if lbArn != aws.ToString(listener.LoadBalancerArn) {
								newLoadBalancerARNs = append(newLoadBalancerARNs, lbArn)
							}
						}

						targetGroupForAction.LoadBalancerArns = newLoadBalancerARNs
					}
				}

				listener.DefaultActions = input.DefaultActions

				// for each new action, add the listener's LB ARN to that action's target groups' lists
				for _, action := range input.DefaultActions {
					var targetGroupForAction *elbv2types.TargetGroup

					for _, tg := range m.TargetGroups {
						if aws.ToString(action.TargetGroupArn) == aws.ToString(tg.TargetGroupArn) {
							targetGroupForAction = &tg
							break
						}
					}

					if targetGroupForAction != nil {
						targetGroupForAction.LoadBalancerArns = append(targetGroupForAction.LoadBalancerArns, aws.ToString(listener.LoadBalancerArn))
					}
				}
			}
			if input.Port != nil {
				listener.Port = input.Port
			}
			if string(input.Protocol) != "" {
				listener.Protocol = input.Protocol
			}

			modifiedListeners = append(modifiedListeners, *listener)
		}

	}

	return &elbv2.ModifyListenerOutput{
		Listeners: modifiedListeners,
	}, nil
}

func (m *MockedFakeEC2) maybeExpectDescribeSecurityGroups(clusterID, groupName string) {
	tags := []ec2types.Tag{
		{Key: aws.String(TagNameKubernetesClusterLegacy), Value: aws.String(clusterID)},
		{Key: aws.String(fmt.Sprintf("%s%s", TagNameKubernetesClusterPrefix, clusterID)), Value: aws.String(ResourceLifecycleOwned)},
	}

	m.On("DescribeSecurityGroups", &ec2.DescribeSecurityGroupsInput{Filters: []ec2types.Filter{
		newEc2Filter("group-name", groupName),
		newEc2Filter("vpc-id", ""),
	}}).Maybe().Return([]ec2types.SecurityGroup{{Tags: tags}})

	m.On("DescribeSecurityGroups", &ec2.DescribeSecurityGroupsInput{}).Maybe().Return([]ec2types.SecurityGroup{{Tags: tags}})
}

func TestNLBNodeRegistration(t *testing.T) {
	awsServices := newMockedFakeAWSServices(TestClusterID)
	awsServices.elbv2 = &MockedFakeELBV2{Tags: make(map[string][]elbv2types.Tag), RegisteredInstances: make(map[string][]string), LoadBalancerAttributes: make(map[string]map[string]string)}
	c, _ := newAWSCloud(config.CloudConfig{}, awsServices)

	awsServices.ec2.(*MockedFakeEC2).Subnets = []ec2types.Subnet{
		{
			AvailabilityZone: aws.String("us-west-2a"),
			SubnetId:         aws.String("subnet-abc123de"),
			Tags: []ec2types.Tag{
				{
					Key:   aws.String(c.tagging.clusterTagKey()),
					Value: aws.String("owned"),
				},
			},
		},
	}

	awsServices.ec2.(*MockedFakeEC2).RouteTables = []ec2types.RouteTable{
		{
			Associations: []ec2types.RouteTableAssociation{
				{
					Main:                    aws.Bool(true),
					RouteTableAssociationId: aws.String("rtbassoc-abc123def456abc78"),
					RouteTableId:            aws.String("rtb-abc123def456abc78"),
					SubnetId:                aws.String("subnet-abc123de"),
				},
			},
			RouteTableId: aws.String("rtb-abc123def456abc78"),
			Routes: []ec2types.Route{
				{
					DestinationCidrBlock: aws.String("0.0.0.0/0"),
					GatewayId:            aws.String("igw-abc123def456abc78"),
					State:                ec2types.RouteStateActive,
				},
			},
		},
	}
	awsServices.ec2.(*MockedFakeEC2).maybeExpectDescribeSecurityGroups(TestClusterID, "k8s-elb-aid")

	nodes := []*v1.Node{makeNamedNode(awsServices, 0, "a"), makeNamedNode(awsServices, 1, "b"), makeNamedNode(awsServices, 2, "c")}

	fauxService := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: "myservice",
			UID:  "id",
			Annotations: map[string]string{
				"service.beta.kubernetes.io/aws-load-balancer-type": "nlb",
			},
		},
		Spec: v1.ServiceSpec{
			Ports: []v1.ServicePort{
				{
					Name:       "http",
					Port:       8080,
					NodePort:   31173,
					TargetPort: intstr.FromInt(31173),
					Protocol:   v1.ProtocolTCP,
				},
			},
			SessionAffinity: v1.ServiceAffinityNone,
		},
	}

	_, err := c.EnsureLoadBalancer(context.TODO(), TestClusterName, fauxService, nodes)
	if err != nil {
		t.Errorf("EnsureLoadBalancer returned an error: %v", err)
	}
	for _, instances := range awsServices.elbv2.(*MockedFakeELBV2).RegisteredInstances {
		if len(instances) != 3 {
			t.Errorf("Expected 3 nodes registered with target group, saw %d", len(instances))
		}
	}

	_, err = c.EnsureLoadBalancer(context.TODO(), TestClusterName, fauxService, nodes[:2])
	if err != nil {
		t.Errorf("EnsureLoadBalancer returned an error: %v", err)
	}
	for _, instances := range awsServices.elbv2.(*MockedFakeELBV2).RegisteredInstances {
		if len(instances) != 2 {
			t.Errorf("Expected 2 nodes registered with target group, saw %d", len(instances))
		}
	}

	_, err = c.EnsureLoadBalancer(context.TODO(), TestClusterName, fauxService, nodes)
	if err != nil {
		t.Errorf("EnsureLoadBalancer returned an error: %v", err)
	}
	for _, instances := range awsServices.elbv2.(*MockedFakeELBV2).RegisteredInstances {
		if len(instances) != 3 {
			t.Errorf("Expected 3 nodes registered with target group, saw %d", len(instances))
		}
	}

	fauxService.Annotations[ServiceAnnotationLoadBalancerHealthCheckProtocol] = "http"
	tgARN := aws.ToString(awsServices.elbv2.(*MockedFakeELBV2).Listeners[0].DefaultActions[0].TargetGroupArn)
	_, err = c.EnsureLoadBalancer(context.TODO(), TestClusterName, fauxService, nodes)
	if err != nil {
		t.Errorf("EnsureLoadBalancer returned an error: %v", err)
	}
	assert.Equal(t, 1, len(awsServices.elbv2.(*MockedFakeELBV2).Listeners))
	assert.NotEqual(t, tgARN, aws.ToString(awsServices.elbv2.(*MockedFakeELBV2).Listeners[0].DefaultActions[0].TargetGroupArn))
}

func makeNamedNode(s *FakeAWSServices, offset int, name string) *v1.Node {
	instanceID := fmt.Sprintf("i-%x", int64(0x02bce90670bb0c7cd)+int64(offset))
	instance := &ec2types.Instance{}
	instance.InstanceId = aws.String(instanceID)
	instance.Placement = &ec2types.Placement{
		AvailabilityZone: aws.String("us-west-2c"),
	}
	instance.PrivateDnsName = aws.String(fmt.Sprintf("ip-172-20-0-%d.ec2.internal", 101+offset))
	instance.PrivateIpAddress = aws.String(fmt.Sprintf("192.168.0.%d", 1+offset))

	var tag ec2types.Tag
	tag.Key = aws.String(TagNameKubernetesClusterLegacy)
	tag.Value = aws.String(TestClusterID)
	instance.Tags = []ec2types.Tag{tag}

	s.instances = append(s.instances, instance)

	testProviderID := "aws:///us-west-2c/" + instanceID
	return &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: v1.NodeSpec{
			ProviderID: testProviderID,
		},
	}
}

func newMockedFakeAWSServices(id string) *FakeAWSServices {
	s := NewFakeAWSServices(id)
	s.ec2 = &MockedFakeEC2{FakeEC2Impl: s.ec2.(*FakeEC2Impl)}
	s.elb = &MockedFakeELB{FakeELB: s.elb.(*FakeELB)}
	return s
}

func TestAzToRegion(t *testing.T) {
	testCases := []struct {
		az     string
		region string
	}{
		{"us-west-2a", "us-west-2"},
		{"us-west-2-lax-1a", "us-west-2"},
		{"ap-northeast-2a", "ap-northeast-2"},
		{"us-gov-east-1a", "us-gov-east-1"},
		{"us-iso-east-1a", "us-iso-east-1"},
		{"us-isob-east-1a", "us-isob-east-1"},
	}

	for _, testCase := range testCases {
		result, err := azToRegion(testCase.az)
		assert.NoError(t, err)
		assert.Equal(t, testCase.region, result)
	}
}

func TestCloud_sortELBSecurityGroupList(t *testing.T) {
	type args struct {
		securityGroupIDs       []string
		annotations            map[string]string
		taggedLBSecurityGroups map[string]struct{}
	}
	tests := []struct {
		name                 string
		args                 args
		wantSecurityGroupIDs []string
	}{
		{
			name: "with no annotation",
			args: args{
				securityGroupIDs: []string{"sg-1"},
				annotations:      map[string]string{},
			},
			wantSecurityGroupIDs: []string{"sg-1"},
		},
		{
			name: "with service.beta.kubernetes.io/aws-load-balancer-security-groups",
			args: args{
				securityGroupIDs: []string{"sg-2", "sg-1", "sg-3"},
				annotations: map[string]string{
					"service.beta.kubernetes.io/aws-load-balancer-security-groups": "sg-3,sg-2,sg-1",
				},
			},
			wantSecurityGroupIDs: []string{"sg-3", "sg-2", "sg-1"},
		},
		{
			name: "with service.beta.kubernetes.io/aws-load-balancer-extra-security-groups",
			args: args{
				securityGroupIDs: []string{"sg-2", "sg-1", "sg-3", "sg-4"},
				annotations: map[string]string{
					"service.beta.kubernetes.io/aws-load-balancer-extra-security-groups": "sg-3,sg-2,sg-1",
				},
			},
			wantSecurityGroupIDs: []string{"sg-4", "sg-3", "sg-2", "sg-1"},
		},
		{
			name: "with both annotation",
			args: args{
				securityGroupIDs: []string{"sg-2", "sg-1", "sg-3", "sg-4", "sg-5", "sg-6"},
				annotations: map[string]string{
					"service.beta.kubernetes.io/aws-load-balancer-security-groups":       "sg-3,sg-2,sg-1",
					"service.beta.kubernetes.io/aws-load-balancer-extra-security-groups": "sg-6,sg-5",
				},
			},
			wantSecurityGroupIDs: []string{"sg-3", "sg-2", "sg-1", "sg-4", "sg-6", "sg-5"},
		},
		{
			name: "with an untagged, and unknown security group",
			args: args{
				securityGroupIDs: []string{"sg-2", "sg-1"},
				taggedLBSecurityGroups: map[string]struct{}{
					"sg-1": {},
				},
			},
			wantSecurityGroupIDs: []string{"sg-1", "sg-2"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Cloud{}
			c.sortELBSecurityGroupList(tt.args.securityGroupIDs, tt.args.annotations, tt.args.taggedLBSecurityGroups)
			assert.Equal(t, tt.wantSecurityGroupIDs, tt.args.securityGroupIDs)
		})
	}
}

func TestCloud_buildNLBHealthCheckConfiguration(t *testing.T) {
	tests := []struct {
		name         string
		annotations  map[string]string
		service      *v1.Service
		modifyConfig func(*config.CloudConfig)
		want         healthCheckConfig
		wantError    bool
	}{
		{
			name:        "default cluster",
			annotations: map[string]string{},
			service: &v1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-svc",
					UID:  "UID",
				},
				Spec: v1.ServiceSpec{
					Ports: []v1.ServicePort{
						{
							Name:       "http",
							Protocol:   v1.ProtocolTCP,
							Port:       8080,
							TargetPort: intstr.FromInt(8880),
							NodePort:   32205,
						},
					},
				},
			},
			want: healthCheckConfig{
				Port:               "traffic-port",
				Protocol:           elbv2types.ProtocolEnumTcp,
				Interval:           30,
				Timeout:            10,
				HealthyThreshold:   3,
				UnhealthyThreshold: 3,
			},
			wantError: false,
		},
		{
			name:        "default cluster with shared health check",
			annotations: map[string]string{},
			service: &v1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-svc",
					UID:  "UID",
				},
				Spec: v1.ServiceSpec{
					Ports: []v1.ServicePort{
						{
							Name:       "http",
							Protocol:   v1.ProtocolTCP,
							Port:       8080,
							TargetPort: intstr.FromInt(8880),
							NodePort:   32205,
						},
					},
				},
			},
			modifyConfig: func(cfg *config.CloudConfig) {
				cfg.Global.ClusterServiceLoadBalancerHealthProbeMode = config.ClusterServiceLoadBalancerHealthProbeModeShared
			},
			want: healthCheckConfig{
				Port:               "10256",
				Protocol:           elbv2types.ProtocolEnumHttp,
				Path:               "/healthz",
				Interval:           30,
				Timeout:            10,
				HealthyThreshold:   3,
				UnhealthyThreshold: 3,
			},
			wantError: false,
		},
		{
			name:        "default cluster with shared health check and custom port",
			annotations: map[string]string{},
			service: &v1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-svc",
					UID:  "UID",
				},
				Spec: v1.ServiceSpec{
					Ports: []v1.ServicePort{
						{
							Name:       "http",
							Protocol:   v1.ProtocolTCP,
							Port:       8080,
							TargetPort: intstr.FromInt(8880),
							NodePort:   32205,
						},
					},
				},
			},
			modifyConfig: func(cfg *config.CloudConfig) {
				cfg.Global.ClusterServiceLoadBalancerHealthProbeMode = config.ClusterServiceLoadBalancerHealthProbeModeShared
				cfg.Global.ClusterServiceSharedLoadBalancerHealthProbePort = 8080
			},
			want: healthCheckConfig{
				Port:               "8080",
				Protocol:           elbv2types.ProtocolEnumHttp,
				Path:               "/healthz",
				Interval:           30,
				Timeout:            10,
				HealthyThreshold:   3,
				UnhealthyThreshold: 3,
			},
			wantError: false,
		},
		{
			name:        "default cluster with shared health check and custom path",
			annotations: map[string]string{},
			service: &v1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-svc",
					UID:  "UID",
				},
				Spec: v1.ServiceSpec{
					Ports: []v1.ServicePort{
						{
							Name:       "http",
							Protocol:   v1.ProtocolTCP,
							Port:       8080,
							TargetPort: intstr.FromInt(8880),
							NodePort:   32205,
						},
					},
				},
			},
			modifyConfig: func(cfg *config.CloudConfig) {
				cfg.Global.ClusterServiceLoadBalancerHealthProbeMode = config.ClusterServiceLoadBalancerHealthProbeModeShared
				cfg.Global.ClusterServiceSharedLoadBalancerHealthProbePath = "/custom-healthz"
			},
			want: healthCheckConfig{
				Port:               "10256",
				Protocol:           elbv2types.ProtocolEnumHttp,
				Path:               "/custom-healthz",
				Interval:           30,
				Timeout:            10,
				HealthyThreshold:   3,
				UnhealthyThreshold: 3,
			},
			wantError: false,
		},
		{
			name:        "default local",
			annotations: map[string]string{},
			service: &v1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-svc",
					UID:  "UID",
				},
				Spec: v1.ServiceSpec{
					Type: v1.ServiceTypeLoadBalancer,
					Ports: []v1.ServicePort{
						{
							Name:       "http",
							Protocol:   v1.ProtocolTCP,
							Port:       8080,
							TargetPort: intstr.FromInt(8880),
							NodePort:   32205,
						},
					},
					ExternalTrafficPolicy: v1.ServiceExternalTrafficPolicyTypeLocal,
					HealthCheckNodePort:   32213,
				},
			},
			want: healthCheckConfig{
				Port:               "32213",
				Path:               "/healthz",
				Protocol:           elbv2types.ProtocolEnumHttp,
				Interval:           10,
				Timeout:            10,
				HealthyThreshold:   2,
				UnhealthyThreshold: 2,
			},
			wantError: false,
		},
		{
			name: "with TCP healthcheck",
			service: &v1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-svc",
					UID:  "UID",
					Annotations: map[string]string{
						ServiceAnnotationLoadBalancerHealthCheckProtocol:  "TCP",
						ServiceAnnotationLoadBalancerHealthCheckPort:      "8001",
						ServiceAnnotationLoadBalancerHealthCheckPath:      "/healthz",
						ServiceAnnotationLoadBalancerHCHealthyThreshold:   "4",
						ServiceAnnotationLoadBalancerHCUnhealthyThreshold: "4",
						ServiceAnnotationLoadBalancerHCInterval:           "10",
						ServiceAnnotationLoadBalancerHCTimeout:            "5",
					},
				},
				Spec: v1.ServiceSpec{
					Type: v1.ServiceTypeLoadBalancer,
					Ports: []v1.ServicePort{
						{
							Name:       "http",
							Protocol:   v1.ProtocolTCP,
							Port:       8080,
							TargetPort: intstr.FromInt(8880),
							NodePort:   32205,
						},
					},
					ExternalTrafficPolicy: v1.ServiceExternalTrafficPolicyTypeLocal,
					HealthCheckNodePort:   32213,
				},
			},
			want: healthCheckConfig{
				Interval:           10,
				Timeout:            5,
				Protocol:           "TCP",
				Port:               "8001",
				HealthyThreshold:   4,
				UnhealthyThreshold: 4,
			},
			wantError: false,
		},
		{
			name: "with HTTP healthcheck",
			service: &v1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-svc",
					UID:  "UID",
					Annotations: map[string]string{
						ServiceAnnotationLoadBalancerHealthCheckProtocol:  "HTTP",
						ServiceAnnotationLoadBalancerHealthCheckPort:      "41233",
						ServiceAnnotationLoadBalancerHealthCheckPath:      "/healthz",
						ServiceAnnotationLoadBalancerHCHealthyThreshold:   "5",
						ServiceAnnotationLoadBalancerHCUnhealthyThreshold: "5",
						ServiceAnnotationLoadBalancerHCInterval:           "30",
						ServiceAnnotationLoadBalancerHCTimeout:            "6",
					},
				},
				Spec: v1.ServiceSpec{
					Ports: []v1.ServicePort{
						{
							Name:       "http",
							Protocol:   v1.ProtocolTCP,
							Port:       8080,
							TargetPort: intstr.FromInt(8880),
							NodePort:   32205,
						},
					},
				},
			},
			want: healthCheckConfig{
				Interval:           30,
				Timeout:            6,
				Protocol:           "HTTP",
				Port:               "41233",
				Path:               "/healthz",
				HealthyThreshold:   5,
				UnhealthyThreshold: 5,
			},
			wantError: false,
		},
		{
			name: "HTTP healthcheck default path",
			service: &v1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-svc",
					UID:  "UID",
					Annotations: map[string]string{
						ServiceAnnotationLoadBalancerHealthCheckProtocol: "Http",
					},
				},
				Spec: v1.ServiceSpec{
					Ports: []v1.ServicePort{
						{
							Name:       "http",
							Protocol:   v1.ProtocolTCP,
							Port:       8080,
							TargetPort: intstr.FromInt(8880),
							NodePort:   32205,
						},
					},
				},
			},
			want: healthCheckConfig{
				Interval:           30,
				Timeout:            10,
				Protocol:           "HTTP",
				Path:               "/",
				Port:               "traffic-port",
				HealthyThreshold:   3,
				UnhealthyThreshold: 3,
			},
			wantError: false,
		},
		{
			name: "interval not 10 or 30",
			service: &v1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-svc",
					UID:  "UID",
					Annotations: map[string]string{
						ServiceAnnotationLoadBalancerHCInterval: "23",
					},
				},
				Spec: v1.ServiceSpec{
					Ports: []v1.ServicePort{
						{
							Name:       "http",
							Protocol:   v1.ProtocolTCP,
							Port:       8080,
							TargetPort: intstr.FromInt(8880),
							NodePort:   32205,
						},
					},
				},
			},
			want: healthCheckConfig{
				Port:               "traffic-port",
				Protocol:           elbv2types.ProtocolEnumTcp,
				Interval:           23,
				Timeout:            10,
				HealthyThreshold:   3,
				UnhealthyThreshold: 3,
			},
			wantError: false,
		},
		{
			name: "invalid timeout",
			service: &v1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-svc",
					UID:  "UID",
					Annotations: map[string]string{
						ServiceAnnotationLoadBalancerHCTimeout: "non-numeric",
					},
				},
				Spec: v1.ServiceSpec{
					Ports: []v1.ServicePort{
						{
							Name:       "http",
							Protocol:   v1.ProtocolTCP,
							Port:       8080,
							TargetPort: intstr.FromInt(8880),
							NodePort:   32205,
						},
					},
				},
			},
			want:      healthCheckConfig{},
			wantError: true,
		},
		{
			name: "mismatch healthy and unhealthy targets",
			service: &v1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-svc",
					UID:  "UID",
					Annotations: map[string]string{
						ServiceAnnotationLoadBalancerHCHealthyThreshold:   "7",
						ServiceAnnotationLoadBalancerHCUnhealthyThreshold: "5",
					},
				},
				Spec: v1.ServiceSpec{
					Ports: []v1.ServicePort{
						{
							Name:       "http",
							Protocol:   v1.ProtocolTCP,
							Port:       8080,
							TargetPort: intstr.FromInt(8880),
							NodePort:   32205,
						},
					},
				},
			},
			want: healthCheckConfig{
				Port:               "traffic-port",
				Protocol:           elbv2types.ProtocolEnumTcp,
				Interval:           30,
				Timeout:            10,
				HealthyThreshold:   7,
				UnhealthyThreshold: 5,
			},
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Cloud{
				cfg: &config.CloudConfig{},
			}

			if tt.modifyConfig != nil {
				tt.modifyConfig(c.cfg)
			}

			hc, err := c.buildNLBHealthCheckConfiguration(tt.service)
			if !tt.wantError {
				assert.Equal(t, tt.want, hc)
			} else {
				assert.NotNil(t, err)
			}
		})
	}
}

func Test_parseStringSliceAnnotation(t *testing.T) {
	tests := []struct {
		name        string
		annotation  string
		annotations map[string]string
		want        []string
		wantExist   bool
	}{
		{
			name:       "empty annotation",
			annotation: "test.annotation",
			wantExist:  false,
		},
		{
			name:       "empty value",
			annotation: "a1",
			annotations: map[string]string{
				"a1": "\t, ,,",
			},
			want:      nil,
			wantExist: true,
		},
		{
			name:       "single value",
			annotation: "a1",
			annotations: map[string]string{
				"a1": "   value 1 ",
			},
			want:      []string{"value 1"},
			wantExist: true,
		},
		{
			name:       "multiple values",
			annotation: "a1",
			annotations: map[string]string{
				"a1": "subnet-1, subnet-2, My Subnet ",
			},
			want:      []string{"subnet-1", "subnet-2", "My Subnet"},
			wantExist: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var gotValue []string
			gotExist := parseStringSliceAnnotation(tt.annotations, tt.annotation, &gotValue)
			assert.Equal(t, tt.wantExist, gotExist)
			assert.Equal(t, tt.want, gotValue)
		})
	}
}

func TestNodeAddressesForFargate(t *testing.T) {
	awsServices := newMockedFakeAWSServices(TestClusterID)
	c, _ := newAWSCloud(config.CloudConfig{}, awsServices)

	nodeAddresses, _ := c.NodeAddressesByProviderID(context.TODO(), "aws:///us-west-2c/1abc-2def/fargate-ip-return-private-dns-name.us-west-2.compute.internal")
	verifyNodeAddressesForFargate(t, "IPV4", true, nodeAddresses)
}

func TestNodeAddressesForFargateIPV6Family(t *testing.T) {
	awsServices := newMockedFakeAWSServices(TestClusterID)
	c, _ := newAWSCloud(config.CloudConfig{}, awsServices)
	c.cfg.Global.NodeIPFamilies = []string{"ipv6"}

	nodeAddresses, _ := c.NodeAddressesByProviderID(context.TODO(), "aws:///us-west-2c/1abc-2def/fargate-ip-return-private-dns-name-ipv6.us-west-2.compute.internal")
	verifyNodeAddressesForFargate(t, "IPV6", true, nodeAddresses)
}

func TestNodeAddressesForFargatePrivateIP(t *testing.T) {
	awsServices := newMockedFakeAWSServices(TestClusterID)
	c, _ := newAWSCloud(config.CloudConfig{}, awsServices)

	nodeAddresses, _ := c.NodeAddressesByProviderID(context.TODO(), "aws:///us-west-2c/1abc-2def/fargate-192.168.164.88")
	verifyNodeAddressesForFargate(t, "IPV4", false, nodeAddresses)
}

func verifyNodeAddressesForFargate(t *testing.T, ipFamily string, verifyPublicIP bool, nodeAddresses []v1.NodeAddress) {
	if verifyPublicIP {
		assert.Equal(t, 2, len(nodeAddresses))
		assert.Equal(t, "ip-1-2-3-4.compute.amazon.com", nodeAddresses[1].Address)
		assert.Equal(t, v1.NodeInternalDNS, nodeAddresses[1].Type)
	} else {
		assert.Equal(t, 1, len(nodeAddresses))
	}

	if ipFamily == "IPV4" {
		assert.Equal(t, "1.2.3.4", nodeAddresses[0].Address)
	} else {
		assert.Equal(t, "2001:db8:3333:4444:5555:6666:7777:8888", nodeAddresses[0].Address)
	}
	assert.Equal(t, v1.NodeInternalIP, nodeAddresses[0].Type)
}

func TestNodeAddressesOrderedByDeviceIndex(t *testing.T) {
	awsServices := newMockedFakeAWSServices(TestClusterID)
	c, _ := newAWSCloud(config.CloudConfig{}, awsServices)

	nodeAddresses, _ := c.NodeAddressesByProviderID(context.TODO(), "aws:///us-west-2a/i-self")
	expectedAddresses := []v1.NodeAddress{
		{Type: v1.NodeInternalIP, Address: "172.20.0.100"},
		{Type: v1.NodeInternalIP, Address: "172.20.0.101"},
		{Type: v1.NodeInternalIP, Address: "172.20.1.1"},
		{Type: v1.NodeInternalIP, Address: "172.20.1.2"},
		{Type: v1.NodeExternalIP, Address: "1.2.3.4"},
		{Type: v1.NodeInternalDNS, Address: "ip-172-20-0-100.ec2.internal"},
		{Type: v1.NodeHostName, Address: "ip-172-20-0-100.ec2.internal"},
	}
	assert.Equal(t, expectedAddresses, nodeAddresses)
}

func TestInstanceExistsByProviderIDForFargate(t *testing.T) {
	awsServices := newMockedFakeAWSServices(TestClusterID)
	c, _ := newAWSCloud(config.CloudConfig{}, awsServices)

	instanceExist, err := c.InstanceExistsByProviderID(context.TODO(), "aws:///us-west-2c/1abc-2def/fargate-192.168.164.88")
	assert.Nil(t, err)
	assert.True(t, instanceExist)
}

func TestInstanceExistsByProviderIDWithNodeNameForFargate(t *testing.T) {
	awsServices := newMockedFakeAWSServices(TestClusterID)
	c, _ := newAWSCloud(config.CloudConfig{}, awsServices)

	instanceExist, err := c.InstanceExistsByProviderID(context.TODO(), "aws:///us-west-2c/1abc-2def/fargate-ip-192-168-164-88.us-west-2.compute.internal")
	assert.Nil(t, err)
	assert.True(t, instanceExist)
}

func TestInstanceExistsByProviderIDForInstanceNotFound(t *testing.T) {
	mockedEC2API := newMockedEC2API()
	c := &Cloud{ec2: &awsSdkEC2{ec2: mockedEC2API}, describeInstanceBatcher: newdescribeInstanceBatcher(context.Background(), &awsSdkEC2{ec2: mockedEC2API})}

	mockedEC2API.On("DescribeInstances", mock.Anything).Return(&ec2.DescribeInstancesOutput{}, errors.New("InvalidInstanceID.NotFound: Instance not found"))

	instanceExists, err := c.InstanceExistsByProviderID(context.TODO(), "aws:///us-west-2c/1abc-2def/i-not-found")
	assert.Nil(t, err)
	assert.False(t, instanceExists)
}

func TestInstanceNotExistsByProviderIDForFargate(t *testing.T) {
	awsServices := newMockedFakeAWSServices(TestClusterID)
	c, _ := newAWSCloud(config.CloudConfig{}, awsServices)

	instanceExist, err := c.InstanceExistsByProviderID(context.TODO(), "aws:///us-west-2c/1abc-2def/fargate-not-found")
	assert.Nil(t, err)
	assert.False(t, instanceExist)
}

func TestInstanceShutdownByProviderIDForFargate(t *testing.T) {
	awsServices := newMockedFakeAWSServices(TestClusterID)
	c, _ := newAWSCloud(config.CloudConfig{}, awsServices)

	instanceExist, err := c.InstanceShutdownByProviderID(context.TODO(), "aws:///us-west-2c/1abc-2def/fargate-192.168.164.88")
	assert.Nil(t, err)
	assert.True(t, instanceExist)
}

func TestInstanceShutdownNotExistsByProviderIDForFargate(t *testing.T) {
	awsServices := newMockedFakeAWSServices(TestClusterID)
	c, _ := newAWSCloud(config.CloudConfig{}, awsServices)

	instanceExist, err := c.InstanceShutdownByProviderID(context.TODO(), "aws:///us-west-2c/1abc-2def/fargate-not-found")
	assert.Nil(t, err)
	assert.False(t, instanceExist)
}

func TestInstanceTypeByProviderIDForFargate(t *testing.T) {
	awsServices := newMockedFakeAWSServices(TestClusterID)
	c, _ := newAWSCloud(config.CloudConfig{}, awsServices)

	instanceType, err := c.InstanceTypeByProviderID(context.TODO(), "aws:///us-west-2c/1abc-2def/fargate-not-found")
	assert.Nil(t, err)
	assert.Equal(t, "", instanceType)
}

func TestGetZoneByProviderIDForFargate(t *testing.T) {
	awsServices := newMockedFakeAWSServices(TestClusterID)
	c, _ := newAWSCloud(config.CloudConfig{}, awsServices)

	zoneDetails, err := c.GetZoneByProviderID(context.TODO(), "aws:///us-west-2c/1abc-2def/fargate-192.168.164.88")
	assert.Nil(t, err)
	assert.Equal(t, "us-west-2c", zoneDetails.FailureDomain)
}

func TestGetRegionFromMetadata(t *testing.T) {
	awsServices := newMockedFakeAWSServices(TestClusterID)
	// Returns region from zone if set
	cfg := config.CloudConfig{}
	cfg.Global.Zone = "us-west-2a"
	region, err := getRegionFromMetadata(context.TODO(), cfg, awsServices.metadata)
	assert.NoError(t, err)
	assert.Equal(t, "us-west-2", region)
	// Returns error if can map to region
	cfg = config.CloudConfig{}
	cfg.Global.Zone = "some-fake-zone"
	_, err = getRegionFromMetadata(context.TODO(), cfg, awsServices.metadata)
	assert.Error(t, err)
	// Returns region from metadata if zone unset
	cfg = config.CloudConfig{}
	region, err = getRegionFromMetadata(context.TODO(), cfg, awsServices.metadata)
	assert.NoError(t, err)
	assert.Equal(t, "us-west-2", region)
}

type MockedEC2API struct {
	EC2API
	mock.Mock
}

func (m *MockedEC2API) DescribeInstances(ctx context.Context, input *ec2.DescribeInstancesInput, optFns ...func(*ec2.Options)) (*ec2.DescribeInstancesOutput, error) {
	args := m.Called(input)
	return args.Get(0).(*ec2.DescribeInstancesOutput), args.Error(1)
}

func (m *MockedEC2API) DescribeInstanceTopology(ctx context.Context, params *ec2.DescribeInstanceTopologyInput, optFns ...func(*ec2.Options)) (*ec2.DescribeInstanceTopologyOutput, error) {
	args := m.Called(ctx, params)
	if args.Get(1) != nil {
		return nil, args.Get(1).(error)
	}
	return args.Get(0).(*ec2.DescribeInstanceTopologyOutput), nil
}

func (m *MockedEC2API) DescribeAvailabilityZones(ctx context.Context, input *ec2.DescribeAvailabilityZonesInput, optFns ...func(*ec2.Options)) (*ec2.DescribeAvailabilityZonesOutput, error) {
	args := m.Called(input)
	return args.Get(0).(*ec2.DescribeAvailabilityZonesOutput), args.Error(1)
}

func newMockedEC2API() *MockedEC2API {
	return &MockedEC2API{}
}

func TestDescribeInstances(t *testing.T) {
	tests := []struct {
		name    string
		input   *ec2.DescribeInstancesInput
		expect  func(EC2API)
		isError bool
	}{
		{
			"MaxResults set on empty DescribeInstancesInput and NextToken respected.",
			&ec2.DescribeInstancesInput{},
			func(mockedEc2 EC2API) {
				m := mockedEc2.(*MockedEC2API)
				m.On("DescribeInstances",
					&ec2.DescribeInstancesInput{
						MaxResults: aws.Int32(1000),
					},
				).Return(
					&ec2.DescribeInstancesOutput{
						NextToken: aws.String("asdf"),
					},
					nil,
				)
				m.On("DescribeInstances",
					&ec2.DescribeInstancesInput{
						MaxResults: aws.Int32(1000),
						NextToken:  aws.String("asdf"),
					},
				).Return(
					&ec2.DescribeInstancesOutput{},
					nil,
				)
			},
			false,
		},
		{
			"MaxResults only set if empty DescribeInstancesInput",
			&ec2.DescribeInstancesInput{
				MaxResults: aws.Int32(3),
			},
			func(mockedEc2 EC2API) {
				m := mockedEc2.(*MockedEC2API)
				m.On("DescribeInstances",
					&ec2.DescribeInstancesInput{
						MaxResults: aws.Int32(3),
					},
				).Return(
					&ec2.DescribeInstancesOutput{},
					nil,
				)
			},
			false,
		},
		{
			"MaxResults not set if instance IDs are provided",
			&ec2.DescribeInstancesInput{
				InstanceIds: []string{"i-1234"},
			},
			func(mockedEc2 EC2API) {
				m := mockedEc2.(*MockedEC2API)
				m.On("DescribeInstances",
					&ec2.DescribeInstancesInput{
						InstanceIds: []string{"i-1234"},
					},
				).Return(
					&ec2.DescribeInstancesOutput{},
					nil,
				)
			},
			false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mockedEC2API := newMockedEC2API()
			test.expect(mockedEC2API)
			fakeEC2 := awsSdkEC2{
				ec2: mockedEC2API,
			}
			_, err := fakeEC2.DescribeInstances(context.TODO(), test.input)
			if !test.isError {
				assert.NoError(t, err)
			}
			mockedEC2API.AssertExpectations(t)
		})
	}
}

func TestInstanceIDIndexFunc(t *testing.T) {
	type args struct {
		obj interface{}
	}
	tests := []struct {
		name    string
		args    args
		want    []string
		wantErr bool
	}{
		{
			name: "returns empty on invalid provider id",
			args: args{
				obj: &v1.Node{
					ObjectMeta: metav1.ObjectMeta{
						Name: "test-node",
					},
					Spec: v1.NodeSpec{
						ProviderID: "foo://com-2351",
					},
				},
			},
			want:    []string{""},
			wantErr: false,
		},
		{
			name: "returns correct instance id on valid provider id",
			args: args{
				obj: &v1.Node{
					ObjectMeta: metav1.ObjectMeta{
						Name: "test-valid-node",
					},
					Spec: v1.NodeSpec{
						ProviderID: "aws:////i-12345678abcdef01",
					},
				},
			},
			want:    []string{"i-12345678abcdef01"},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := InstanceIDIndexFunc(tt.args.obj)
			if (err != nil) != tt.wantErr {
				t.Errorf("InstanceIDIndexFunc() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("InstanceIDIndexFunc() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsAWSErrorInstanceNotFound(t *testing.T) {
	mockedEC2API := newMockedEC2API()
	ec2Client := &awsSdkEC2{
		ec2: mockedEC2API,
	}

	// API error
	mockedEC2API.On("DescribeInstances", mock.Anything).Return(&ec2.DescribeInstancesOutput{}, error(&smithy.GenericAPIError{
		Code:    string(ec2types.UnsuccessfulInstanceCreditSpecificationErrorCodeInstanceNotFound),
		Message: "test",
	}))
	_, err := ec2Client.ec2.DescribeInstances(context.Background(), &ec2.DescribeInstancesInput{})
	assert.True(t, IsAWSErrorInstanceNotFound(err))

	// Wrapped error
	_, err = ec2Client.ec2.DescribeInstances(context.Background(), &ec2.DescribeInstancesInput{})
	err = fmt.Errorf("error listing AWS instances: %q", err)
	assert.True(t, IsAWSErrorInstanceNotFound(err))

	// Expect false for nil and any other errors
	assert.False(t, IsAWSErrorInstanceNotFound(nil))

	mockedEC2API.On("DescribeInstances", mock.Anything).Return(&ec2.DescribeInstancesInput{}, &smithy.GenericAPIError{
		Code: string(ec2types.UnsuccessfulInstanceCreditSpecificationErrorCodeIncorrectInstanceState),
	})
	_, err = ec2Client.ec2.DescribeInstances(context.Background(), &ec2.DescribeInstancesInput{})
	assert.False(t, IsAWSErrorInstanceNotFound(nil))
}
