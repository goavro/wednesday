package client

import "testing"

var emptyDescribeGroupsRequestBytes = []byte{0x00, 0x00, 0x00, 0x00}
var goodDescribeGroupsRequestBytes = []byte{0x00, 0x00, 0x00, 0x01, 0x00, 0x03, 'f', 'o', 'o'}

var emptyDescribeGroupsResponseBytes = []byte{0x00, 0x00, 0x00, 0x00}
var goodDescribeGroupsResponseBytes = []byte{0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x03, 'f', 'o', 'o', 0x00, 0x02, 'o', 'k', 0x00, 0x08, 'c', 'o', 'n', 's', 'u', 'm', 'e', 'r', 0x00, 0x01, 'x', 0x00, 0x00, 0x00, 0x01, 0x00, 0x03, 'b', 'a', 'r', 0x00, 0x04, 't', 'e', 's', 't', 0x00, 0x04, 'h', 'o', 's', 't', 0x00, 0x00, 0x00, 0x01, 0x01, 0x00, 0x00, 0x00, 0x01, 0x02}

func TestDescribeGroupsRequest(t *testing.T) {
	emptyRequest := new(DescribeGroupsRequest)
	testRequest(t, emptyRequest, emptyDescribeGroupsRequestBytes)

	goodRequest := new(DescribeGroupsRequest)
	goodRequest.Groups = []string{"foo"}
	testRequest(t, goodRequest, goodDescribeGroupsRequestBytes)
}

func TestDescribeGroupsResponse(t *testing.T) {
	emptyResponse := new(DescribeGroupsResponse)
	decode(t, emptyResponse, emptyDescribeGroupsResponseBytes)
	assert(t, len(emptyResponse.Groups), 0)

	goodResponse := new(DescribeGroupsResponse)
	decode(t, goodResponse, goodDescribeGroupsResponseBytes)
	assert(t, len(goodResponse.Groups), 1)
	group := goodResponse.Groups[0]
	assert(t, group.Error, ErrNoError)
	assert(t, group.GroupID, "foo")
	assert(t, group.State, "ok")
	assert(t, group.ProtocolType, "consumer")
	assert(t, group.Protocol, "x")

	assert(t, len(group.Members), 1)
	member := group.Members[0]
	assert(t, member.ClientHost, "host")
	assert(t, member.ClientID, "test")
	assert(t, member.MemberID, "bar")
	assert(t, member.MemberMetadata, []byte{0x01})
	assert(t, member.MemberAssignment, []byte{0x02})
}
