package client

import "testing"

var emptyJoinGroupRequestBytes = []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
var joinGroupRequestWithProtocolsBytes = []byte{0x00, 0x05, 'g', 'r', 'o', 'u', 'p', 0x00, 0x00, 0x00, 50, 0x00, 0x06, 'm', 'e', 'm', 'b', 'e', 'r', 0x00, 0x08, 'c', 'o', 'n', 's', 'u', 'm', 'e', 'r', 0x00, 0x00, 0x00, 0x02, 0x00, 0x03, 'f', 'o', 'o', 0x00, 0x00, 0x00, 0x01, 0x01, 0x00, 0x03, 'b', 'a', 'r', 0x00, 0x00, 0x00, 0x01, 0x02}

var goodJoinGroupResponseBytes = []byte{0x00, 0x00, 0x00, 0x00, 0x00, 132, 0x00, 0x05, 'p', 'r', 'o', 't', 'o', 0x00, 0x03, 'f', 'o', 'o', 0x00, 0x03, 'b', 'a', 'r', 0x00, 0x00, 0x00, 0x01, 0x00, 0x03, 'f', 'o', 'o', 0x00, 0x00, 0x00, 0x01, 0x01}
var errorJoinGroupResponseBytes = []byte{0x00, 23, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}

func TestJoinGroupRequest(t *testing.T) {
	emptyJoinGroupRequest := new(JoinGroupRequest)
	testRequest(t, emptyJoinGroupRequest, emptyJoinGroupRequestBytes)

	joinGroupRequestWithProtocols := new(JoinGroupRequest)
	joinGroupRequestWithProtocols.GroupID = "group"
	joinGroupRequestWithProtocols.SessionTimeout = 50
	joinGroupRequestWithProtocols.MemberID = "member"
	joinGroupRequestWithProtocols.ProtocolType = "consumer"
	joinGroupRequestWithProtocols.GroupProtocols = []*GroupProtocol{
		{
			ProtocolName:     "foo",
			ProtocolMetadata: []byte{0x01},
		},
		{
			ProtocolName:     "bar",
			ProtocolMetadata: []byte{0x02},
		},
	}
	testRequest(t, joinGroupRequestWithProtocols, joinGroupRequestWithProtocolsBytes)
}

func TestJoinGroupResponse(t *testing.T) {
	goodJoinGroupResponse := new(JoinGroupResponse)
	decode(t, goodJoinGroupResponse, goodJoinGroupResponseBytes)
	assert(t, goodJoinGroupResponse.Error, ErrNoError)
	assert(t, goodJoinGroupResponse.GenerationID, int32(132))
	assert(t, goodJoinGroupResponse.GroupProtocol, "proto")
	assert(t, goodJoinGroupResponse.LeaderID, "foo")
	assert(t, goodJoinGroupResponse.MemberID, "bar")
	assert(t, len(goodJoinGroupResponse.Members), 1)

	errorJoinGroupResponse := new(JoinGroupResponse)
	decode(t, errorJoinGroupResponse, errorJoinGroupResponseBytes)
	assert(t, errorJoinGroupResponse.Error, ErrInconsistentGroupProtocol)
}
