package gonsumer

import (
	"gopkg.in/stretchr/testify.v1/assert"
	"testing"
)

func TestDecoders(t *testing.T) {
	// normal bytes
	bytes := []byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05}
	decodedBytes, err := ByteDecoder(bytes)

	assert.Equal(t, nil, err)
	assert.Equal(t, bytes, decodedBytes)

	// empty bytes
	bytes = nil
	decodedBytes, err = ByteDecoder(bytes)

	assert.Equal(t, nil, err)
	assert.Equal(t, bytes, decodedBytes)

	// normal string
	str := "hello world"
	decodedString, err := StringDecoder([]byte(str))

	assert.Equal(t, nil, err)
	assert.Equal(t, str, decodedString)

	// empty string
	str = ""
	decodedString, err = StringDecoder([]byte(str))

	assert.Equal(t, nil, err)
	assert.Equal(t, str, decodedString)
}
