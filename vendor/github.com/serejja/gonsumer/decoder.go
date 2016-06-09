package gonsumer

// Decoder serves to decode given raw message bytes to something meaningful.
// Returns an error if fails to decoder given bytes.
type Decoder func(raw []byte) (interface{}, error)

// ByteDecoder is a default decoder implementation that does nothing and just returns the input untouched.
// Never returns an error.
var ByteDecoder = func(bytes []byte) (interface{}, error) {
	return bytes, nil
}

// StringDecoder converts the given bytes into a string. Never returns an error.
var StringDecoder = func(bytes []byte) (interface{}, error) {
	return string(bytes), nil
}
