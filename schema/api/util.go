package api

import (
	"encoding/json"
	"net/http"

	"github.com/yanzay/log"
)

const (
	ErrSchemaNotFound       = "Schema not found"
	ErrInBackendStore       = "Error in the backend datastore"
	ErrAuthStore            = "Error in authorization backend"
	ErrEncoding             = "Error encoding response"
	ErrDecoding             = "Error decoding request"
	ErrSubjectNotFound      = "Subject not found"
	ErrInvalidSchema        = "Invalid Avro schema"
	ErrIncompatibleSchema   = "Incompatible Avro schema"
	ErrInvalidCompatibility = "Invalid compatibility level"
	ErrUnauthorized         = "Client authorization required"
	ErrUserExists           = "User already exists"
)

type ErrorMessage struct {
	ErrorCode int    `json:"error_code"`
	Message   string `json:"message"`
}

func registryError(w http.ResponseWriter, errorMessage string, code int, err error) {
	if err != nil {
		log.Error(err)
	}
	log.Warningf("Registry error: %s, %s", errorMessage, err)
	w.WriteHeader(code)
	mes := &ErrorMessage{
		ErrorCode: code,
		Message:   errorMessage,
	}
	encoder := json.NewEncoder(w)
	err = encoder.Encode(mes)
	if err != nil {
		log.Errorf("Can't respond with error: %s\n", err)
	}
}
