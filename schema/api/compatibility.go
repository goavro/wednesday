package api

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/julienschmidt/httprouter"
)

type CompatibilityMessage struct {
	IsCompatible bool `json:"is_compatible"`
}

func (as *ApiServer) CheckCompatibility(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	client := ps.ByName("client")
	subject := ps.ByName("subject")
	versionStr := ps.ByName("version")
	version, err := strconv.Atoi(versionStr)
	if err != nil {
		registryError(w, ErrDecoding, http.StatusBadRequest, err)
		return
	}

	defer r.Body.Close()
	var schema SchemaMessage
	decoder := json.NewDecoder(r.Body)
	err = decoder.Decode(&schema)
	if err != nil || !schemaValid(schema.Schema) {
		registryError(w, ErrInvalidSchema, 422, err)
	}

	oldSchema, found, err := as.storage.GetSchema(client, subject, version)
	if err != nil {
		registryError(w, ErrInBackendStore, http.StatusInternalServerError, err)
		return
	}

	if !found {
		registryError(w, ErrSchemaNotFound, http.StatusNotFound, err)
		return
	}

	compatibility, found, _ := as.storage.GetSubjectConfig(client, subject)
	if !found {
		compatibility, _ = as.storage.GetGlobalConfig(client)
	}
	resp := CompatibilityMessage{
		IsCompatible: schemaCompatible(schema.Schema, oldSchema, compatibility), //TODO compatibility
	}
	encoder := json.NewEncoder(w)
	err = encoder.Encode(resp)
	if err != nil {
		registryError(w, ErrEncoding, http.StatusInternalServerError, err)
	}
}
