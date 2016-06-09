package api

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/julienschmidt/httprouter"
)

func (as *ApiServer) GetSchema(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	client := ps.ByName("client")
	idStr := ps.ByName("id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		registryError(w, ErrDecoding, http.StatusBadRequest, err)
	}
	schema, found, err := as.storage.GetSchemaByID(client, id)
	if err != nil {
		registryError(w, ErrInBackendStore, http.StatusInternalServerError, err)
		return
	}
	if !found {
		registryError(w, ErrSchemaNotFound, http.StatusNotFound, err)
		return
	}
	w.Header().Add("Content-Type", "application/vnd.schemaregistry.v1+json")
	message := SchemaMessage{schema}
	encoder := json.NewEncoder(w)
	err = encoder.Encode(message)
	if err != nil {
		registryError(w, ErrInBackendStore, http.StatusInternalServerError, err)
		return
	}
}
