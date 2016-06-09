package api

import (
	"fmt"
	"net/http"

	"github.com/yanzay/log"
)

type UserRequest struct {
	Name  string `json:"name"`
	Admin bool   `json:"admin"`
}

func (as *ApiServer) clientFromRequest(r *http.Request) (string, error) {
	if as.storage.Empty() {
		log.Info("Storage is empty")
		return "admin", nil
	}
	token := r.Header.Get("X-Api-Key")
	if token == "" {
		return "", fmt.Errorf("Token required")
	}
	user, found := as.storage.UserByToken(token)
	if !found {
		return "", fmt.Errorf("User with token %s not found", token)
	}
	return user.Name, nil
}
