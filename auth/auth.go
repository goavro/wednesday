package auth

import (
	uuid "github.com/satori/go.uuid"
	"github.com/yanzay/log"
)

var Storage AuthStorage = NewAuthInMemoryStorage()

func InitStorage(vaultUrl string, vaultToken string) {
	if vaultUrl != "" && vaultToken != "" {
		Storage = NewAuthVaultStorage(vaultUrl)
	} else {
		log.Infof("Vault URL (%s) of VAULT_TOKEN (%s) is empty, continue with in memory storage", vaultUrl, vaultToken)
	}
}

func Authorize(name string, token string) (bool, error) {
	return Storage.Authorize(name, token)
}

func AddUser(name string, admin bool) (string, error) {
	return Storage.AddUser(name, admin)
}

func IsAdmin(name string) (bool, error) {
	return Storage.IsAdmin(name)
}

func RefreshToken(name string) (string, error) {
	return Storage.RefreshToken(name)
}

func generateApiKey() string {
	return uuid.NewV4().String()
}
