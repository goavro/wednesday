package auth

import (
	"fmt"
	"os"

	vault "github.com/hashicorp/vault/api"
	"github.com/yanzay/log"
)

type AuthVaultStorage struct {
	vault *vault.Logical
}

func NewAuthVaultStorage(url string) *AuthVaultStorage {
	avs := &AuthVaultStorage{}
	vaultConfig := vault.DefaultConfig()
	vaultConfig.Address = url

	client, err := vault.NewClient(vaultConfig)
	if err != nil {
		log.Fatalf("Error connecting to Vault: %s", err)
	}
	token := os.Getenv("VAULT_TOKEN")
	if token == "" {
		log.Fatal("VAULT_TOKEN should not be empty")
	}
	client.SetToken(token)
	avs.vault = client.Logical()
	return avs
}

func (avs *AuthVaultStorage) Authorize(name string, token string) (bool, error) {
	secret, err := avs.vault.Read(pathForUser(name))
	if err != nil || secret == nil {
		return false, fmt.Errorf("Can't get the secret for user %s: %s", name, err)
	}
	log.Infof("[AuthVaultStorage] Secret: %v", secret)
	log.Infof("[AuthVaultStorage] Data: %v", secret.Data)
	return secret.Data["token"] == token, nil
}

func (avs *AuthVaultStorage) AddUser(name string, admin bool) (string, error) {
	apiKey := generateApiKey()
	data := map[string]interface{}{"token": apiKey, "admin": admin}
	_, err := avs.vault.Write(pathForUser(name), data)
	if err != nil {
		log.Errorf("Can't create user %s: %s", name, err)
		return "", err
	}
	return apiKey, nil
}

func (avs *AuthVaultStorage) RefreshToken(name string) (string, error) {
	apiKey := generateApiKey()
	secret, err := avs.vault.Read(pathForUser(name))
	if err != nil {
		return "", err
	}
	secret.Data["token"] = apiKey
	_, err = avs.vault.Write(pathForUser(name), secret.Data)
	if err != nil {
		log.Errorf("Can't write new token for user %s: %s", name, err)
		return "", err
	}
	return apiKey, nil
}

func (avs *AuthVaultStorage) IsAdmin(name string) (bool, error) {
	secret, err := avs.vault.Read(pathForUser(name))
	if err != nil {
		return false, err
	}
	return secret.Data["admin"].(bool), nil
}

func pathForUser(username string) string {
	return fmt.Sprintf("secret/token/%s", username)
}
