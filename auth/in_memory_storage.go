package auth

import (
	"fmt"
	"sync"
)

type AuthInMemoryStorage struct {
	sync.RWMutex
	users map[string]*User
}

func NewAuthInMemoryStorage() *AuthInMemoryStorage {
	return &AuthInMemoryStorage{users: make(map[string]*User)}
}

func (ams *AuthInMemoryStorage) Authorize(name string, token string) (bool, error) {
	ams.RLock()
	defer ams.RUnlock()

	return ams.users[name].Token == token, nil
}

func (ams *AuthInMemoryStorage) AddUser(name string, admin bool) (string, error) {
	ams.Lock()
	defer ams.Unlock()

	token := generateApiKey()
	ams.users[name] = &User{Name: name, Token: token, Admin: admin}
	return token, nil
}

func (ams *AuthInMemoryStorage) RefreshToken(name string) (string, error) {
	ams.Lock()
	defer ams.Unlock()

	if ams.users[name] == nil {
		return "", fmt.Errorf("User %s does not exist", name)
	}
	ams.users[name].Token = generateApiKey()
	return ams.users[name].Token, nil
}

func (ams *AuthInMemoryStorage) IsAdmin(name string) (bool, error) {
	ams.RLock()
	defer ams.RUnlock()

	if ams.users[name] == nil {
		return false, fmt.Errorf("User %s does not exist", name)
	}
	return ams.users[name].Admin, nil
}
