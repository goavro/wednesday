package auth

type AuthStorage interface {
	Authorize(string, string) (bool, error)
	AddUser(string, bool) (string, error)
	IsAdmin(string) (bool, error)
	RefreshToken(string) (string, error)
}
