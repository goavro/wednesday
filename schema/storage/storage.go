package storage

type Schema struct {
	Subject string `json:"subject"`
	ID      int64  `json:"id"`
	Version int    `json:"version"`
	Schema  string `json:"schema"`
}

const (
	CompatibilityNone     = "NONE"
	CompatibilityFull     = "FULL"
	CompatibilityForward  = "FORWARD"
	CompatibilityBackward = "BACKWARD"
)

type CompatibilityConfig struct {
	Compatibility string `json:"compatibility"`
}

type Storage interface {
	StorageStateReader
	StorageStateWriter
	StorageWriter
}

type CombinedStorage struct {
	StorageStateReader
	StorageStateWriter
	StorageWriter
}

type StorageWriter interface {
	StoreSchema(string, string, string) (int64, error)

	UpdateGlobalConfig(string, CompatibilityConfig) error
	UpdateSubjectConfig(string, string, CompatibilityConfig) error

	CreateUser(string, string, bool) (string, error)
}

type StorageStater interface {
	StorageStateWriter
	StorageStateReader
}

type StorageStateReader interface {
	Empty() bool

	GetID(client string, schema string) int64

	GetSchemaByID(string, int64) (string, bool, error)
	GetSubjects(string) ([]string, error)
	GetVersions(string, string) ([]int, bool, error)
	GetSchema(string, string, int) (string, bool, error)
	GetLatestSchema(string, string) (*Schema, bool, error)

	GetGlobalConfig(string) (string, error)
	GetSubjectConfig(string, string) (string, bool, error)

	UserByName(string) (*User, bool)
	UserByToken(string) (*User, bool)
}

type StorageStateWriter interface {
	AddSchema(string, string, int64, string) error
	SetGlobalConfig(string, string) error
	SetSubjectConfig(string, string, string) error
	AddUser(string, string, bool) error
}

type User struct {
	Name  string
	Token string
	Admin bool
}
