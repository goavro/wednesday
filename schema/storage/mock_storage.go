package storage

type MockStorageWriter struct {
}

func (*MockStorageWriter) StoreSchema(string, string, string) (int64, error) {
	return 0, nil
}

func (*MockStorageWriter) UpdateGlobalConfig(string, CompatibilityConfig) error {
	return nil
}
func (*MockStorageWriter) UpdateSubjectConfig(string, string, CompatibilityConfig) error {
	return nil
}

func (*MockStorageWriter) CreateUser(string, string, bool) (string, error) {
	return "", nil
}
