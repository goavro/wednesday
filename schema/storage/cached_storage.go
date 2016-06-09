package storage

type CachedStorage struct {
	StorageWriter
	StorageStateWriter
	Cache   StorageStateReader
	Backend StorageStater
}

func (cs *CachedStorage) Empty() bool {
	return cs.Cache.Empty() && cs.Backend.Empty()
}

func (cs *CachedStorage) GetID(client string, schema string) int64 {
	id := cs.Cache.GetID(client, schema)
	if id == -1 {
		return cs.Backend.GetID(client, schema)
	}
	return id
}

func (cs *CachedStorage) GetSchemaByID(client string, id int64) (string, bool, error) {
	schema, found, err := cs.Cache.GetSchemaByID(client, id)
	if !found || err != nil {
		return cs.Backend.GetSchemaByID(client, id)
	}
	return schema, found, err
}

func (cs *CachedStorage) GetSubjects(client string) ([]string, error) {
	subjects, err := cs.Cache.GetSubjects(client)
	if err != nil {
		return cs.Backend.GetSubjects(client)
	}
	return subjects, nil
}

func (cs *CachedStorage) GetVersions(client string, subject string) ([]int, bool, error) {
	versions, found, err := cs.Cache.GetVersions(client, subject)
	if !found || err != nil {
		return cs.Backend.GetVersions(client, subject)
	}
	return versions, found, err
}

func (cs *CachedStorage) GetSchema(client string, subject string, version int) (string, bool, error) {
	schema, found, err := cs.Cache.GetSchema(client, subject, version)
	if !found || err != nil {
		return cs.Backend.GetSchema(client, subject, version)
	}
	return schema, found, err
}

func (cs *CachedStorage) GetLatestSchema(client string, subject string) (*Schema, bool, error) {
	schema, found, err := cs.Cache.GetLatestSchema(client, subject)
	if !found || err != nil {
		return cs.Backend.GetLatestSchema(client, subject)
	}
	return schema, found, err
}

func (cs *CachedStorage) GetGlobalConfig(client string) (string, error) {
	level, err := cs.Cache.GetGlobalConfig(client)
	if err != nil {
		return cs.Backend.GetGlobalConfig(client)
	}
	return level, err
}

func (cs *CachedStorage) GetSubjectConfig(client string, subject string) (string, bool, error) {
	level, found, err := cs.Cache.GetSubjectConfig(client, subject)
	if !found || err != nil {
		return cs.Backend.GetSubjectConfig(client, subject)
	}
	return level, found, err
}

func (cs *CachedStorage) UserByName(name string) (*User, bool) {
	return cs.Cache.UserByName(name)
}
func (cs *CachedStorage) UserByToken(token string) (*User, bool) {
	return cs.Cache.UserByToken(token)
}
