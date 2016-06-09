package storage

type StorageMultiwriter struct {
	kafkaWriter     StorageWriter
	cassandraWriter StorageStateWriter
}

func NewStorageMultiwriter(kafkaStorage StorageWriter, cassandraStorage StorageStateWriter) *StorageMultiwriter {
	return &StorageMultiwriter{
		kafkaWriter:     kafkaStorage,
		cassandraWriter: cassandraStorage,
	}
}

func (sm *StorageMultiwriter) StoreSchema(client string, subject string, schema string) (int64, error) {
	id, err := sm.kafkaWriter.StoreSchema(client, subject, schema)
	if err != nil {
		return -1, err
	}
	err = sm.cassandraWriter.AddSchema(client, subject, id, schema)
	return id, err
}

func (sm *StorageMultiwriter) UpdateGlobalConfig(client string, config CompatibilityConfig) error {
	err := sm.kafkaWriter.UpdateGlobalConfig(client, config)
	if err != nil {
		return err
	}
	return sm.cassandraWriter.SetGlobalConfig(client, config.Compatibility)
}

func (sm *StorageMultiwriter) UpdateSubjectConfig(client string, subject string, config CompatibilityConfig) error {
	err := sm.kafkaWriter.UpdateSubjectConfig(client, subject, config)
	if err != nil {
		return err
	}
	return sm.cassandraWriter.SetSubjectConfig(client, subject, config.Compatibility)
}

func (sm *StorageMultiwriter) CreateUser(name string, token string, admin bool) (string, error) {
	return sm.kafkaWriter.CreateUser(name, token, admin)
}
