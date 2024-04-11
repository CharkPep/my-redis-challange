package storage

// problem with redis implementation of db that each key has associated type
// (each key has to include type information to prevent from two keys with different types to collide in one db idx)
// there is not much of what we can do but something like this
// data types storage
// t[k] -> data type
// s[k] -> data type storage
// data type storage
// d[k] -> v
// problems
// (1) value expiry, sync value between concrete data type db and global types db - solution proxy each request
// (2) separate data type specific implementation from global type - solution proxy
// (3) lots of mutexes

type TypedStorage interface {
	GetType() StorageType
}

type RedisDataTypes struct {
	index     int
	keyTypes  *keyTypeMap
	dataTypes map[StorageType]TypedStorage
}

func NewDb(idx int) *RedisDataTypes {
	kType := newKeyType()
	return &RedisDataTypes{
		index:    idx,
		keyTypes: kType,
		dataTypes: map[StorageType]TypedStorage{
			STREAMS: NewStreamStorage(),
			STRINGS: &StringsProxy{
				keyTypes: kType,
				storage:  NewStringsStorage(),
			},
		},
	}
}

func (db RedisDataTypes) GetType(key string) StorageType {
	return db.keyTypes.GetType(key)
}

func (db RedisDataTypes) GetStorage(t StorageType) TypedStorage {
	return db.dataTypes[t]
}
