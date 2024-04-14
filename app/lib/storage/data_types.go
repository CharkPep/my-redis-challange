package storage

// problem with redis implementation of db that each Key has associated type
// (each Key has to include type information to prevent from two keys with different types to collide in one db idx)
// there is not much of what we can do but something like this
// Data types stream
// t[k] -> Data type
// s[k] -> Data type stream
// Data type stream
// d[k] -> v
// problems
// (1) value expiry, sync value between concrete Data type db and global types db - solution proxy each request
// (2) separate Data type specific implementation from global type - solution proxy
// (3) lots of mutexes

type TypedStorage interface {
	GetType() DataType
}

type RedisDataTypes struct {
	index     int
	keyTypes  *keyTypeMap
	dataTypes map[DataType]TypedStorage
}

func NewDb(idx int) *RedisDataTypes {
	kType := newKeyType()
	return &RedisDataTypes{
		index:    idx,
		keyTypes: kType,
		dataTypes: map[DataType]TypedStorage{
			STREAMS: NewStreamIdx(kType),
			STRINGS: &StringsProxy{
				keyTypes: kType,
				storage:  NewStringsStorage(),
			},
		},
	}
}

func (db RedisDataTypes) GetType(key string) DataType {
	return db.keyTypes.GetType(key)
}

func (db RedisDataTypes) GetStorage(t DataType) TypedStorage {
	return db.dataTypes[t]
}
