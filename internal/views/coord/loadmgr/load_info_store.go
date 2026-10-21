package loadmgr

import (
	"context"
	"strconv"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/kv/queryview"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/lock"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type loadInfoKey struct {
	collectionID int64
	version      uint64
}

// lockKey is ordered so batch recovery can use lock.LockManyOrdered.
func (k loadInfoKey) lockKey() string {
	return strconv.FormatInt(k.collectionID, 10) + "/" + strconv.FormatUint(k.version, 10)
}

// LoadInfoStore persists exact field/partition scopes independently of the latest
// desired configuration. Index descriptors are resolved from current metadata
// on every read; a QueryView does not retain an index identity.
// There is no history deletion in this refactor.
type LoadInfoStore struct {
	catalog queryview.LoadInfoCatalog
	indexes func(int64) []*indexpb.IndexInfo
	schema  func(context.Context, int64) (*schemapb.CollectionSchema, error)
	locks   *lock.KeyLock[string]
	cache   sync.Map
}

func NewLoadInfoStore(catalog queryview.LoadInfoCatalog, indexes func(int64) []*indexpb.IndexInfo, schema ...func(context.Context, int64) (*schemapb.CollectionSchema, error)) *LoadInfoStore {
	s := &LoadInfoStore{catalog: catalog, indexes: indexes, locks: lock.NewKeyLock[string]()}
	if len(schema) > 0 {
		s.schema = schema[0]
	}
	return s
}

func (s *LoadInfoStore) Get(ctx context.Context, collectionID int64, version uint64) (*querypb.GetQueryViewLoadInfoResponse, error) {
	key := loadInfoKey{collectionID, version}
	if cached, ok := s.cache.Load(key); ok {
		return s.withCurrentIndexes(cached.(*querypb.GetQueryViewLoadInfoResponse)), nil
	}
	info, err := s.catalog.GetLoadInfo(ctx, collectionID, version)
	if err != nil {
		return nil, err
	}
	if info == nil {
		return nil, merr.WrapErrServiceNotReadyMsg("query view load snapshot %d for collection %d is unavailable", version, collectionID)
	}
	s.cache.Store(key, info)
	return s.withCurrentIndexes(info), nil
}

// Ensure persists a snapshot before its config can drive creation of a view.
// Only the field/partition scope is retained; indexes are resolved when read.
func (s *LoadInfoStore) Ensure(ctx context.Context, cfg *LoadConfig) error {
	key := loadInfoKey{cfg.CollectionID, loadInfoVersion(cfg)}
	lockKey := key.lockKey()
	s.locks.Lock(lockKey)
	defer s.locks.Unlock(lockKey)
	if _, ok := s.cache.Load(key); ok {
		return nil
	}
	existing, err := s.catalog.GetLoadInfo(ctx, key.collectionID, key.version)
	if err != nil {
		return err
	}
	if existing != nil {
		s.cache.Store(key, existing)
		return nil
	}
	info := newLoadInfoSnapshot(cfg, key.version)
	if err := s.catalog.SaveLoadInfo(ctx, info); err != nil {
		return err
	}
	s.cache.Store(key, info)
	return nil
}

// EnsureAll is Ensure for many configs with batched catalog I/O. Recovery
// ensures every loaded collection; one round trip per collection took minutes
// at hundreds of thousands of collections.
func (s *LoadInfoStore) EnsureAll(ctx context.Context, cfgs []*LoadConfig) error {
	pending := make(map[loadInfoKey]*LoadConfig, len(cfgs))
	for _, cfg := range cfgs {
		key := loadInfoKey{cfg.CollectionID, loadInfoVersion(cfg)}
		if _, ok := s.cache.Load(key); !ok {
			pending[key] = cfg
		}
	}
	if len(pending) == 0 {
		return nil
	}
	lockKeys := make([]string, 0, len(pending))
	for key := range pending {
		lockKeys = append(lockKeys, key.lockKey())
	}
	lock.LockManyOrdered(s.locks, lockKeys)
	defer s.locks.UnlockMany(lockKeys)

	keys := make([]loadInfoKey, 0, len(pending))
	ids := make([]queryview.LoadInfoID, 0, len(pending))
	for key := range pending {
		// A concurrent Ensure may have finished while this call waited.
		if _, ok := s.cache.Load(key); !ok {
			keys = append(keys, key)
			ids = append(ids, queryview.LoadInfoID{CollectionID: key.collectionID, Version: key.version})
		}
	}
	if len(keys) == 0 {
		return nil
	}
	existing, err := s.catalog.GetLoadInfos(ctx, ids)
	if err != nil {
		return err
	}
	missing := make([]*querypb.GetQueryViewLoadInfoResponse, 0)
	for i, key := range keys {
		if existing[i] != nil {
			s.cache.Store(key, existing[i])
			continue
		}
		missing = append(missing, newLoadInfoSnapshot(pending[key], key.version))
	}
	if len(missing) == 0 {
		return nil
	}
	if err := s.catalog.SaveLoadInfos(ctx, missing); err != nil {
		return err
	}
	for _, info := range missing {
		s.cache.Store(loadInfoKey{info.GetCollectionID(), info.GetVersion()}, info)
	}
	return nil
}

func newLoadInfoSnapshot(cfg *LoadConfig, version uint64) *querypb.GetQueryViewLoadInfoResponse {
	owned := cfg.Clone()
	info := &querypb.GetQueryViewLoadInfoResponse{Status: merr.Success(), CollectionID: cfg.CollectionID, Version: version, PartitionIDs: owned.PartitionIDs, LoadFields: owned.LoadFields, RequiredFields: owned.RequiredFields, SkipLoadDynamicField: owned.SkipLoadDynamicField}
	for _, fields := range [][]*messagespb.LoadFieldConfig{info.LoadFields, info.RequiredFields} {
		for _, field := range fields {
			field.IndexId = 0
		}
	}
	return info
}

// QueryViews retain loading scope, never an index identity. IndexID zero means
// the newest available index; no index metadata is persisted in this history.
func (s *LoadInfoStore) withCurrentIndexes(stored *querypb.GetQueryViewLoadInfoResponse) *querypb.GetQueryViewLoadInfoResponse {
	info := proto.Clone(stored).(*querypb.GetQueryViewLoadInfoResponse)
	fields := make(map[int64]struct{}, len(info.RequiredFields))
	for _, field := range info.RequiredFields {
		field.IndexId = 0
		fields[field.FieldId] = struct{}{}
	}
	info.IndexInfoList = nil
	for _, index := range s.indexes(info.CollectionID) {
		if _, loaded := fields[index.GetFieldID()]; loaded || len(fields) == 0 {
			info.IndexInfoList = append(info.IndexInfoList, proto.Clone(index).(*indexpb.IndexInfo))
		}
	}
	return info
}

// normalize materializes legacy required scopes before desired state publication.
// Historical snapshots are resolved against their View schema by the worker.
func (s *LoadInfoStore) normalize(ctx context.Context, cfg *LoadConfig) (*LoadConfig, error) {
	cfg = cfg.Clone()
	if len(cfg.RequiredFields) == 0 && s.schema != nil {
		schema, err := s.schema(ctx, cfg.CollectionID)
		if err != nil {
			return nil, err
		}
		cfg.RequiredFields = RequiredFieldsForSchema(schema)
		if len(cfg.RequiredFields) == 0 {
			return nil, merr.WrapErrServiceInternalMsg("collection %d has no required query fields", cfg.CollectionID)
		}
	}
	if cfg.LoadAllFields != nil && *cfg.LoadAllFields {
		cfg.LoadFields = nil
	}
	return cfg, nil
}
