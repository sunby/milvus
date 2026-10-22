package queryview

import (
	"context"
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/kv"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// LoadInfoCatalog retains immutable loading requirements referenced by views.
// Historical snapshot GC is intentionally deferred.
type LoadInfoCatalog interface {
	GetLoadInfo(context.Context, int64, uint64) (*querypb.GetQueryViewLoadInfoResponse, error)
	SaveLoadInfo(context.Context, *querypb.GetQueryViewLoadInfoResponse) error
	// GetLoadInfos returns one snapshot per ID in input order; nil marks a
	// missing snapshot.
	GetLoadInfos(context.Context, []LoadInfoID) ([]*querypb.GetQueryViewLoadInfoResponse, error)
	// SaveLoadInfos persists snapshots in store-sized transactions. Each
	// transaction is atomic; the whole call is not.
	SaveLoadInfos(context.Context, []*querypb.GetQueryViewLoadInfoResponse) error
}

// LoadInfoID identifies one retained load snapshot.
type LoadInfoID struct {
	CollectionID int64
	Version      uint64
}

type loadInfoCatalog struct{ storage kv.MetaKv }

func NewLoadInfoCatalog(storage kv.MetaKv) LoadInfoCatalog {
	return &loadInfoCatalog{storage: kv.NewReliableWriteMetaKv(storage)}
}

func loadInfoKey(collectionID int64, version uint64) string {
	return fmt.Sprintf("coord/qv-load-info/%d/%d", collectionID, version)
}

func (c *loadInfoCatalog) GetLoadInfo(ctx context.Context, collectionID int64, version uint64) (*querypb.GetQueryViewLoadInfoResponse, error) {
	value, err := c.storage.Load(ctx, loadInfoKey(collectionID, version))
	if errors.Is(err, merr.ErrIoKeyNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return decodeLoadInfo(value, LoadInfoID{collectionID, version})
}

func (c *loadInfoCatalog) SaveLoadInfo(ctx context.Context, info *querypb.GetQueryViewLoadInfoResponse) error {
	value, err := proto.Marshal(info)
	if err != nil {
		return merr.Wrap(err, "encode query view load snapshot")
	}
	return c.storage.Save(ctx, loadInfoKey(info.GetCollectionID(), info.GetVersion()), string(value))
}

// GetLoadInfos reads store-sized batches concurrently, bounded by
// metastore.readConcurrency.
func (c *loadInfoCatalog) GetLoadInfos(ctx context.Context, ids []LoadInfoID) ([]*querypb.GetQueryViewLoadInfoResponse, error) {
	infos := make([]*querypb.GetQueryViewLoadInfoResponse, len(ids))
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(max(paramtable.Get().MetaStoreCfg.ReadConcurrency.GetAsInt(), 1))
	for _, batch := range lo.Chunk(lo.Range(len(ids)), max(c.storage.MaxTxnOps(), 1)) {
		g.Go(func() error {
			keys := lo.Map(batch, func(i int, _ int) string { return loadInfoKey(ids[i].CollectionID, ids[i].Version) })
			values, err := c.storage.MultiLoad(ctx, keys)
			if err != nil && !errors.Is(err, merr.ErrIoKeyNotFound) {
				return err
			}
			if len(values) != len(keys) {
				return merr.WrapErrServiceInternalMsg("metastore returned %d values for %d query view load snapshot keys", len(values), len(keys))
			}
			for j, i := range batch {
				// MultiLoad reports a missing key as an empty value; an encoded
				// snapshot is never empty because it carries its identity.
				if values[j] == "" {
					continue
				}
				info, err := decodeLoadInfo(values[j], ids[i])
				if err != nil {
					return err
				}
				infos[i] = info
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return infos, nil
}

// maxLoadInfoTxnBytes keeps a batch well below etcd's default 1.5 MiB request
// limit; a snapshot carries every loaded partition ID. ReliableWriteMetaKv
// would retry an oversized request forever.
const maxLoadInfoTxnBytes = 512 << 10

// saveLoadInfoConcurrency bounds the snapshot transactions in flight, and so
// their bytes to 4 MiB. It is a write bound, independent of
// metastore.readConcurrency.
const saveLoadInfoConcurrency = 8

// SaveLoadInfos writes the transactions of a batch-by-batch write, up to
// saveLoadInfoConcurrency at a time. A batch closes before the snapshot that
// would exceed MaxTxnOps keys or maxLoadInfoTxnBytes. Distinct snapshots have
// disjoint keys, so the stored result does not depend on the order of the
// transactions; repeated snapshots are written one transaction at a time, in
// order. No transaction starts after one has failed, and an encoding failure
// is reported after the transactions closed before that snapshot complete.
func (c *loadInfoCatalog) SaveLoadInfos(ctx context.Context, infos []*querypb.GetQueryViewLoadInfoResponse) error {
	g, gctx := errgroup.WithContext(ctx)
	if repeatsLoadInfo(infos) {
		g.SetLimit(1)
	} else {
		g.SetLimit(saveLoadInfoConcurrency)
	}
	maxOps := max(c.storage.MaxTxnOps(), 1)
	kvs := make(map[string]string)
	size := 0
	flush := func() {
		if len(kvs) == 0 {
			return
		}
		batch := kvs
		g.Go(func() error {
			if err := gctx.Err(); err != nil {
				return err
			}
			return c.storage.MultiSave(gctx, batch)
		})
		kvs, size = make(map[string]string), 0
	}
	for _, info := range infos {
		value, err := proto.Marshal(info)
		if err != nil {
			if err := g.Wait(); err != nil {
				return err
			}
			return merr.Wrap(err, "encode query view load snapshot")
		}
		key := loadInfoKey(info.GetCollectionID(), info.GetVersion())
		if len(kvs) >= maxOps || size+len(key)+len(value) > maxLoadInfoTxnBytes {
			flush()
		}
		kvs[key] = string(value)
		size += len(key) + len(value)
	}
	flush()
	return g.Wait()
}

func repeatsLoadInfo(infos []*querypb.GetQueryViewLoadInfoResponse) bool {
	seen := make(map[LoadInfoID]struct{}, len(infos))
	for _, info := range infos {
		id := LoadInfoID{info.GetCollectionID(), info.GetVersion()}
		if _, ok := seen[id]; ok {
			return true
		}
		seen[id] = struct{}{}
	}
	return false
}

func decodeLoadInfo(value string, id LoadInfoID) (*querypb.GetQueryViewLoadInfoResponse, error) {
	info := new(querypb.GetQueryViewLoadInfoResponse)
	if err := proto.Unmarshal([]byte(value), info); err != nil {
		return nil, merr.Wrap(err, "decode query view load snapshot")
	}
	if info.GetCollectionID() != id.CollectionID || info.GetVersion() != id.Version {
		return nil, merr.WrapErrDataIntegrityMsg("query view load snapshot identity does not match its catalog key")
	}
	return info, nil
}
