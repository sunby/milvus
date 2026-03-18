package datacoord

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/retry"
	"github.com/tikv/client-go/v2/txnkv"
	"github.com/tikv/client-go/v2/txnkv/transaction"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
)

var (
	ErrKeyAlreadyExists = fmt.Errorf("key already exists")
	ErrKeyNotFound      = fmt.Errorf("key not found")
)

// OptimisticTxnPersist is a persist layer that uses optimistic transactions.
type OptimisticTxnPersist[K comparable, V any] interface {
	// Txn creates a new transaction. Add operations via Insert/Update/Upsert/Delete,
	// then call Commit to execute them atomically.
	Txn(ctx context.Context) Txn[K, V]
	// Scan reads all key-value pairs with the given prefix.
	Scan(ctx context.Context, prefix K) (keys []K, values []V, versions []int64, err error)
}

// Txn collects operations and commits them atomically.
type Txn[K comparable, V any] interface {
	// Insert adds a write for a key that must not exist.
	Insert(key K, value V)
	// Update adds a read-modify-write for a key that must exist.
	Update(key K, f UpdateFunc[V])
	// Upsert adds an insert-or-update. If key doesn't exist, inserts value. If exists, applies f.
	Upsert(key K, value V, f UpdateFunc[V])
	// Delete adds a delete for a key that must exist.
	Delete(key K)
	// Commit executes all operations atomically. Returns results in the order ops were added.
	// On CAS failure, retries automatically (re-reads and re-applies UpdateFuncs).
	Commit() ([]TxnResult[V], error)
}

// TxnResult is the result of a single operation after commit.
type TxnResult[V any] struct {
	Value   V
	Version int64
}

// UpdateFunc transforms an existing value. Returns (newValue, shouldWrite).
// If shouldWrite is false, the write is skipped and the existing value/version are returned.
type UpdateFunc[T any] func(existing T) (T, bool)

type Marshaler[T any] interface {
	Marshal(v T) ([]byte, error)
	Unmarshal(v []byte) (T, error)
}

// --- Segment key helpers ---

const segmentMetaPrefix = "datacoord-meta/s/"

func segmentKey(collectionID, partitionID, segmentID int64) string {
	return fmt.Sprintf("%s%d/%d/%d", segmentMetaPrefix, collectionID, partitionID, segmentID)
}

func segmentIDFromKey(key string) (int64, error) {
	parts := strings.Split(key, "/")
	if len(parts) == 0 {
		return 0, fmt.Errorf("invalid segment key: %s", key)
	}
	return strconv.ParseInt(parts[len(parts)-1], 10, 64)
}

// --- SegmentInfo Marshaler (protobuf) ---

type SegmentInfoMarshaler struct{}

func (m *SegmentInfoMarshaler) Marshal(v *datapb.SegmentInfo) ([]byte, error) {
	return proto.Marshal(v)
}

func (m *SegmentInfoMarshaler) Unmarshal(data []byte) (*datapb.SegmentInfo, error) {
	v := &datapb.SegmentInfo{}
	if err := proto.Unmarshal(data, v); err != nil {
		return nil, err
	}
	return v, nil
}

// ============================================================
// op types (internal)
// ============================================================

type opKind int

const (
	opInsert opKind = iota
	opUpdate
	opUpsert
	opDelete
)

type txnOp[K comparable, V any] struct {
	kind        opKind
	key         K
	value       V // used by Insert, Upsert (insert case)
	updateFunc  UpdateFunc[V] // used by Update, Upsert (update case)
}

// ============================================================
// Etcd implementation
// ============================================================

type etcdPersist[K string, V any] struct {
	cli            *clientv3.Client
	marshaler      Marshaler[V]
	maxOpsPerTxn   int // max etcd ops (cmps + puts) per transaction; each write op costs 2
}

// NewOptimisticTxnEtcdPersist creates an etcd-backed persist layer.
// maxOpsPerTxn is the etcd max-txn-ops limit (e.g. paramtable MaxEtcdTxnNum).
// Each write operation uses 2 etcd ops (1 cmp + 1 put/delete), so effective batch
// size is maxOpsPerTxn/2.
func NewOptimisticTxnEtcdPersist[K string, V any](cli *clientv3.Client, marshaler Marshaler[V], maxOpsPerTxn int) OptimisticTxnPersist[K, V] {
	return &etcdPersist[K, V]{cli: cli, marshaler: marshaler, maxOpsPerTxn: maxOpsPerTxn}
}

func (p *etcdPersist[K, V]) Txn(ctx context.Context) Txn[K, V] {
	return &etcdTxn[K, V]{ctx: ctx, persist: p, maxWriteOps: p.maxOpsPerTxn / 2}
}

func (p *etcdPersist[K, V]) Scan(ctx context.Context, prefix K) ([]K, []V, []int64, error) {
	const batchSize int64 = 10000
	key := string(prefix)
	end := clientv3.GetPrefixRangeEnd(key)

	var ks []K
	var vals []V
	var vers []int64

	for {
		resp, err := p.cli.Get(ctx, key, clientv3.WithRange(end), clientv3.WithLimit(batchSize), clientv3.WithSerializable())
		if err != nil {
			return nil, nil, nil, err
		}
		for _, kv := range resp.Kvs {
			v, err := p.marshaler.Unmarshal(kv.Value)
			if err != nil {
				return nil, nil, nil, err
			}
			ks = append(ks, K(kv.Key))
			vals = append(vals, v)
			vers = append(vers, kv.ModRevision)
		}
		if !resp.More {
			break
		}
		// Next batch starts after the last key
		key = string(resp.Kvs[len(resp.Kvs)-1].Key) + "\x00"
		log.Info("etcdPersist.Scan next batch", zap.String("key", key))
	}
	log.Info("etcdPersist.Scan done", zap.String("prefix", string(prefix)), zap.Int("count", len(ks)))
	return ks, vals, vers, nil
}

type etcdTxn[K string, V any] struct {
	ctx         context.Context
	persist     *etcdPersist[K, V]
	ops         []txnOp[K, V]
	maxWriteOps int // max written ops per etcd txn (= maxOpsPerTxn / 2)
}

func (t *etcdTxn[K, V]) Insert(key K, value V) {
	t.ops = append(t.ops, txnOp[K, V]{kind: opInsert, key: key, value: value})
}

func (t *etcdTxn[K, V]) Update(key K, f UpdateFunc[V]) {
	t.ops = append(t.ops, txnOp[K, V]{kind: opUpdate, key: key, updateFunc: f})
}

func (t *etcdTxn[K, V]) Upsert(key K, value V, f UpdateFunc[V]) {
	t.ops = append(t.ops, txnOp[K, V]{kind: opUpsert, key: key, value: value, updateFunc: f})
}

func (t *etcdTxn[K, V]) Delete(key K) {
	t.ops = append(t.ops, txnOp[K, V]{kind: opDelete, key: key})
}

func (t *etcdTxn[K, V]) Commit() ([]TxnResult[V], error) {
	results := make([]TxnResult[V], len(t.ops))

	// Phase 1: read existing values and prepare per-op write plans.
	type writePlan struct {
		opIndex int // index into t.ops / results
		cmp     clientv3.Cmp
		etcdOp  clientv3.Op
		hasCmp  bool
	}
	plans, err := t.prepareWritePlans(results)
	if err != nil {
		return nil, err
	}
	if len(plans) == 0 {
		return results, nil // all skipped
	}

	// Phase 2: commit in batches respecting maxWriteOps.
	batchSize := t.maxWriteOps
	if batchSize <= 0 {
		batchSize = 64
	}
	for start := 0; start < len(plans); start += batchSize {
		end := start + batchSize
		if end > len(plans) {
			end = len(plans)
		}
		batch := plans[start:end]

		if err := t.commitBatch(batch, results); err != nil {
			return nil, err
		}
	}

	return results, nil
}

// writePlan holds the prepared etcd operations for a single op.
type writePlan struct {
	opIndex int
	cmp     clientv3.Cmp
	etcdOp  clientv3.Op
	hasCmp  bool
}

// prepareWritePlans reads current values from etcd and builds write plans.
// Ops that are skipped (shouldWrite=false) have their results populated directly.
func (t *etcdTxn[K, V]) prepareWritePlans(results []TxnResult[V]) ([]writePlan, error) {
	var plans []writePlan

	err := retry.Do(t.ctx, func() error {
		plans = plans[:0] // reset on retry
		for i, op := range t.ops {
			keyStr := string(op.key)
			resp, err := t.persist.cli.Get(t.ctx, keyStr, clientv3.WithSerializable())
			if err != nil {
				return err
			}
			exists := len(resp.Kvs) > 0

			switch op.kind {
			case opInsert:
				if exists {
					return retry.Unrecoverable(fmt.Errorf("%w: %s", ErrKeyAlreadyExists, keyStr))
				}
				valBytes, err := t.persist.marshaler.Marshal(op.value)
				if err != nil {
					return retry.Unrecoverable(err)
				}
				plans = append(plans, writePlan{
					opIndex: i,
					cmp:     clientv3.Compare(clientv3.CreateRevision(keyStr), "=", 0),
					etcdOp:  clientv3.OpPut(keyStr, string(valBytes)),
					hasCmp:  true,
				})
				results[i].Value = op.value

			case opUpdate:
				if !exists {
					return retry.Unrecoverable(fmt.Errorf("%w: %s", ErrKeyNotFound, keyStr))
				}
				existing, err := t.persist.marshaler.Unmarshal(resp.Kvs[0].Value)
				if err != nil {
					return retry.Unrecoverable(err)
				}
				newV, shouldWrite := op.updateFunc(existing)
				if !shouldWrite {
					results[i] = TxnResult[V]{Value: existing, Version: resp.Kvs[0].ModRevision}
					continue
				}
				valBytes, err := t.persist.marshaler.Marshal(newV)
				if err != nil {
					return retry.Unrecoverable(err)
				}
				plans = append(plans, writePlan{
					opIndex: i,
					cmp:     clientv3.Compare(clientv3.ModRevision(keyStr), "=", resp.Kvs[0].ModRevision),
					etcdOp:  clientv3.OpPut(keyStr, string(valBytes)),
					hasCmp:  true,
				})
				results[i].Value = newV

			case opUpsert:
				if exists {
					existing, err := t.persist.marshaler.Unmarshal(resp.Kvs[0].Value)
					if err != nil {
						return retry.Unrecoverable(err)
					}
					newV, shouldWrite := op.updateFunc(existing)
					if !shouldWrite {
						results[i] = TxnResult[V]{Value: existing, Version: resp.Kvs[0].ModRevision}
						continue
					}
					valBytes, err := t.persist.marshaler.Marshal(newV)
					if err != nil {
						return retry.Unrecoverable(err)
					}
					plans = append(plans, writePlan{
						opIndex: i,
						cmp:     clientv3.Compare(clientv3.ModRevision(keyStr), "=", resp.Kvs[0].ModRevision),
						etcdOp:  clientv3.OpPut(keyStr, string(valBytes)),
						hasCmp:  true,
					})
					results[i].Value = newV
				} else {
					valBytes, err := t.persist.marshaler.Marshal(op.value)
					if err != nil {
						return retry.Unrecoverable(err)
					}
					plans = append(plans, writePlan{
						opIndex: i,
						etcdOp:  clientv3.OpPut(keyStr, string(valBytes)),
						hasCmp:  false,
					})
					results[i].Value = op.value
				}

			case opDelete:
				if !exists {
					return retry.Unrecoverable(fmt.Errorf("%w: %s", ErrKeyNotFound, keyStr))
				}
				plans = append(plans, writePlan{
					opIndex: i,
					cmp:     clientv3.Compare(clientv3.ModRevision(keyStr), "=", resp.Kvs[0].ModRevision),
					etcdOp:  clientv3.OpDelete(keyStr),
					hasCmp:  true,
				})
			}
		}
		return nil
	}, retry.AttemptAlways())

	if err != nil {
		return nil, err
	}
	return plans, nil
}

// commitBatch commits a batch of write plans as a single etcd transaction with CAS.
func (t *etcdTxn[K, V]) commitBatch(batch []writePlan, results []TxnResult[V]) error {
	return retry.Do(t.ctx, func() error {
		cmps := make([]clientv3.Cmp, 0, len(batch))
		ops := make([]clientv3.Op, 0, len(batch))
		for _, p := range batch {
			if p.hasCmp {
				cmps = append(cmps, p.cmp)
			}
			ops = append(ops, p.etcdOp)
		}

		var txnResp *clientv3.TxnResponse
		var err error
		if len(cmps) == 0 {
			txnResp, err = t.persist.cli.Txn(t.ctx).Then(ops...).Commit()
		} else {
			txnResp, err = t.persist.cli.Txn(t.ctx).If(cmps...).Then(ops...).Commit()
		}
		if err != nil {
			return err
		}
		if !txnResp.Succeeded {
			return fmt.Errorf("CAS failed, concurrent modification")
		}
		for _, p := range batch {
			results[p.opIndex].Version = txnResp.Header.Revision
		}
		return nil
	}, retry.AttemptAlways())
}

// ============================================================
// TiKV implementation
// ============================================================

type tikvPersist[K string, V any] struct {
	cli       *txnkv.Client
	marshaler Marshaler[V]
}

func NewOptimisticTxnTiKVPersist[K string, V any](cli *txnkv.Client, marshaler Marshaler[V]) OptimisticTxnPersist[K, V] {
	return &tikvPersist[K, V]{cli: cli, marshaler: marshaler}
}

func (p *tikvPersist[K, V]) Txn(ctx context.Context) Txn[K, V] {
	return &tikvTxn[K, V]{ctx: ctx, persist: p}
}

func (p *tikvPersist[K, V]) captureCommitTS(txn interface{ SetCommitCallback(func(string, error)) }) *uint64 {
	var commitTS uint64
	txn.SetCommitCallback(func(info string, err error) {
		if err == nil {
			var txnInfo transaction.TxnInfo
			json.Unmarshal([]byte(info), &txnInfo)
			commitTS = txnInfo.CommitTS
		}
	})
	return &commitTS
}

func (p *tikvPersist[K, V]) tikvKeyExists(ctx context.Context, txn interface {
	Get(ctx context.Context, k []byte) ([]byte, error)
}, key []byte) ([]byte, bool, error) {
	val, err := txn.Get(ctx, key)
	if err != nil {
		return nil, false, nil
	}
	return val, true, nil
}

func (p *tikvPersist[K, V]) Scan(ctx context.Context, prefix K) ([]K, []V, []int64, error) {
	txn, err := p.cli.Begin()
	if err != nil {
		return nil, nil, nil, err
	}
	defer txn.Rollback()
	prefixBytes := []byte(prefix)
	endKey := make([]byte, len(prefixBytes))
	copy(endKey, prefixBytes)
	endKey[len(endKey)-1]++

	iter, err := txn.Iter(prefixBytes, endKey)
	if err != nil {
		return nil, nil, nil, err
	}
	defer iter.Close()

	var ks []K
	var vals []V
	var vers []int64
	for iter.Valid() {
		v, err := p.marshaler.Unmarshal(iter.Value())
		if err != nil {
			return nil, nil, nil, err
		}
		ks = append(ks, K(iter.Key()))
		vals = append(vals, v)
		vers = append(vers, int64(txn.StartTS()))
		if err := iter.Next(); err != nil {
			return nil, nil, nil, err
		}
	}
	return ks, vals, vers, nil
}

type tikvTxn[K string, V any] struct {
	ctx     context.Context
	persist *tikvPersist[K, V]
	ops     []txnOp[K, V]
}

func (t *tikvTxn[K, V]) Insert(key K, value V) {
	t.ops = append(t.ops, txnOp[K, V]{kind: opInsert, key: key, value: value})
}

func (t *tikvTxn[K, V]) Update(key K, f UpdateFunc[V]) {
	t.ops = append(t.ops, txnOp[K, V]{kind: opUpdate, key: key, updateFunc: f})
}

func (t *tikvTxn[K, V]) Upsert(key K, value V, f UpdateFunc[V]) {
	t.ops = append(t.ops, txnOp[K, V]{kind: opUpsert, key: key, value: value, updateFunc: f})
}

func (t *tikvTxn[K, V]) Delete(key K) {
	t.ops = append(t.ops, txnOp[K, V]{kind: opDelete, key: key})
}

func (t *tikvTxn[K, V]) Commit() ([]TxnResult[V], error) {
	results := make([]TxnResult[V], len(t.ops))

	err := retry.Do(t.ctx, func() error {
		txn, err := t.persist.cli.Begin()
		if err != nil {
			return err
		}
		defer txn.Rollback()

		anyWrite := false
		for i, op := range t.ops {
			keyBytes := []byte(op.key)
			val, exists, err := t.persist.tikvKeyExists(t.ctx, txn, keyBytes)
			if err != nil {
				return err
			}

			switch op.kind {
			case opInsert:
				if exists {
					return retry.Unrecoverable(fmt.Errorf("%w: %s", ErrKeyAlreadyExists, string(op.key)))
				}
				valBytes, err := t.persist.marshaler.Marshal(op.value)
				if err != nil {
					return retry.Unrecoverable(err)
				}
				if err := txn.Set(keyBytes, valBytes); err != nil {
					return err
				}
				results[i].Value = op.value
				anyWrite = true

			case opUpdate:
				if !exists {
					return retry.Unrecoverable(fmt.Errorf("%w: %s", ErrKeyNotFound, string(op.key)))
				}
				existing, err := t.persist.marshaler.Unmarshal(val)
				if err != nil {
					return retry.Unrecoverable(err)
				}
				newV, shouldWrite := op.updateFunc(existing)
				if !shouldWrite {
					results[i].Value = existing
					continue
				}
				valBytes, err := t.persist.marshaler.Marshal(newV)
				if err != nil {
					return retry.Unrecoverable(err)
				}
				if err := txn.Set(keyBytes, valBytes); err != nil {
					return err
				}
				results[i].Value = newV
				anyWrite = true

			case opUpsert:
				if exists {
					existing, err := t.persist.marshaler.Unmarshal(val)
					if err != nil {
						return retry.Unrecoverable(err)
					}
					newV, shouldWrite := op.updateFunc(existing)
					if !shouldWrite {
						results[i].Value = existing
						continue
					}
					valBytes, err := t.persist.marshaler.Marshal(newV)
					if err != nil {
						return retry.Unrecoverable(err)
					}
					if err := txn.Set(keyBytes, valBytes); err != nil {
						return err
					}
					results[i].Value = newV
				} else {
					valBytes, err := t.persist.marshaler.Marshal(op.value)
					if err != nil {
						return retry.Unrecoverable(err)
					}
					if err := txn.Set(keyBytes, valBytes); err != nil {
						return err
					}
					results[i].Value = op.value
				}
				anyWrite = true

			case opDelete:
				if !exists {
					return retry.Unrecoverable(fmt.Errorf("%w: %s", ErrKeyNotFound, string(op.key)))
				}
				if err := txn.Delete(keyBytes); err != nil {
					return err
				}
				anyWrite = true
			}
		}

		if !anyWrite {
			return nil
		}

		cts := t.persist.captureCommitTS(txn)
		err = txn.Commit(t.ctx)
		if err == nil {
			for i := range t.ops {
				results[i].Version = int64(*cts)
			}
		}
		return err
	}, retry.AttemptAlways())

	if err != nil {
		return nil, err
	}
	return results, nil
}

// ============================================================
// In-memory implementation (for testing)
// ============================================================

type memEntry[V any] struct {
	value   V
	version int64
}

type memPersist[K comparable, V any] struct {
	data    map[K]*memEntry[V]
	nextVer int64
}

func NewOptimisticTxnMemoryPersist[K comparable, V any](marshaler Marshaler[V]) OptimisticTxnPersist[K, V] {
	return &memPersist[K, V]{
		data:    make(map[K]*memEntry[V]),
		nextVer: 1,
	}
}

func (p *memPersist[K, V]) Txn(ctx context.Context) Txn[K, V] {
	return &memTxn[K, V]{persist: p}
}

func (p *memPersist[K, V]) Scan(ctx context.Context, prefix K) ([]K, []V, []int64, error) {
	prefixStr := fmt.Sprintf("%v", prefix)
	var ks []K
	var vals []V
	var vers []int64
	for k, entry := range p.data {
		keyStr := fmt.Sprintf("%v", k)
		if strings.HasPrefix(keyStr, prefixStr) {
			ks = append(ks, k)
			vals = append(vals, entry.value)
			vers = append(vers, entry.version)
		}
	}
	return ks, vals, vers, nil
}

type memTxn[K comparable, V any] struct {
	persist *memPersist[K, V]
	ops     []txnOp[K, V]
}

func (t *memTxn[K, V]) Insert(key K, value V) {
	t.ops = append(t.ops, txnOp[K, V]{kind: opInsert, key: key, value: value})
}

func (t *memTxn[K, V]) Update(key K, f UpdateFunc[V]) {
	t.ops = append(t.ops, txnOp[K, V]{kind: opUpdate, key: key, updateFunc: f})
}

func (t *memTxn[K, V]) Upsert(key K, value V, f UpdateFunc[V]) {
	t.ops = append(t.ops, txnOp[K, V]{kind: opUpsert, key: key, value: value, updateFunc: f})
}

func (t *memTxn[K, V]) Delete(key K) {
	t.ops = append(t.ops, txnOp[K, V]{kind: opDelete, key: key})
}

func (t *memTxn[K, V]) Commit() ([]TxnResult[V], error) {
	p := t.persist
	// Validate all ops first (don't partially apply).
	for _, op := range t.ops {
		switch op.kind {
		case opInsert:
			if _, ok := p.data[op.key]; ok {
				return nil, fmt.Errorf("%w: %v", ErrKeyAlreadyExists, op.key)
			}
		case opUpdate:
			if _, ok := p.data[op.key]; !ok {
				return nil, fmt.Errorf("%w: %v", ErrKeyNotFound, op.key)
			}
		case opDelete:
			if _, ok := p.data[op.key]; !ok {
				return nil, fmt.Errorf("%w: %v", ErrKeyNotFound, op.key)
			}
		case opUpsert:
			// always valid
		}
	}

	ver := p.nextVer
	p.nextVer++
	results := make([]TxnResult[V], len(t.ops))

	for i, op := range t.ops {
		switch op.kind {
		case opInsert:
			p.data[op.key] = &memEntry[V]{value: op.value, version: ver}
			results[i] = TxnResult[V]{Value: op.value, Version: ver}

		case opUpdate:
			entry := p.data[op.key]
			newV, shouldWrite := op.updateFunc(entry.value)
			if !shouldWrite {
				results[i] = TxnResult[V]{Value: entry.value, Version: entry.version}
				continue
			}
			p.data[op.key] = &memEntry[V]{value: newV, version: ver}
			results[i] = TxnResult[V]{Value: newV, Version: ver}

		case opUpsert:
			if entry, ok := p.data[op.key]; ok {
				newV, shouldWrite := op.updateFunc(entry.value)
				if !shouldWrite {
					results[i] = TxnResult[V]{Value: entry.value, Version: entry.version}
					continue
				}
				p.data[op.key] = &memEntry[V]{value: newV, version: ver}
				results[i] = TxnResult[V]{Value: newV, Version: ver}
			} else {
				p.data[op.key] = &memEntry[V]{value: op.value, version: ver}
				results[i] = TxnResult[V]{Value: op.value, Version: ver}
			}

		case opDelete:
			delete(p.data, op.key)
			results[i] = TxnResult[V]{Version: ver}
		}
	}

	return results, nil
}
