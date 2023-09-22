package dependency

import (
	"context"
	"errors"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	acquireLockTimeout = 5 * time.Second
	lockTTL            = 10 // in seconds
	etcdTxnTimeout     = 5 * time.Second
	lockKey            = "storage-lock-key"
)

type EtcdLockManager struct {
	cli              *clientv3.Client
	leaseID          clientv3.LeaseID
	keepAliveCancel  context.CancelFunc
	fetchVersionFunc func() (int64, error)
	saveVersionFunc  func(int64) error
}

func (m *EtcdLockManager) Acquire() (version int64, err error) {
	ctx, cancel := context.WithTimeout(context.TODO(), acquireLockTimeout)
	defer cancel()
	resp, err := m.cli.Grant(ctx, lockTTL)
	if err != nil {
		return -1, err
	}
	if resp.Error != "" {
		return -1, errors.New(resp.Error)
	}
	m.leaseID = resp.ID
	defer func() {
		if err != nil {
			m.cli.Revoke(context.TODO(), m.leaseID)
		}
	}()

	keepAliveCtx, keepAliveCancel := context.WithCancel(context.TODO())
	_, err = m.cli.KeepAlive(keepAliveCtx, m.leaseID)
	if err != nil {
		return -1, err
	}
	m.keepAliveCancel = keepAliveCancel
	defer func() {
		if err != nil {
			m.keepAliveCancel()
		}
	}()

	ctx, cancel = context.WithTimeout(context.TODO(), etcdTxnTimeout)
	defer cancel()
	var txnResp *clientv3.TxnResponse
	txnResp, err = m.cli.Txn(ctx).If(clientv3.Compare(clientv3.CreateRevision(lockKey), "=", 0)).Then(clientv3.OpPut(lockKey, "", clientv3.WithLease(m.leaseID))).Commit()
	if err != nil {
		return -1, err
	}
	if !txnResp.Succeeded {
		return -1, errors.New("unable to lock")
	}

	return m.fetchVersionFunc()
}

func (m *EtcdLockManager) Release(version int64, success bool) {
	if success {
		m.saveVersionFunc(version)
	}
	m.keepAliveCancel()
	m.cli.Revoke(context.TODO(), m.leaseID)
}

func NewEtcdLockManager(cli *clientv3.Client, fetch func() (int64, error), save func(int64) error) *EtcdLockManager {
	return &EtcdLockManager{
		cli:              cli,
		fetchVersionFunc: fetch,
		saveVersionFunc:  save,
	}
}
