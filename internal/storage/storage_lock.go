package storage

import (
	"context"
	"errors"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

const (
	acquireLockTimeout = 5 * time.Second
	lockTTL            = 10 // in seconds
	etcdTxnTimeout     = 5 * time.Second
	lockKey            = "storage-lock-key"
)

type EtcdLockManager struct {
	id               int64
	cli              *clientv3.Client
	fetchVersionFunc func() (int64, error)
	saveVersionFunc  func(int64) error
	session          *concurrency.Session
	mutex            *concurrency.Mutex
}

func (m *EtcdLockManager) Acquire() (version int64, useLatest bool, err error) {
	ctx, cancel := context.WithTimeout(context.TODO(), acquireLockTimeout)
	defer cancel()

	resp, err := m.cli.Grant(ctx, lockTTL)
	if err != nil {
		return -1, false, err
	}
	if resp.Error != "" {
		return -1, false, errors.New(resp.Error)
	}

	m.session, err = concurrency.NewSession(m.cli, concurrency.WithLease(resp.ID))
	if err != nil {
		return -1, false, err
	}

	m.mutex = concurrency.NewMutex(m.session, lockKey)
	err = m.mutex.Lock(ctx)
	if err != nil {
		m.session.Close()
		m.session = nil
		return -1, false, err
	}

	version, err = m.fetchVersionFunc()
	return version, false, err
}

func (m *EtcdLockManager) Release(version int64, success bool) (err error) {
	defer func() {
		m.mutex.Unlock(context.TODO())
		m.session.Close()
		m.mutex = nil
		m.session = nil
	}()
	if success {
		err = m.saveVersionFunc(version)
	}
	return
}

func NewEtcdLockManager(id int64, cli *clientv3.Client, fetch func() (int64, error), save func(int64) error) *EtcdLockManager {
	return &EtcdLockManager{
		id:               id,
		cli:              cli,
		fetchVersionFunc: fetch,
		saveVersionFunc:  save,
	}
}
