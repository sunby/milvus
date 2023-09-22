package dependency

import (
	"testing"

	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/stretchr/testify/assert"
)

var Params = paramtable.Get()

func TestMain(m *testing.M) {
	paramtable.Init()
	m.Run()
}
func TestStorageLock(t *testing.T) {
	etcdCli, err := etcd.GetEtcdClient(
		Params.EtcdCfg.UseEmbedEtcd.GetAsBool(),
		Params.EtcdCfg.EtcdUseSSL.GetAsBool(),
		Params.EtcdCfg.Endpoints.GetAsStrings(),
		Params.EtcdCfg.EtcdTLSCert.GetValue(),
		Params.EtcdCfg.EtcdTLSKey.GetValue(),
		Params.EtcdCfg.EtcdTLSCACert.GetValue(),
		Params.EtcdCfg.EtcdTLSMinVersion.GetValue())
	defer etcdCli.Close()
	assert.NoError(t, err)

	var version int64 = 0
	fetch := func() (int64, error) {
		return version, nil
	}
	save := func(v int64) error {
		version = v
		return nil
	}
	lm := NewEtcdLockManager(etcdCli, fetch, save)
	v, err := lm.Acquire()
	assert.NoError(t, err)
	assert.EqualValues(t, 0, v)
	// another will block and timeout
	lm2 := NewEtcdLockManager(etcdCli, fetch, save)
	_, err = lm2.Acquire()
	assert.Error(t, err)

	lm.Release(1, true)
	// now another will success
	v, err = lm2.Acquire()
	assert.NoError(t, err)
	assert.EqualValues(t, 1, v)
	lm2.Release(-1, false)
}
