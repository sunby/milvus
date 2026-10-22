package vchannel

import (
	"slices"
	"sync"

	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// moduleIndex holds the modules of a pchannel by vchannel and as a list for
// the sweeps over every module on each pchannel-level message and every
// second, where ranging over a map of tens of thousands of vchannels costs
// more than the sweep itself. Insertion appends and removal copies, so a list
// returned by List stays valid while modules come and go.
type moduleIndex struct {
	byVChannel *typeutil.ConcurrentMap[string, *VChannelRecoveryModule]
	mu         sync.Mutex
	list       []*VChannelRecoveryModule
}

func newModuleIndex() *moduleIndex {
	return &moduleIndex{byVChannel: typeutil.NewConcurrentMap[string, *VChannelRecoveryModule]()}
}

func (i *moduleIndex) Get(vchannel string) (*VChannelRecoveryModule, bool) {
	return i.byVChannel.Get(vchannel)
}

// GetOrInsert returns the module of the vchannel, inserting module if there
// is none, and whether it was already there.
func (i *moduleIndex) GetOrInsert(module *VChannelRecoveryModule) (*VChannelRecoveryModule, bool) {
	i.mu.Lock()
	defer i.mu.Unlock()
	stored, loaded := i.byVChannel.GetOrInsert(module.vchannel, module)
	if !loaded {
		i.list = append(i.list, stored)
	}
	return stored, loaded
}

// Remove removes module if it is still the module of its vchannel.
func (i *moduleIndex) Remove(module *VChannelRecoveryModule) bool {
	i.mu.Lock()
	defer i.mu.Unlock()
	if current, ok := i.byVChannel.Get(module.vchannel); !ok || current != module {
		return false
	}
	i.byVChannel.Remove(module.vchannel)
	i.list = slices.DeleteFunc(slices.Clone(i.list), func(m *VChannelRecoveryModule) bool { return m == module })
	return true
}

// List returns every module. The caller must not modify it.
func (i *moduleIndex) List() []*VChannelRecoveryModule {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.list
}
