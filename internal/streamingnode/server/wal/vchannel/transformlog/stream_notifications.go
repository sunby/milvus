package transformlog

import (
	"sort"
	"sync"
)

// streamNotifications coalesces wakeups, not log entries. The stream drains
// each changed vchannel from its subscription cursor, outside these locks.
type streamNotifications struct {
	mu      sync.Mutex
	pending map[string]int
	queue   []string
	ready   chan struct{}
}

func newStreamNotifications() *streamNotifications {
	return &streamNotifications{
		pending: make(map[string]int),
		ready:   make(chan struct{}, 1),
	}
}

func (n *streamNotifications) notify(vchannel string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if _, exists := n.pending[vchannel]; exists {
		return
	}
	n.pending[vchannel] = len(n.queue)
	n.queue = append(n.queue, vchannel)
	select {
	case n.ready <- struct{}{}:
	default:
	}
}

func (n *streamNotifications) takePending() []string {
	n.mu.Lock()
	pending := n.queue
	n.queue = nil
	// A previously large batch can leave a sparse map with large capacity.
	// Walk the queue and delete its keys instead of ranging over/clearing the
	// map, so draining costs only the number of pending vchannels.
	for _, vchannel := range pending {
		delete(n.pending, vchannel)
	}
	n.mu.Unlock()
	// Preserve deterministic dispatch without holding the manager's shared
	// mutex or blocking appends while sorting the stream's changed vchannels.
	sort.Strings(pending)
	return pending
}

func (n *streamNotifications) forget(vchannel string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	index, exists := n.pending[vchannel]
	if !exists {
		return
	}
	last := len(n.queue) - 1
	if index != last {
		n.queue[index] = n.queue[last]
		n.pending[n.queue[index]] = index
	}
	n.queue[last] = ""
	n.queue = n.queue[:last]
	delete(n.pending, vchannel)
}
