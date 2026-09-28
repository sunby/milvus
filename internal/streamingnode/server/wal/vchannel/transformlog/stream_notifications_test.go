package transformlog

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
)

func TestStreamNotificationsRouteAndCoalesce(t *testing.T) {
	manager := NewStreamManager("pchannel")
	first, second, shared := newStreamNotifications(), newStreamNotifications(), newStreamNotifications()
	manager.watchStream("v1", first)
	manager.watchStream("v1", shared)
	manager.watchStream("v2", second)
	manager.watchStream("v2", shared)

	for i := 0; i < 100; i++ {
		manager.notify("v1")
	}
	require.Len(t, first.ready, 1)
	require.Len(t, shared.ready, 1)
	require.Empty(t, second.ready, "an unrelated stream must not wake")
	manager.notify("v2")
	<-shared.ready
	require.Equal(t, []string{"v1", "v2"}, shared.takePending())
	<-first.ready
	require.Equal(t, []string{"v1"}, first.takePending())
	manager.notify("")
	require.Equal(t, []string{"v1", "v2"}, shared.takePending())
	require.Equal(t, []string{"v2"}, second.takePending())
	require.Equal(t, []string{"v1"}, first.takePending())
	<-first.ready

	// A write after draining must enqueue another wakeup, even for the same key.
	manager.notify("v1")
	require.Len(t, first.ready, 1)
	<-first.ready
	require.Equal(t, []string{"v1"}, first.takePending())
	manager.notify("v1")
	manager.unwatchStream("v1", first)
	require.Empty(t, first.takePending(), "closing the last subscription discards stale pending work")
	manager.notify("v1")
	require.Empty(t, first.takePending())
	manager.unwatchStream("v1", shared)
	manager.unwatchStream("v2", shared)
	manager.unwatchStream("v2", second)
	require.Empty(t, manager.streamsByV)
}

func TestStreamNotificationsForgetPendingVChannel(t *testing.T) {
	notifications := newStreamNotifications()
	for _, vchannel := range []string{"v1", "v2", "v3"} {
		notifications.notify(vchannel)
	}
	notifications.forget("v2")
	notifications.forget("v3") // v3 was moved when v2 was removed.
	notifications.forget("absent")
	notifications.notify("v2")
	<-notifications.ready
	require.Equal(t, []string{"v1", "v2"}, notifications.takePending())
	require.Empty(t, notifications.pending)
	require.Empty(t, notifications.queue)
	notifications.notify("v3")
	<-notifications.ready
	require.Equal(t, []string{"v3"}, notifications.takePending())
}

func TestStreamNotificationsCatchupHandoffDoesNotLoseUpdates(t *testing.T) {
	manager := NewStreamManager("pchannel")
	log := New(Config{VChannel: "v1"})
	manager.Register("v1", log)
	require.True(t, log.append(newTransformLogTestDeleteMessage(t, 1), appendOption{}).Appended)
	stream, err := manager.AcquireStream(context.Background(), "pchannel")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, stream.Close()) })
	handler := newGatedStreamHandler(func(event wal.TransformLogStreamEvent) bool { return event.SyncUp != nil })
	t.Cleanup(handler.unblock)
	_, err = stream.Subscribe(context.Background(), wal.TransformLogSubscriptionOption{VChannel: "v1", Handler: handler})
	require.NoError(t, err)
	require.Equal(t, uint64(1), recvStreamEvent(t, handler.events).Entry.GetTimeTick())
	waitStreamSignal(t, handler.entered)
	// The initial SyncUp handler is still running, so no live watch exists yet.
	requireStreamWatchCount(t, manager, "v1", 0)
	for tick := uint64(2); tick <= 10; tick++ {
		require.True(t, log.append(newTransformLogTestDeleteMessage(t, tick), appendOption{}).Appended)
	}
	log.syncUp(20)
	handler.unblock()
	require.Equal(t, uint64(1), recvStreamEvent(t, handler.events).SyncUp.TimeTick)
	for tick := uint64(2); tick <= 10; tick++ {
		require.Equal(t, tick, recvStreamEvent(t, handler.events).Entry.GetTimeTick())
	}
	require.Equal(t, uint64(20), recvStreamEvent(t, handler.events).SyncUp.TimeTick)
	requireStreamWatchCount(t, manager, "v1", 1)
	log.syncUp(30)
	require.Equal(t, uint64(30), recvStreamEvent(t, handler.events).SyncUp.TimeTick)
}

func TestStreamNotificationsCoalesceWhileDeliveringWithoutLosingDeletes(t *testing.T) {
	manager := NewStreamManager("pchannel")
	log := New(Config{VChannel: "v1"})
	manager.Register("v1", log)
	stream, err := manager.AcquireStream(context.Background(), "pchannel")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, stream.Close()) })
	handler := newGatedStreamHandler(func(event wal.TransformLogStreamEvent) bool { return event.Entry != nil })
	t.Cleanup(handler.unblock)
	_, err = stream.Subscribe(context.Background(), wal.TransformLogSubscriptionOption{VChannel: "v1", Handler: handler})
	require.NoError(t, err)
	require.NotNil(t, recvStreamEvent(t, handler.events).SyncUp)
	requireStreamWatchCount(t, manager, "v1", 1)
	require.True(t, log.append(newTransformLogTestDeleteMessage(t, 1), appendOption{}).Appended)
	waitStreamSignal(t, handler.entered)
	for tick := uint64(2); tick <= 10; tick++ {
		require.True(t, log.append(newTransformLogTestDeleteMessage(t, tick), appendOption{}).Appended)
	}
	log.syncUp(20)
	require.Len(t, stream.(*transformLogStream).notifications.ready, 1)
	handler.unblock()
	for tick := uint64(1); tick <= 10; tick++ {
		require.Equal(t, tick, recvStreamEvent(t, handler.events).Entry.GetTimeTick())
	}
	require.Equal(t, uint64(20), recvStreamEvent(t, handler.events).SyncUp.TimeTick)
	requireNoStreamEvent(t, handler.events)
}

func TestStreamNotificationsKeepWatchUntilLastSubscriptionCloses(t *testing.T) {
	manager := NewStreamManager("pchannel")
	log := New(Config{VChannel: "v1"})
	manager.Register("v1", log)
	stream, err := manager.AcquireStream(context.Background(), "pchannel")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, stream.Close()) })
	first, second := newRecordingStreamHandler(), newRecordingStreamHandler()
	sub1, err := stream.Subscribe(context.Background(), wal.TransformLogSubscriptionOption{VChannel: "v1", Handler: first})
	require.NoError(t, err)
	sub2, err := stream.Subscribe(context.Background(), wal.TransformLogSubscriptionOption{VChannel: "v1", Handler: second})
	require.NoError(t, err)
	require.NotNil(t, recvStreamEvent(t, first.events).SyncUp)
	require.NotNil(t, recvStreamEvent(t, second.events).SyncUp)
	requireStreamWatchCount(t, manager, "v1", 1)
	// RecoveryStorage registers the same active log after every message.
	for i := 0; i < 10; i++ {
		manager.Register("v1", log)
	}
	require.NoError(t, sub1.Close())
	requireStreamWatchCount(t, manager, "v1", 1)
	log.syncUp(10)
	require.Equal(t, uint64(10), recvStreamEvent(t, second.events).SyncUp.TimeTick)
	require.NoError(t, sub2.Close())
	requireStreamWatchCount(t, manager, "v1", 0)
}

func TestStreamNotificationsRemoveClosesLiveSubscription(t *testing.T) {
	manager := NewStreamManager("pchannel")
	manager.Register("v1", New(Config{VChannel: "v1"}))
	stream, err := manager.AcquireStream(context.Background(), "pchannel")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, stream.Close()) })
	handler := newRecordingStreamHandler()
	_, err = stream.Subscribe(context.Background(), wal.TransformLogSubscriptionOption{VChannel: "v1", Handler: handler})
	require.NoError(t, err)
	require.NotNil(t, recvStreamEvent(t, handler.events).SyncUp)
	requireStreamWatchCount(t, manager, "v1", 1)
	manager.Remove("v1")
	require.ErrorIs(t, recvStreamEvent(t, handler.events).Err, wal.ErrTransformLogVChannelUnavailable)
	waitStreamSignal(t, handler.closed)
	requireStreamWatchCount(t, manager, "v1", 0)
}

func TestStreamNotificationsSwitchVChannel(t *testing.T) {
	manager := NewStreamManager("pchannel")
	first, second := New(Config{VChannel: "v1"}), New(Config{VChannel: "v2"})
	manager.Register("v1", first)
	manager.Register("v2", second)
	stream, err := manager.AcquireStream(context.Background(), "pchannel")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, stream.Close()) })
	oldHandler, newHandler := newRecordingStreamHandler(), newRecordingStreamHandler()
	_, err = stream.Subscribe(context.Background(), wal.TransformLogSubscriptionOption{SubscriptionID: 42, VChannel: "v1", Handler: oldHandler})
	require.NoError(t, err)
	require.NotNil(t, recvStreamEvent(t, oldHandler.events).SyncUp)
	requireStreamWatchCount(t, manager, "v1", 1)
	_, err = stream.Subscribe(context.Background(), wal.TransformLogSubscriptionOption{SubscriptionID: 42, VChannel: "v2", Handler: newHandler})
	require.NoError(t, err)
	require.NotNil(t, recvStreamEvent(t, newHandler.events).SyncUp)
	waitStreamSignal(t, oldHandler.closed)
	requireStreamWatchCount(t, manager, "v1", 0)
	requireStreamWatchCount(t, manager, "v2", 1)
	first.syncUp(10)
	second.syncUp(20)
	event := recvStreamEvent(t, newHandler.events)
	require.Equal(t, "v2", event.VChannel)
	require.Equal(t, uint64(20), event.SyncUp.TimeTick)
}

func TestStreamNotificationsRemoveDuringCatchupHandoff(t *testing.T) {
	manager := NewStreamManager("pchannel")
	manager.Register("v1", New(Config{VChannel: "v1"}))
	stream, err := manager.AcquireStream(context.Background(), "pchannel")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, stream.Close()) })
	handler := newGatedStreamHandler(func(event wal.TransformLogStreamEvent) bool { return event.SyncUp != nil })
	t.Cleanup(handler.unblock)
	_, err = stream.Subscribe(context.Background(), wal.TransformLogSubscriptionOption{VChannel: "v1", Handler: handler})
	require.NoError(t, err)
	waitStreamSignal(t, handler.entered)
	manager.Remove("v1")
	handler.unblock()
	require.NotNil(t, recvStreamEvent(t, handler.events).SyncUp)
	require.ErrorIs(t, recvStreamEvent(t, handler.events).Err, wal.ErrTransformLogVChannelUnavailable)
	waitStreamSignal(t, handler.closed)
	requireStreamWatchCount(t, manager, "v1", 0)
}

func TestStreamNotificationsHandlerFailureRemovesWatch(t *testing.T) {
	manager := NewStreamManager("pchannel")
	log := New(Config{VChannel: "v1"})
	manager.Register("v1", log)
	stream, err := manager.AcquireStream(context.Background(), "pchannel")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, stream.Close()) })
	handler := &rejectingEntryHandler{newRecordingStreamHandler()}
	_, err = stream.Subscribe(context.Background(), wal.TransformLogSubscriptionOption{VChannel: "v1", Handler: handler})
	require.NoError(t, err)
	require.NotNil(t, recvStreamEvent(t, handler.events).SyncUp)
	requireStreamWatchCount(t, manager, "v1", 1)
	require.True(t, log.append(newTransformLogTestDeleteMessage(t, 10), appendOption{}).Appended)
	require.ErrorIs(t, recvStreamEvent(t, handler.events).Err, io.ErrClosedPipe)
	waitStreamSignal(t, handler.closed)
	requireStreamWatchCount(t, manager, "v1", 0)
}

func TestStreamNotificationsCancelWhilePublishing(t *testing.T) {
	manager := NewStreamManager("pchannel")
	log := New(Config{VChannel: "v1"})
	manager.Register("v1", log)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	stream, err := manager.AcquireStream(ctx, "pchannel")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, stream.Close()) })
	handler := &countingStreamHandler{caughtUp: make(chan struct{}, 10001)}
	_, err = stream.Subscribe(ctx, wal.TransformLogSubscriptionOption{VChannel: "v1", Handler: handler})
	require.NoError(t, err)
	waitStreamSignal(t, handler.caughtUp)
	requireStreamWatchCount(t, manager, "v1", 1)
	started, published := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(published)
		close(started)
		for tick := uint64(1); tick <= 10000; tick++ {
			log.syncUp(tick)
		}
	}()
	waitStreamSignal(t, started)
	cancel()
	waitStreamSignal(t, stream.Done())
	waitStreamSignal(t, published)
	requireStreamWatchCount(t, manager, "v1", 0)
}

func requireStreamWatchCount(t *testing.T, manager *StreamManager, vchannel string, count int) {
	t.Helper()
	require.Eventually(t, func() bool {
		manager.streamMu.Lock()
		defer manager.streamMu.Unlock()
		return len(manager.streamsByV[vchannel]) == count
	}, time.Second, time.Millisecond)
}

func waitStreamSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for stream signal")
	}
}

type gatedStreamHandler struct {
	*recordingStreamHandler
	match       func(wal.TransformLogStreamEvent) bool
	entered     chan struct{}
	release     chan struct{}
	gateOnce    sync.Once
	releaseOnce sync.Once
}

func newGatedStreamHandler(match func(wal.TransformLogStreamEvent) bool) *gatedStreamHandler {
	return &gatedStreamHandler{
		recordingStreamHandler: newRecordingStreamHandler(),
		match:                  match,
		entered:                make(chan struct{}),
		release:                make(chan struct{}),
	}
}

func (h *gatedStreamHandler) Handle(event wal.TransformLogStreamEvent) error {
	if h.match(event) {
		h.gateOnce.Do(func() {
			close(h.entered)
			<-h.release
		})
	}
	return h.recordingStreamHandler.Handle(event)
}

func (h *gatedStreamHandler) unblock() {
	h.releaseOnce.Do(func() { close(h.release) })
}

type rejectingEntryHandler struct {
	*recordingStreamHandler
}

func (h *rejectingEntryHandler) Handle(event wal.TransformLogStreamEvent) error {
	if event.Entry != nil {
		return io.ErrClosedPipe
	}
	return h.recordingStreamHandler.Handle(event)
}
