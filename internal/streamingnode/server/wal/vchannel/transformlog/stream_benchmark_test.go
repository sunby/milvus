package transformlog

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
)

// BenchmarkTransformLogNotification measures a frontier update through the
// actual stream handler. Only v0 changes; other streams subscribe to cold
// vchannels. All registered vchannels have previously published a frontier.
func BenchmarkTransformLogNotification(b *testing.B) {
	for _, vchannels := range []int{1000, 50000} {
		for _, streams := range []int{1, 32} {
			b.Run(fmt.Sprintf("vchannels=%d/streams=%d", vchannels, streams), func(b *testing.B) {
				manager := NewStreamManager("pchannel")
				var hot *TransformLog
				for i := 0; i < vchannels; i++ {
					vchannel := fmt.Sprintf("v%d", i)
					log := New(Config{VChannel: vchannel})
					manager.Register(vchannel, log)
					log.syncUp(1)
					if i == 0 {
						hot = log
					}
				}
				var delivered <-chan uint64
				for i := 0; i < streams; i++ {
					stream, err := manager.AcquireStream(context.Background(), "pchannel")
					require.NoError(b, err)
					b.Cleanup(func() { require.NoError(b, stream.Close()) })
					handler := &benchmarkStreamHandler{frontiers: make(chan uint64, 1)}
					_, err = stream.Subscribe(context.Background(), wal.TransformLogSubscriptionOption{
						VChannel: fmt.Sprintf("v%d", i),
						Handler:  handler,
					})
					require.NoError(b, err)
					require.Equal(b, uint64(1), <-handler.frontiers)
					if i == 0 {
						delivered = handler.frontiers
					}
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					frontier := uint64(i + 2)
					hot.syncUp(frontier)
					if got := <-delivered; got != frontier {
						b.Fatalf("frontier: got %d, want %d", got, frontier)
					}
				}
				b.StopTimer()
			})
		}
	}
}

type benchmarkStreamHandler struct {
	frontiers chan uint64
}

func (h *benchmarkStreamHandler) Handle(event wal.TransformLogStreamEvent) error {
	if event.SyncUp != nil {
		h.frontiers <- event.SyncUp.TimeTick
	}
	return nil
}

func (*benchmarkStreamHandler) Close() {}

// A large earlier notification batch must not make later sparse drains scan
// the retained capacity of a mostly empty map.
func BenchmarkStreamNotificationsAfterBurst(b *testing.B) {
	for _, burst := range []int{1, 50000} {
		b.Run(fmt.Sprintf("burst=%d", burst), func(b *testing.B) {
			notifications := newStreamNotifications()
			for i := 0; i < burst; i++ {
				notifications.notify(fmt.Sprintf("v%d", i))
			}
			<-notifications.ready
			require.Len(b, notifications.takePending(), burst)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				notifications.notify("v0")
				<-notifications.ready
				if pending := notifications.takePending(); len(pending) != 1 || pending[0] != "v0" {
					b.Fatalf("unexpected pending vchannels: %v", pending)
				}
			}
		})
	}
}
