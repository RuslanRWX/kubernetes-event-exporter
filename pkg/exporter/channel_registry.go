package exporter

import (
	"context"
	"sync"

	"github.com/resmoio/kubernetes-event-exporter/pkg/kube"
	"github.com/resmoio/kubernetes-event-exporter/pkg/metrics"
	"github.com/resmoio/kubernetes-event-exporter/pkg/sinks"
	"github.com/rs/zerolog/log"
)

// defaultSinkBufferSize is how many events may be queued per sink while that sink
// is busy. Events arriving on a full queue are dropped and counted in the
// events_dropped metric, which bounds the exporter's memory during an event storm
// or a sink outage. This mirrors fluent-bit's Mem_Buf_Limit.
const defaultSinkBufferSize = 1024

// ChannelBasedReceiverRegistry creates two channels for each receiver. One is for receiving events and other one is
// for breaking out of the infinite loop. Each message is passed to receivers
// On closing, the registry sends a signal on all exit channels, and then waits for all to complete.
type ChannelBasedReceiverRegistry struct {
	ch           map[string]chan kube.EnhancedEvent
	exitCh       map[string]chan interface{}
	wg           *sync.WaitGroup
	MetricsStore *metrics.Store
	// BufferSize overrides defaultSinkBufferSize when non-zero.
	BufferSize int
}

func (r *ChannelBasedReceiverRegistry) SendEvent(name string, event *kube.EnhancedEvent) {
	ch := r.ch[name]
	if ch == nil {
		log.Error().Str("name", name).Msg("There is no channel")
		return
	}

	// A non-blocking send on a buffered channel. The previous implementation
	// spawned a goroutine per event onto an unbuffered channel, so a slow or
	// unreachable sink accumulated unbounded goroutines (each holding a copy of
	// the event) until the process was OOM-killed.
	select {
	case ch <- *event:
	default:
		r.MetricsStore.EventsDropped.Inc()
		log.Warn().
			Str("sink", name).
			Str("event", event.Message).
			Msg("Sink queue is full, dropping event")
	}
}

func (r *ChannelBasedReceiverRegistry) Register(name string, receiver sinks.Sink) {
	if r.ch == nil {
		r.ch = make(map[string]chan kube.EnhancedEvent)
		r.exitCh = make(map[string]chan interface{})
	}

	bufferSize := r.BufferSize
	if bufferSize <= 0 {
		bufferSize = defaultSinkBufferSize
	}

	ch := make(chan kube.EnhancedEvent, bufferSize)
	exitCh := make(chan interface{})

	r.ch[name] = ch
	r.exitCh[name] = exitCh

	if r.wg == nil {
		r.wg = &sync.WaitGroup{}
	}
	r.wg.Add(1)

	go func() {
		send := func(ev *kube.EnhancedEvent) {
			log.Debug().Str("sink", name).Str("event", ev.Message).Msg("sending event to sink")
			if err := receiver.Send(context.Background(), ev); err != nil {
				r.MetricsStore.SendErrors.Inc()
				log.Error().Err(err).Str("sink", name).Str("event", ev.Message).Msg("Cannot send event")
			}
		}

	Loop:
		for {
			select {
			case ev := <-ch:
				send(&ev)
			case <-exitCh:
				log.Info().Str("sink", name).Msg("Closing the sink")
				break Loop
			}
		}

		// Flush whatever is still queued so that a graceful shutdown does not
		// discard up to bufferSize events.
		for {
			select {
			case ev := <-ch:
				send(&ev)
			default:
				receiver.Close()
				log.Info().Str("sink", name).Msg("Closed")
				r.wg.Done()
				return
			}
		}
	}()
}

// Close signals closing to all sinks and waits for them to complete.
// The wait could block indefinitely depending on the sink implementations.
func (r *ChannelBasedReceiverRegistry) Close() {
	// Send exit command and wait for exit of all sinks
	for _, ec := range r.exitCh {
		ec <- 1
	}
	r.wg.Wait()
}
