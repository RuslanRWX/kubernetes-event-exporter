package kube

import (
	"sync"
	"time"

	"github.com/resmoio/kubernetes-event-exporter/pkg/metrics"
	"github.com/rs/zerolog/log"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
)

var startUpTime = time.Now()

type EventHandler func(event *EnhancedEvent)

type EventWatcher struct {
	wg                  sync.WaitGroup
	informer            cache.SharedInformer
	stopper             chan struct{}
	objectMetadataCache ObjectMetadataProvider
	omitLookup          bool
	fn                  EventHandler
	maxEventAgeSeconds  time.Duration
	metricsStore        *metrics.Store
	dynamicClient       *dynamic.DynamicClient
	clientset           *kubernetes.Clientset
}

func NewEventWatcher(config *rest.Config, namespace string, MaxEventAgeSeconds int64, metricsStore *metrics.Store, fn EventHandler, omitLookup bool, cacheSize int) *EventWatcher {
	clientset := kubernetes.NewForConfigOrDie(config)
	factory := informers.NewSharedInformerFactoryWithOptions(clientset, 0, informers.WithNamespace(namespace))
	informer := factory.Core().V1().Events().Informer()

	watcher := &EventWatcher{
		informer:            informer,
		stopper:             make(chan struct{}),
		objectMetadataCache: NewObjectMetadataProvider(cacheSize),
		omitLookup:          omitLookup,
		fn:                  fn,
		maxEventAgeSeconds:  time.Second * time.Duration(MaxEventAgeSeconds),
		metricsStore:        metricsStore,
		dynamicClient:       dynamic.NewForConfigOrDie(config),
		clientset:           clientset,
	}

	if _, err := informer.AddEventHandler(watcher); err != nil {
		log.Fatal().Err(err).Msg("cannot register event handler on the informer")
	}
	if err := informer.SetWatchErrorHandler(func(r *cache.Reflector, err error) {
		watcher.metricsStore.WatchErrors.Inc()
	}); err != nil {
		log.Fatal().Err(err).Msg("cannot set watch error handler on the informer")
	}

	return watcher
}

// OnAdd implements cache.ResourceEventHandler. isInInitialList is true while the
// informer replays the pre-existing events from its initial LIST; those are
// filtered by maxEventAgeSeconds in isEventDiscarded like any other event.
func (e *EventWatcher) OnAdd(obj interface{}, isInInitialList bool) {
	event, ok := obj.(*corev1.Event)
	if !ok {
		log.Error().Msgf("expected a *corev1.Event from the informer, got %T", obj)
		return
	}
	e.onEvent(event)
}

func (e *EventWatcher) OnUpdate(oldObj, newObj interface{}) {
	// Ignore updates
}

// Ignore events older than the maxEventAgeSeconds
func (e *EventWatcher) isEventDiscarded(event *corev1.Event) bool {
	timestamp := event.LastTimestamp.Time
	if timestamp.IsZero() {
		timestamp = event.EventTime.Time
	}
	eventAge := time.Since(timestamp)
	if eventAge > e.maxEventAgeSeconds {
		// Log discarded events if they were created after the watcher started
		// (to suppres warnings from initial synchrnization)
		if timestamp.After(startUpTime) {
			log.Warn().
				Str("event age", eventAge.String()).
				Str("event namespace", event.Namespace).
				Str("event name", event.Name).
				Msg("Event discarded as being older then maxEventAgeSeconds")
			e.metricsStore.EventsDiscarded.Inc()
		}
		return true
	}
	return false
}

func (e *EventWatcher) onEvent(event *corev1.Event) {
	if e.isEventDiscarded(event) {
		return
	}

	log.Debug().
		Str("msg", event.Message).
		Str("namespace", event.Namespace).
		Str("reason", event.Reason).
		Str("involvedObject", event.InvolvedObject.Name).
		Msg("Received event")

	e.metricsStore.EventsProcessed.Inc()

	ev := &EnhancedEvent{
		Event: *event.DeepCopy(),
	}
	ev.Event.ManagedFields = nil

	if e.omitLookup {
		ev.InvolvedObject.ObjectReference = *event.InvolvedObject.DeepCopy()
	} else {
		objectMetadata, err := e.objectMetadataCache.GetObjectMetadata(&event.InvolvedObject, e.clientset, e.dynamicClient, e.metricsStore)
		if err != nil {
			if errors.IsNotFound(err) {
				// Routine: events routinely outlive the object they describe
				// (pod Killing/Deleted events in particular). Logging this at
				// error level produces a steady stream of false alarms.
				ev.InvolvedObject.Deleted = true
				log.Debug().Err(err).Msg("Object not found, likely deleted")
			} else {
				log.Error().Err(err).Msg("Failed to get object metadata")
			}
			ev.InvolvedObject.ObjectReference = *event.InvolvedObject.DeepCopy()
		} else {
			ev.InvolvedObject.Labels = objectMetadata.Labels
			ev.InvolvedObject.Annotations = objectMetadata.Annotations
			ev.InvolvedObject.OwnerReferences = objectMetadata.OwnerReferences
			ev.InvolvedObject.ObjectReference = *event.InvolvedObject.DeepCopy()
			ev.InvolvedObject.Deleted = objectMetadata.Deleted
		}
	}

	e.fn(ev)
}

func (e *EventWatcher) OnDelete(obj interface{}) {
	// Ignore deletes
}

func (e *EventWatcher) Start() {
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		e.informer.Run(e.stopper)
	}()
}

func (e *EventWatcher) Stop() {
	close(e.stopper)
	e.wg.Wait()
}

func (e *EventWatcher) setStartUpTime(time time.Time) {
	startUpTime = time
}
