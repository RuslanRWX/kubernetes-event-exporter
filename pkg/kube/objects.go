package kube

import (
	"context"
	"strings"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru"
	"github.com/resmoio/kubernetes-event-exporter/pkg/metrics"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/restmapper"
)

type ObjectMetadataProvider interface {
	GetObjectMetadata(reference *v1.ObjectReference, clientset *kubernetes.Clientset, dynClient dynamic.Interface, metricsStore *metrics.Store) (ObjectMetadata, error)
}

type ObjectMetadataCache struct {
	cache *lru.ARCCache

	// mapperMu guards mapper and lastMapperReset; the mapper itself is safe for
	// concurrent use once built.
	mapperMu        sync.Mutex
	mapper          meta.ResettableRESTMapper
	lastMapperReset time.Time
}

// mapperResetCooldown throttles discovery refreshes. Without it, a stream of
// events referencing a kind that genuinely cannot be resolved (a removed CRD,
// say) would trigger a full re-discovery per event -- exactly the behaviour this
// cache exists to avoid.
const mapperResetCooldown = time.Minute

var _ ObjectMetadataProvider = &ObjectMetadataCache{}

type ObjectMetadata struct {
	Annotations     map[string]string
	Labels          map[string]string
	OwnerReferences []metav1.OwnerReference
	Deleted         bool
}

func NewObjectMetadataProvider(size int) ObjectMetadataProvider {
	cache, err := lru.NewARC(size)
	if err != nil {
		panic("cannot init cache: " + err.Error())
	}

	var o ObjectMetadataProvider = &ObjectMetadataCache{
		cache: cache,
	}

	return o
}

func (o *ObjectMetadataCache) GetObjectMetadata(reference *v1.ObjectReference, clientset *kubernetes.Clientset, dynClient dynamic.Interface, metricsStore *metrics.Store) (ObjectMetadata, error) {
	// ResourceVersion changes when the object is updated.
	// We use "UID/ResourceVersion" as cache key so that if the object is updated we get the new metadata.
	cacheKey := strings.Join([]string{string(reference.UID), reference.ResourceVersion}, "/")
	if val, ok := o.cache.Get(cacheKey); ok {
		metricsStore.KubeApiReadCacheHits.Inc()
		return val.(ObjectMetadata), nil
	}

	var group, version string
	s := strings.Split(reference.APIVersion, "/")
	if len(s) == 1 {
		group = ""
		version = s[0]
	} else {
		group = s[0]
		version = s[1]
	}

	gk := schema.GroupKind{Group: group, Kind: reference.Kind}

	mapping, err := o.restMapping(clientset, gk, version)
	if err != nil {
		return ObjectMetadata{}, err
	}

	metricsStore.KubeApiReadRequests.Inc()

	item, err := dynClient.
		Resource(mapping.Resource).
		Namespace(reference.Namespace).
		Get(context.Background(), reference.Name, metav1.GetOptions{})
	if err != nil {
		return ObjectMetadata{}, err
	}

	objectMetadata := ObjectMetadata{
		OwnerReferences: item.GetOwnerReferences(),
		Labels:          item.GetLabels(),
		Annotations:     item.GetAnnotations(),
	}

	if item.GetDeletionTimestamp() != nil {
		objectMetadata.Deleted = true
	}

	o.cache.Add(cacheKey, objectMetadata)
	return objectMetadata, nil
}

// restMapping returns a lazily built RESTMapper backed by an in-memory discovery
// cache. Discovery is expensive -- it costs one request for the API group list
// plus one per group-version -- so it must not be repeated per event. The mapper
// is reset and retried once on a no-match so that resources from CRDs installed
// after start-up are still resolved.
func (o *ObjectMetadataCache) restMapping(clientset *kubernetes.Clientset, gk schema.GroupKind, version string) (*meta.RESTMapping, error) {
	o.mapperMu.Lock()
	if o.mapper == nil {
		o.mapper = restmapper.NewDeferredDiscoveryRESTMapper(memory.NewMemCacheClient(clientset.Discovery()))
	}
	mapper := o.mapper
	o.mapperMu.Unlock()

	mapping, err := mapper.RESTMapping(gk, version)
	if err != nil && meta.IsNoMatchError(err) && o.tryResetMapper() {
		mapping, err = mapper.RESTMapping(gk, version)
	}
	if err != nil {
		return nil, err
	}

	return mapping, nil
}

// tryResetMapper drops the cached discovery data so that kinds added since
// start-up become resolvable, at most once per mapperResetCooldown. It reports
// whether a reset actually happened.
func (o *ObjectMetadataCache) tryResetMapper() bool {
	o.mapperMu.Lock()
	defer o.mapperMu.Unlock()

	if time.Since(o.lastMapperReset) < mapperResetCooldown {
		return false
	}
	o.lastMapperReset = time.Now()
	o.mapper.Reset()
	return true
}
