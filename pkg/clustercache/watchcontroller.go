package clustercache

import (
	"fmt"
	"reflect"
	"time"

	"github.com/opencost/opencost/core/pkg/log"

	"k8s.io/apimachinery/pkg/fields"
	rt "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
)

// Type alias for a receiver func
type WatchHandler = func(interface{})

// WatchController defines a contract for an object which watches a specific resource set for
// add, updates, and removals
type WatchController interface {
	// Initializes the cache
	WarmUp(chan struct{})

	// Run starts the watching process
	Run(int, chan struct{})

	// GetAll returns all of the resources
	GetAll() []interface{}

	// SetUpdateHandler sets a specific handler for adding/updating individual resources
	SetUpdateHandler(WatchHandler) WatchController

	// SetRemovedHandler sets a specific handler for removing individual resources
	SetRemovedHandler(WatchHandler) WatchController
}

// CachingWatchController composites the watching behavior and a cache to ensure that all
// up to date resources are readily available
type CachingWatchController struct {
	indexer  cache.Indexer
	queue    workqueue.RateLimitingInterface
	informer cache.Controller

	resource     string
	resourceType string

	updateHandler WatchHandler
	removeHandler WatchHandler
}

func NewCachingWatcher(restClient rest.Interface, resource string, resourceType rt.Object, namespace string, fieldSelector fields.Selector) WatchController {
	resourceCache := cache.NewListWatchFromClient(restClient, resource, namespace, fieldSelector)
	queue := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())
	indexer, informer := cache.NewIndexerInformer(resourceCache, resourceType, 0, cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			key, err := cache.MetaNamespaceKeyFunc(obj)
			if err == nil {
				queue.Add(key)
			}
		},
		UpdateFunc: func(old interface{}, new interface{}) {
			key, err := cache.MetaNamespaceKeyFunc(new)
			if err == nil {
				queue.Add(key)
			}
		},
		DeleteFunc: func(obj interface{}) {
			// IndexerInformer uses a delta queue, therefore for deletes we have to use this
			// key function.
			key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
			if err == nil {
				queue.Add(key)
			}
		},
	}, cache.Indexers{})

	return &CachingWatchController{
		indexer:      indexer,
		queue:        queue,
		informer:     informer,
		resource:     resource,
		resourceType: reflect.TypeOf(resourceType).String(),
	}
}

func (c *CachingWatchController) GetAll() []interface{} {
	list := c.indexer.List()

	// since the indexer returns the as-is pointer to the resource,
	// we deep copy the resources such that callers don't corrupt the
	// index
	cloneList := make([]interface{}, 0, len(list))
	for _, v := range list {
		if deepCopyable, ok := v.(rt.Object); ok {
			cloneList = append(cloneList, deepCopyable.DeepCopyObject())
		}
	}

	return cloneList
}

func (c *CachingWatchController) SetUpdateHandler(handler WatchHandler) WatchController {
	c.updateHandler = handler
	return c
}

func (c *CachingWatchController) SetRemovedHandler(handler WatchHandler) WatchController {
	c.removeHandler = handler
	return c
}

func (c *CachingWatchController) processNextItem() bool {
	// Wait until there is a new item in the working queue
	key, quit := c.queue.Get()
	if quit {
		return false
	}
	// Tell the queue that we are done with processing this key. This unblocks the key for other workers
	// This allows safe parallel processing because two pods with the same key are never processed in
	// parallel.
	defer c.queue.Done(key)

	// Invoke the method containing the business logic
	err := c.handle(key.(string))
	// Handle the error if something went wrong during the execution of the business logic
	c.handleErr(err, key)
	return true
}

// handle is the business logic of the controller.
func (c *CachingWatchController) handle(key string) error {
	obj, exists, err := c.indexer.GetByKey(key)
	if err != nil {
		log.Errorf("Fetching %s with key %s from store failed with %v", c.resourceType, key, err)
		return err
	}

	if !exists {
		if c.removeHandler != nil {
			c.removeHandler(key)
		}
	} else {
		if c.updateHandler != nil {
			c.updateHandler(obj)
		}
	}
	return nil
}

// handleErr checks if an error happened and makes sure we will retry later.
func (c *CachingWatchController) handleErr(err error, key interface{}) {
	if err == nil {
		// Forget about the #AddRateLimited history of the key on every successful synchronization.
		// This ensures that future processing of updates for this key is not delayed because of
		// an outdated error history.
		c.queue.Forget(key)
		return
	}

	// This controller retries 5 times if something goes wrong. After that, it stops trying.
	if c.queue.NumRequeues(key) < 5 {
		log.Errorf("Error syncing %s %v: %v", c.resourceType, key, err)

		// Re-enqueue the key rate limited. Based on the rate limiter on the
		// queue and the re-enqueue history, the key will be processed later again.
		c.queue.AddRateLimited(key)
		return
	}

	c.queue.Forget(key)
	// Report to an external entity that, even after several retries, we could not successfully process this key
	runtime.HandleError(err)
	log.Infof("Dropping %s %q out of the queue: %v", c.resourceType, key, err)
}

func (c *CachingWatchController) WarmUp(cancelCh chan struct{}) {
	go c.informer.Run(cancelCh)

	// Wait for all involved caches to be synced, before processing items from the queue is started
	if !cache.WaitForCacheSync(cancelCh, c.informer.HasSynced) {
		runtime.HandleError(fmt.Errorf("Timed out waiting for caches to sync"))
		return
	}
}

func (c *CachingWatchController) Run(threadiness int, stopCh chan struct{}) {
	defer runtime.HandleCrash()

	// Let the workers stop when we are done
	defer c.queue.ShutDown()
	log.Infof("Starting %s controller", c.resourceType)

	for i := 0; i < threadiness; i++ {
		go wait.Until(c.runWorker, time.Second, stopCh)
	}

	<-stopCh
	log.Infof("Stopping %s controller", c.resourceType)
}

func (c *CachingWatchController) runWorker() {
	for c.processNextItem() {
	}
}
