package whelk.component

import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheLoader
import com.google.common.cache.LoadingCache
import com.google.common.util.concurrent.ListenableFuture
import com.google.common.util.concurrent.ListenableFutureTask
import com.google.common.util.concurrent.ThreadFactoryBuilder
import groovy.util.logging.Log4j2 as Log
import io.prometheus.client.guava.cache.CacheMetricsCollector
import whelk.Document
import whelk.Link
import whelk.exception.MissingMainIriException

import java.util.concurrent.Callable
import java.util.concurrent.Executor
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.function.BiFunction
import java.util.function.Supplier

import static whelk.component.PostgreSQLComponent.NotificationType.DEPENDENCY_CACHE_INVALIDATE

@Log
class DependencyCache {
    private static final int CACHE_SIZE = 50_000
    private static final int REFRESH_INTERVAL_MINUTES = 5

    private static final CacheMetricsCollector cacheMetrics = new CacheMetricsCollector().register()

    PostgreSQLComponent storage

    private Executor cacheRefresher = Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder().setDaemon(true).build())

    private LoadingCache<Link, Set<String>> dependersCache = CacheBuilder.newBuilder()
            .maximumSize(CACHE_SIZE)
            .refreshAfterWrite(REFRESH_INTERVAL_MINUTES, TimeUnit.MINUTES)
            .recordStats()
            .build(loader(storage.&getDependersOfType))

    private LoadingCache<Link, Set<String>> dependenciesCache = CacheBuilder.newBuilder()
            .maximumSize(CACHE_SIZE)
            .refreshAfterWrite(REFRESH_INTERVAL_MINUTES, TimeUnit.MINUTES)
            .recordStats()
            .build(loader(storage.&getDependenciesOfType))
    
    DependencyCache(PostgreSQLComponent storage) {
        this.storage = storage

        cacheMetrics.addCache('dependersCache', dependersCache)
        cacheMetrics.addCache('dependencyCache', dependenciesCache)
    }

    Set<String> getDependenciesOfType(String iri, String typeOfRelation) {
        return dependenciesCache.getUnchecked(new Link(iri: iri, relation: typeOfRelation))
    }

    Set<String> getDependersOfType(String iri, String typeOfRelation) {
        return dependersCache.getUnchecked(new Link(iri: iri, relation: typeOfRelation))
    }

    void invalidate(Document createdDoc) {
        def i = []
        createdDoc.getThingIdentifiers().each { fromIri ->
            createdDoc.getExternalRefs().each { link ->
                i << new Tuple2(fromIri, link)
            }
        }
        _invalidate(i)
    }

    void invalidate(Document preUpdateDoc, Document postUpdateDoc) {
        Set thingIris = new HashSet<>()
        thingIris.addAll(preUpdateDoc.getThingIdentifiers())
        thingIris.addAll(postUpdateDoc.getThingIdentifiers())

        Set<Link> before = preUpdateDoc.getExternalRefs()
        Set<Link> after = postUpdateDoc.deleted ? Collections.emptySet() : postUpdateDoc.getExternalRefs()
        Set<Link> added = (after - before)
        Set<Link> removed = (before - after)

        def i = []
        (added + removed).each { Link link ->
            thingIris.each { fromIri ->
                i << new Tuple2(fromIri, link)
            }
        }
        _invalidate(i)
    }

    private void _invalidate(Collection<Tuple2<String, Link>> fromTo) {
        fromTo.each { String fromIri, Link link ->
            invalidate(fromIri, link)
        }
        
        def notification = fromTo.collect{ String fromIri, Link link ->
            "$fromIri ${link.relation} ${link.iri}"
        }
        log.debug("Sending {}", notification)
        storage.sendNotification(DEPENDENCY_CACHE_INVALIDATE, notification)
    }
    
    void handleInvalidateNotification(List<String> payload) {
        payload.each {
            def s = it.split(' ')
            def (fromIri, relation, toIri) = [s[0], s[1], s[2]]
            log.debug("Invalidate {} {} {}", fromIri, relation, toIri)
            invalidate(fromIri, new Link(relation: relation, iri: toIri))
        }
    }
    
    void invalidate(String fromIri, Link link) {
        dependersCache.invalidate(link)
        dependenciesCache.invalidate(new Link(iri: fromIri, relation: link.relation))
    }
    
    void invalidateAll() {
        dependenciesCache.invalidateAll()
        dependersCache.invalidateAll()
    }

    void logStats() {
        log.info("dependersCache: ${dependersCache.stats()}")
        log.info("dependenciesCache: ${dependenciesCache.stats()}")
    }
    
    private CacheLoader<Link, Set<String>> loader(BiFunction<String, String, Set> func) {
        return new CacheLoader<Link, Set<String>>() {
            @Override
            Set<String> load(Link link) {
                try {
                    def iris = func.apply(storage.getSystemIdByThingId(link.iri), link.relation)
                            .findResults (this.&tryGetThingMainIriBySystemId)

                    return iris.isEmpty()
                            ? Collections.EMPTY_SET
                            : Collections.unmodifiableSet(new HashSet(iris))
                }
                catch (MissingMainIriException e) {
                    log.warn("Missing Main IRI: $e")
                    return Collections.EMPTY_SET
                }
            }
            
            private String tryGetThingMainIriBySystemId(String systemId) {
                try {
                    return storage.getThingMainIriBySystemId(systemId)
                }
                catch (MissingMainIriException ignored) {
                    log.warn("Missing thing main IRI for $systemId. Deleted?")
                    return null
                }
            }

            @Override
            ListenableFuture<Set<String>> reload(Link key, Set<String> oldValue) throws Exception {
                return reloadTask( { load(key) } )
            }
        }
    }

    private <V> ListenableFutureTask<V> reloadTask(Supplier<V> reloadFunction) {
        ListenableFutureTask<V> task = ListenableFutureTask.create(new Callable<V>() {
            @Override
            V call() throws Exception {
                return reloadFunction.get()
            }
        })

        cacheRefresher.execute(task)
        return task
    }
}
