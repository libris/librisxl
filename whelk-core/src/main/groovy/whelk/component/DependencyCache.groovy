package whelk.component

import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheLoader
import com.google.common.cache.LoadingCache
import com.google.common.util.concurrent.ListenableFuture
import com.google.common.util.concurrent.ListenableFutureTask
import com.google.common.util.concurrent.ThreadFactoryBuilder
import io.prometheus.client.guava.cache.CacheMetricsCollector
import whelk.Document

import java.util.concurrent.Callable
import java.util.concurrent.Executor
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.function.BiFunction
import java.util.function.Supplier

class DependencyCache {
    private static final int CACHE_SIZE = 10_000
    private static final int REFRESH_INTERVAL_MINUTES = 5

    private static final CacheMetricsCollector cacheMetrics = new CacheMetricsCollector().register()

    PostgreSQLComponent storage

    private Executor cacheRefresher = Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder().setDaemon(true).build())

    private LoadingCache<Tuple2<String,String>, Set<String>> dependersCache = CacheBuilder.newBuilder()
            .maximumSize(CACHE_SIZE)
            .refreshAfterWrite(REFRESH_INTERVAL_MINUTES, TimeUnit.MINUTES)
            .recordStats()
            .build(loader(storage.&getDependersOfType))

    private LoadingCache<Tuple2<String,String>, Set<String>> dependenciesCache = CacheBuilder.newBuilder()
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
        return dependenciesCache.getUnchecked(new Tuple2<>(iri, typeOfRelation))
    }

    Set<String> getDependersOfType(String iri, String typeOfRelation) {
        return dependersCache.getUnchecked(new Tuple2<>(iri, typeOfRelation))
    }

    void invalidate(Document doc) {
        String docIri = doc.getThingIdentifiers()[0]
        doc.getRefsWithRelation().each {
            String relation = it[0]
            String iri = it[1]
            dependersCache.invalidate(new Tuple2(iri, relation))
            dependenciesCache.invalidate(new Tuple2(docIri, relation))
        }
    }

    private CacheLoader<Tuple2<String,String>, Set<String>> loader(BiFunction<String, String, Set> func) {
        return new CacheLoader<Tuple2<String,String>, Set<String>>() {
            @Override
            Set<String> load(Tuple2<String,String> iriAndRelation) {
                String iri = iriAndRelation.first
                String typeOfRelation = iriAndRelation.second
                def iris = func.apply(storage.getSystemIdByThingId(iri), typeOfRelation)
                        .collect(storage.&getThingMainIriBySystemId)

                return iris.isEmpty()
                        ? Collections.EMPTY_SET
                        : Collections.unmodifiableSet(new HashSet(iris))
            }

            @Override
            ListenableFuture<Set<String>> reload(Tuple2<String, String> key, Set<String> oldValue) throws Exception {
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
