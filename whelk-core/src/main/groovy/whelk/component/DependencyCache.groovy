package whelk.component

import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheLoader
import com.google.common.cache.LoadingCache
import com.google.common.util.concurrent.ListenableFuture
import com.google.common.util.concurrent.ListenableFutureTask
import com.google.common.util.concurrent.ThreadFactoryBuilder
import whelk.Document

import java.util.concurrent.Callable
import java.util.concurrent.Executor
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.function.BiFunction
import java.util.function.Supplier

class DependencyCache {
    PostgreSQLComponent storage

    private Executor cacheRefresher = Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder().setDaemon(true).build())

    private LoadingCache<Tuple2<String,String>, Set<String>> dependersCache = CacheBuilder.newBuilder()
            .maximumSize(1000)
            .refreshAfterWrite(5, TimeUnit.MINUTES)
            .build(loader(storage.&getDependersOfType))

    private LoadingCache<Tuple2<String,String>, Set<String>> dependenciesCache = CacheBuilder.newBuilder()
            .maximumSize(1000)
            .refreshAfterWrite(5, TimeUnit.MINUTES)
            .build(loader(storage.&getDependenciesOfType))

    DependencyCache(PostgreSQLComponent storage) {
        this.storage = storage
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
