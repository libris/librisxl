package whelk

import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheLoader
import com.google.common.cache.LoadingCache
import com.google.common.util.concurrent.ListenableFuture
import com.google.common.util.concurrent.ListenableFutureTask
import com.google.common.util.concurrent.ThreadFactoryBuilder
import whelk.component.PostgreSQLComponent

import java.util.concurrent.Callable
import java.util.concurrent.Executor
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.function.Supplier;

class Relations {
    public static final List<String> BROADER_RELATIONS = ['broader', 'broadMatch', 'exactMatch']

    PostgreSQLComponent storage
    private Executor cacheRefresher = Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder().setDaemon(true).build())

    private LoadingCache<String, Set<String>> broaderCache = CacheBuilder.newBuilder()
            .maximumSize(100)
            .refreshAfterWrite(5, TimeUnit.MINUTES)
            .build(new CacheLoader<String, Set<String>>() {
                @Override
                Set<String> load(String iri) {
                    return Collections.unmodifiableSet(computeInverseBroaderRelations(iri))
                }

                @Override
                ListenableFuture<Set<String>> reload(String iri, Set<String> oldValue) throws Exception {
                    return reloadTask( { load(iri) } )
                }
            })

    private LoadingCache<Tuple2<String,String>, Set<String>> dependencyCache = CacheBuilder.newBuilder()
            .maximumSize(1000)
            .refreshAfterWrite(5, TimeUnit.MINUTES)
            .build(new CacheLoader<Tuple2<String,String>, Set<String>>() {
                @Override
                Set<String> load(Tuple2<String,String> iriAndRelation) {
                    String iri = iriAndRelation.first
                    String typeOfRelation = iriAndRelation.second
                    return Collections.unmodifiableSet(new HashSet(storage
                            .getDependenciesOfType(storage.getSystemIdByThingId(iri), typeOfRelation)
                            .collect(storage.&getThingMainIriBySystemId)))
                }

                @Override
                ListenableFuture<Set<String>> reload(Tuple2<String,String> key, Set<String> oldValue) throws Exception {
                    return reloadTask( { load(key) } )
                }
            })

    Relations(PostgreSQLComponent storage) {
        this.storage = storage
    }

    boolean isImpliedBy(String broaderIri, String narrowerIri) {
        Set<String> visited = []
        List<String> stack = [narrowerIri]

        while (!stack.isEmpty()) {
            String iri = stack.pop()
            for (String relation : BROADER_RELATIONS) {
                Set<String> dependencies = new HashSet<>(getDependenciesOfType(iri, relation))
                if (dependencies.contains(broaderIri)) {
                    return true
                }
                dependencies.removeAll(visited)
                visited.addAll(dependencies)
                stack.addAll(dependencies)
            }
        }

        return false
    }

    Set<String> findInverseBroaderRelations(String iri) {
        return broaderCache.getUnchecked(iri)
    }

    private Set<String> getDependenciesOfType(String id, String typeOfRelation) {
        return dependencyCache.getUnchecked(new Tuple2<>(id, typeOfRelation))
    }

    private Set<String> computeInverseBroaderRelations(String iri) {
        Set<String> ids = []
        List<String> stack = [storage.getSystemIdByIri(iri)]
        while (!stack.isEmpty()) {
            String id = stack.pop()
            for (String relation : BROADER_RELATIONS ) {
                List<String> dependers = storage.getDependersOfType(id, relation)
                dependers.removeAll(ids)
                stack.addAll(dependers)
                ids.addAll(dependers)
            }
        }
        return new HashSet<>(ids.collect(storage.&getThingMainIriBySystemId))
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
