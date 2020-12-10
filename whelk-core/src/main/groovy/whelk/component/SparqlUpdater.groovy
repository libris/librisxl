package whelk.component

import com.google.common.util.concurrent.MoreExecutors
import com.google.common.util.concurrent.ThreadFactoryBuilder
import groovy.transform.CompileStatic
import groovy.util.logging.Log4j2 as Log
import org.apache.http.client.HttpClient
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.impl.conn.PoolingClientConnectionManager
import org.apache.http.params.HttpConnectionParams
import org.apache.http.params.HttpParams
import whelk.Document

import javax.sql.DataSource
import java.util.concurrent.ExecutorService
import java.util.concurrent.LinkedBlockingDeque
import java.util.concurrent.ThreadFactory
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

@Log
@CompileStatic
class SparqlUpdater {
    private static final long PERIODIC_CHECK_MS = 30 * 1000

    private static final int NUM_WORKERS = 2
    private static final int MAX_CONNECTION_POOL_SIZE = 8

    // HTTP timeout parameters
    private static final int CONNECT_TIMEOUT_MS = 5 * 1000
    private static final int READ_TIMEOUT_MS = 60 * 1000

    // Number of items to take from queue each time
    private static final int QUEUE_TAKE_NUM = 1

    private final ExecutorService executorService = buildExecutorService()
    private final Runnable task
    private final Timer timer = new Timer()

    SparqlUpdater(PostgreSQLComponent storage, Virtuoso sparql) {
        int poolSize = Math.min(NUM_WORKERS, MAX_CONNECTION_POOL_SIZE)
        DataSource connectionPool = storage.createAdditionalConnectionPool(this.getClass().getSimpleName(), poolSize)

        PostgreSQLComponent.QueueHandler handler = { Document doc ->
            try {
                if (doc.deleted) {
                    sparql.deleteNamedGraph(doc)
                }
                else {
                    sparql.insertNamedGraph(doc)
                }
                return true
            }
            catch (Exception e) {
                log.warn("Failed, will retry: $e")
                return false
            }
        }

        task = {
            try {
                // Run as long as there might be docs in the queue and we haven't failed
                if (storage.sparqlQueueTake(handler, QUEUE_TAKE_NUM, connectionPool)) {
                    poke()
                }
            }
            catch (Exception e) {
                log.warn("Error executing task: $e", e)
            }
        }

        timer.scheduleAtFixedRate({ poke() }, PERIODIC_CHECK_MS, PERIODIC_CHECK_MS)
    }

    /**
     * Force polling SPARQL update queue now
     */
    void poke() {
        executorService.submit(task)
    }

    private ExecutorService buildExecutorService() {
        ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("${getClass().getSimpleName()}-%d")
                .build()

        // A fixed-sized pool which allows queuing the same number of tasks as the pool size and drops additional tasks.
        ThreadPoolExecutor executor = new ThreadPoolExecutor(NUM_WORKERS, NUM_WORKERS, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingDeque<>(NUM_WORKERS), threadFactory, new ThreadPoolExecutor.DiscardPolicy())
        return MoreExecutors.getExitingExecutorService(executor, 5, TimeUnit.SECONDS)
    }

    static HttpClient buildHttpClient() {
        PoolingClientConnectionManager cm = new PoolingClientConnectionManager()
        int poolSize = Math.min(NUM_WORKERS, MAX_CONNECTION_POOL_SIZE)
        cm.setMaxTotal(poolSize)
        cm.setDefaultMaxPerRoute(poolSize)

        HttpClient httpClient = new DefaultHttpClient(cm)
        HttpParams httpParams = httpClient.getParams()

        HttpConnectionParams.setConnectionTimeout(httpParams, CONNECT_TIMEOUT_MS)
        HttpConnectionParams.setSoTimeout(httpParams, READ_TIMEOUT_MS)

        return httpClient
    }
}
