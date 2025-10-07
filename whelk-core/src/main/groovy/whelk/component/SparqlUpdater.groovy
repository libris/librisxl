package whelk.component

import com.google.common.util.concurrent.MoreExecutors
import com.google.common.util.concurrent.ThreadFactoryBuilder
import groovy.transform.CompileStatic
import groovy.util.logging.Log4j2 as Log
import org.apache.hc.client5.http.config.ConnectionConfig
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder
import org.apache.hc.client5.http.io.HttpClientConnectionManager
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager
import org.apache.hc.core5.util.Timeout
import org.apache.hc.core5.util.TimeValue
import whelk.Document
import whelk.exception.UnexpectedHttpStatusException

import javax.sql.DataSource
import java.util.concurrent.ExecutorService
import java.util.concurrent.LinkedBlockingDeque
import java.util.concurrent.ThreadFactory
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

import static whelk.component.PostgreSQLComponent.QueueHandler.Result.FAIL_REQUEUE
import static whelk.component.PostgreSQLComponent.QueueHandler.Result.FAIL_RETRY
import static whelk.component.PostgreSQLComponent.QueueHandler.Result.HANDLED

@Log
@CompileStatic
class SparqlUpdater {
    private static final long PERIODIC_CHECK_MS = 30 * 1000
    private static final int DEFAULT_NUM_WORKERS = 4

    // Number of items to take from queue each time
    private static final int QUEUE_TAKE_NUM = 1

    private final ExecutorService executorService
    private final Runnable task
    private final Timer timer = new Timer("${SparqlUpdater.class.getName()}-timer", true)

    private final int numWorkers

    static SparqlUpdater build(PostgreSQLComponent storage, Map jsonLdContext, Properties configuration) {
        if (!(configuration.getProperty("sparqlEnabled")?.toLowerCase() == 'false')) {
            storage.sparqlQueueEnabled = true
        }

        String sparqlCrudUrl = configuration.getProperty("sparqlCrudUrl")
        if (sparqlCrudUrl) {
            int numWorkers = configuration.getProperty("sparqlNumWorkers")
                    ? Integer.parseInt(configuration.getProperty("sparqlNumWorkers"))
                    : DEFAULT_NUM_WORKERS

            Virtuoso virtuoso = new Virtuoso(
                    jsonLdContext,
                    buildHttpClientConnectionManager(numWorkers),
                    sparqlCrudUrl,
                    configuration.getProperty("sparqlUser"),
                    configuration.getProperty("sparqlPass"))

            return new SparqlUpdater(storage, virtuoso, numWorkers)
        }
        else {
            return new SparqlUpdater()
        }
    }

    private SparqlUpdater() {
        executorService = null
        task = null
        numWorkers = 0
    }

    private SparqlUpdater(PostgreSQLComponent storage, Virtuoso sparql, int numWorkers) {
        this.numWorkers = numWorkers
        this.executorService = buildExecutorService(numWorkers)

        PostgreSQLComponent.QueueHandler handler = { Document doc ->
            try {
                if (doc.deleted) {
                    sparql.deleteNamedGraph(doc)
                }
                else {
                    sparql.insertNamedGraph(doc)
                }
                return HANDLED
            }
            catch (UnexpectedHttpStatusException e) {
                log.info("Failed, will requeue: $e")
                return FAIL_REQUEUE
            }
            catch (Exception e) {
                log.info("Failed, will retry: $e")
                return FAIL_RETRY
            }
        }

        DataSource connectionPool = storage.createAdditionalConnectionPool(this.getClass().getSimpleName(), numWorkers)
        this.task = {
            try {
                // Run as long as there still might be docs in the queue and we haven't failed
                if (storage.sparqlQueueTake(handler, QUEUE_TAKE_NUM, connectionPool)) {
                    pollNow()
                }
            }
            catch (Exception e) {
                log.warn("Error executing task: $e", e)
            }
        }

        timer.scheduleAtFixedRate({ pollNow() }, PERIODIC_CHECK_MS, PERIODIC_CHECK_MS)
    }

    /**
     * Force polling SPARQL update queue now
     */
    void pollNow() {
        if (executorService) {
            try {
                executorService.submit(task)
            }
            catch (Exception e) {
                // this should never happen...
                log.warn("Error calling executorService.submit: $e", e)
            }
        }
    }

    private static HttpClientConnectionManager buildHttpClientConnectionManager(final int poolSize) {
        PoolingHttpClientConnectionManager cm = PoolingHttpClientConnectionManagerBuilder.create()
                .setMaxConnTotal(poolSize)
                .setMaxConnPerRoute(poolSize)
                .setDefaultConnectionConfig(
                        ConnectionConfig.custom()
                                .setConnectTimeout(Timeout.ofMilliseconds(5000)) // 5 seconds
                                .setSocketTimeout(Timeout.ofMilliseconds(5000))   // 5 seconds
                                .setTimeToLive(TimeValue.ofMinutes(10))
                                .build()
                )
                .build()
        return cm
    }

    private static ExecutorService buildExecutorService(final int poolSize) {
        ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("${SparqlUpdater.class.getSimpleName()}-%d")
                .build()

        // A fixed-sized pool which allows queuing the same number of tasks as the pool size and drops additional tasks.
        ThreadPoolExecutor executor = new ThreadPoolExecutor(poolSize, poolSize, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingDeque<>(poolSize), threadFactory, new ThreadPoolExecutor.DiscardPolicy())
        return MoreExecutors.getExitingExecutorService(executor, 5, TimeUnit.SECONDS)
    }
}