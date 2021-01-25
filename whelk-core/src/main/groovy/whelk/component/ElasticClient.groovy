package whelk.component

import com.google.common.collect.Iterators
import groovy.util.logging.Log4j2 as Log
import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.github.resilience4j.prometheus.collectors.CircuitBreakerMetricsCollector
import io.github.resilience4j.prometheus.collectors.RetryMetricsCollector
import io.github.resilience4j.retry.Retry
import io.github.resilience4j.retry.RetryConfig
import io.github.resilience4j.retry.RetryRegistry
import io.prometheus.client.CollectorRegistry
import org.apache.http.HttpEntity
import org.apache.http.HttpResponse
import org.apache.http.client.HttpClient
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.methods.HttpPost
import org.apache.http.client.methods.HttpPut
import org.apache.http.client.methods.HttpRequestBase
import org.apache.http.conn.HttpClientConnectionManager
import org.apache.http.entity.ContentType
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.impl.client.HttpClients
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager
import org.apache.http.util.EntityUtils
import whelk.exception.ElasticIOException
import whelk.exception.UnexpectedHttpStatusException

import java.time.Duration
import java.util.function.Function

@Log
class ElasticClient {
    static final int MAX_CONNECTIONS_PER_HOST = 12
    static final int CONNECTION_POOL_SIZE = 30

    static final int CONNECT_TIMEOUT_MS = 5 * 1000
    static final int READ_TIMEOUT_MS = 60 * 1000
    static final int MAX_BACKOFF_S = 1024

    static final CircuitBreakerConfig CB_CONFIG = CircuitBreakerConfig.custom()
            .minimumNumberOfCalls(10)
            .slidingWindowSize(10)
            .failureRateThreshold(50)
            .waitDurationInOpenState(Duration.ofSeconds(10))
            .slowCallRateThreshold(50)
            .slowCallDurationThreshold(Duration.ofSeconds(30))
            .permittedNumberOfCallsInHalfOpenState(1)
            .build()

    List<ElasticNode> elasticNodes
    HttpClient httpClient
    Random random = new Random()
    boolean useCircuitBreaker

    CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.ofDefaults()
    RetryRegistry retryRegistry = RetryRegistry.ofDefaults()
    Retry globalRetry

    static ElasticClient withDefaultHttpClient(List<String> elasticHosts) {
        HttpClientConnectionManager cm = new PoolingHttpClientConnectionManager()
        cm.setMaxTotal(CONNECTION_POOL_SIZE)
        cm.setDefaultMaxPerRoute(MAX_CONNECTIONS_PER_HOST)

        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(CONNECT_TIMEOUT_MS)
                .setConnectionRequestTimeout(CONNECT_TIMEOUT_MS)
                .setSocketTimeout(READ_TIMEOUT_MS)
                .build()

        CloseableHttpClient httpClient = HttpClients.custom()
                .setConnectionManager(cm)
                .setDefaultRequestConfig(requestConfig)
                .build()
        
        return new ElasticClient(httpClient, elasticHosts, true)
    }

    static ElasticClient withBulkHttpClient(List<String> elasticHosts) {
        HttpClientConnectionManager cm = new PoolingHttpClientConnectionManager()
        cm.setMaxTotal(CONNECTION_POOL_SIZE)
        cm.setDefaultMaxPerRoute(MAX_CONNECTIONS_PER_HOST)

        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(0)
                .setSocketTimeout(READ_TIMEOUT_MS * 20)
                .build()

        CloseableHttpClient httpClient = HttpClients.custom()
                .setConnectionManager(cm)
                .setDefaultRequestConfig(requestConfig)
                .build()
        
        return new ElasticClient(httpClient, elasticHosts, false)
    }

    private ElasticClient(HttpClient httpClient, List<String> elasticHosts, boolean useCircuitBreaker) {
        this.httpClient = httpClient
        this.elasticNodes = elasticHosts.collect { new ElasticNode(it) }

        if (useCircuitBreaker) {
            globalRetry = retryRegistry.retry(ElasticClient.class.getSimpleName(), RetryConfig.custom()
                    .waitDuration(Duration.ofMillis(10))
                    .maxAttempts(elasticNodes.size() * 2)
                    .build())

            CollectorRegistry collectorRegistry = CollectorRegistry.defaultRegistry
            collectorRegistry.register(RetryMetricsCollector.ofRetryRegistry(retryRegistry))
            collectorRegistry.register(CircuitBreakerMetricsCollector.ofCircuitBreakerRegistry(circuitBreakerRegistry))
        }

        this.useCircuitBreaker = useCircuitBreaker

        log.info "ElasticSearch component initialized with ${elasticHosts.size()} nodes."
    }

    String performRequest(String method, String path, String body, String contentType0 = null)
        throws ElasticIOException, UnexpectedHttpStatusException {
        try {
            def nodes = cycleNodes()
            if (useCircuitBreaker) {
                globalRetry.executeSupplier({ -> nodes.next().performRequest(method, path, body, contentType0) })
            }
            else {
                nodes.next().performRequest(method, path, body, contentType0)
            }
        }
        catch (UnexpectedHttpStatusException e) {
            throw e
        }
        catch (Exception e) {
            log.warn("Request to ElasticSearch failed: ${e}", e)
            throw new ElasticIOException(e.getMessage(), e)
        }
    }

    private Iterator<ElasticNode> cycleNodes() {
        def cycle = Iterators.cycle(elasticNodes)
        Iterators.advance(cycle, random.nextInt(elasticNodes.size()))
        return cycle
    }

    class ElasticNode {
        String host
        Function<HttpRequestBase, Tuple2<Integer, String>> send

        ElasticNode(String host) {
            this.host = host

            if (useCircuitBreaker) {
                CircuitBreaker cb = circuitBreakerRegistry.circuitBreaker(host, CB_CONFIG)

                Retry tryTwice = retryRegistry.retry(host, RetryConfig.custom()
                        .maxAttempts(2)
                        .retryOnException({ !(it instanceof RetriesExceededException) }).build())

                this.send = CircuitBreaker.decorateFunction(cb, Retry.decorateFunction(tryTwice, this.&sendRequest))
            }
            else {
                this.send = this.&sendRequest
            }
        }

        String performRequest(String method, String path, String body, String contentType0 = null) {
            def (int statusCode, String resultBody) = send.apply(buildRequest(method, path, body, contentType0))
            if (statusCode >= 200 && statusCode < 300) {
                return resultBody
            }
            else {
                throw new UnexpectedHttpStatusException(resultBody, statusCode)
            }
        }

        private Tuple2<Integer, String> sendRequest(HttpRequestBase request) {
            try {
                return sendRequestRetry4XX(request)
            }
            catch (Exception e) {
                throw new RuntimeException(e.getMessage(), e)
            }
            finally {
                request.reset()
                request.releaseConnection()
            }
        }

        private Tuple2<Integer, String> sendRequestRetry4XX(HttpRequestBase request) {
            int backOffSeconds = 1
            while (true) {
                HttpResponse response = httpClient.execute(request)
                int statusCode = response.getStatusLine().getStatusCode()

                if (statusCode != 429 && statusCode != 409) {
                    def result = new Tuple2(statusCode, EntityUtils.toString(response.getEntity()))

                    if (log.isDebugEnabled()) {
                        String r = result.getSecond()
                        if (r.size() < 50_000) {
                            log.debug("Elastic response: $r")
                        }
                    }
                    return result
                } else {
                    if (backOffSeconds > MAX_BACKOFF_S) {
                        throw new RetriesExceededException("Max retries exceeded: HTTP 4XX from ElasticSearch")
                    }

                    request.reset()

                    log.info("Bulk indexing request to ElasticSearch was throttled (HTTP 429) waiting $backOffSeconds seconds before retry.")
                    Thread.sleep(backOffSeconds * 1000)

                    backOffSeconds *= 2
                }
            }
        }

        private HttpRequestBase buildRequest(String method, String path, String body, String contentType0 = null) {
            switch (method) {
                case 'GET':
                    return new HttpGet(host + path)
                case 'PUT':
                    HttpPut request = new HttpPut(host + path)
                    request.setEntity(httpEntity(body, contentType0))
                    return request
                case 'POST':
                    HttpPost request = new HttpPost(host + path)
                    request.setEntity(httpEntity(body, contentType0))
                    return request
                default:
                    throw new IllegalArgumentException("Bad request method:" + method)
            }
        }

        private static HttpEntity httpEntity(String body, String contentType) {
            return new StringEntity(body,
                    contentType ? ContentType.create(contentType) : ContentType.APPLICATION_JSON)
        }
    }

    class RetriesExceededException extends RuntimeException {
        RetriesExceededException(String msg) {
            super(msg)
        }
    }
}
