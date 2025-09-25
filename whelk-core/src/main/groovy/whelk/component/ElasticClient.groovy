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
import org.apache.hc.client5.http.ssl.DefaultClientTlsStrategy
import org.apache.hc.client5.http.ssl.HostnameVerificationPolicy
import org.apache.hc.core5.http.Header
import org.apache.hc.core5.http.HttpEntity
import org.apache.hc.core5.http.HttpHeaders
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient
import org.apache.hc.client5.http.config.RequestConfig
import org.apache.hc.client5.http.classic.methods.HttpGet
import org.apache.hc.client5.http.classic.methods.HttpPost
import org.apache.hc.client5.http.classic.methods.HttpPut
import org.apache.hc.core5.http.ClassicHttpRequest
import org.apache.hc.client5.http.ssl.NoopHostnameVerifier
import org.apache.hc.core5.ssl.TrustStrategy
import org.apache.hc.core5.http.ContentType
import org.apache.hc.core5.http.io.entity.StringEntity
import org.apache.hc.client5.http.impl.classic.HttpClients
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder
import org.apache.hc.core5.http.message.BasicHeader
import org.apache.hc.core5.ssl.SSLContexts
import org.apache.hc.core5.http.io.entity.EntityUtils
import org.apache.hc.core5.util.Timeout
import org.apache.hc.core5.pool.PoolConcurrencyPolicy
import org.apache.hc.core5.pool.PoolReusePolicy
import org.apache.hc.client5.http.config.ConnectionConfig
import org.apache.hc.core5.http.ssl.TLS
import org.apache.hc.core5.util.TimeValue
import whelk.exception.ElasticIOException
import whelk.exception.UnexpectedHttpStatusException

import javax.net.ssl.SSLContext
import java.time.Duration
import java.util.function.Function

@Log
class ElasticClient {
    static final int MAX_CONNECTIONS_PER_HOST = 12
    static final int CONNECTION_POOL_SIZE = 30

    static final int CONNECT_TIMEOUT_MS = 5 * 1000
    static final int READ_TIMEOUT_MS = 15 * 1000
    static final int BATCH_CONNECT_TIMEOUT_MS = 0 // infinite
    static final int BATCH_READ_TIMEOUT_MS = READ_TIMEOUT_MS * 80
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
    CloseableHttpClient httpClient
    Random random = new Random()
    boolean useCircuitBreaker

    CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.ofDefaults()
    RetryRegistry retryRegistry = RetryRegistry.ofDefaults()
    Retry globalRetry

    static ElasticClient withDefaultHttpClient(List<String> elasticHosts, String elasticUser, String elasticPassword) {
        TrustStrategy acceptingTrustStrategy = (cert, authType) -> true
        SSLContext sslContext = SSLContexts.custom().loadTrustMaterial(null, acceptingTrustStrategy).build()

        PoolingHttpClientConnectionManager cm = PoolingHttpClientConnectionManagerBuilder.create()
                .setTlsSocketStrategy(
                        new DefaultClientTlsStrategy(
                                sslContext,
                                HostnameVerificationPolicy.CLIENT,
                                NoopHostnameVerifier.INSTANCE
                        )
                )
                .setMaxConnTotal(CONNECTION_POOL_SIZE)
                .setMaxConnPerRoute(MAX_CONNECTIONS_PER_HOST)
                .setPoolConcurrencyPolicy(PoolConcurrencyPolicy.STRICT)
                .setConnPoolPolicy(PoolReusePolicy.LIFO)
                .setDefaultConnectionConfig(
                        ConnectionConfig.custom()
                                .setConnectTimeout(Timeout.ofMilliseconds(CONNECT_TIMEOUT_MS))
                                .setSocketTimeout(Timeout.ofMilliseconds(READ_TIMEOUT_MS))
                                .setTimeToLive(TimeValue.ofMinutes(10))
                                .build()
                )
                .build()

        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectionRequestTimeout(Timeout.ofMilliseconds(CONNECT_TIMEOUT_MS))
                .setResponseTimeout(Timeout.ofMilliseconds(READ_TIMEOUT_MS))
                .build()

        String auth = elasticUser + ":" + elasticPassword
        Header authHeader = new BasicHeader(HttpHeaders.AUTHORIZATION, "Basic " + auth.bytes.encodeBase64().toString())
        List<Header> headers = List.of(authHeader)

        CloseableHttpClient httpClient = HttpClients.custom()
                .setConnectionManager(cm)
                .setDefaultRequestConfig(requestConfig)
                .setDefaultHeaders(headers)
                .build()

        return new ElasticClient(httpClient, elasticHosts, true)
    }

    static ElasticClient withBulkHttpClient(List<String> elasticHosts, String elasticUser, String elasticPassword) {
        TrustStrategy acceptingTrustStrategy = (cert, authType) -> true
        SSLContext sslContext = SSLContexts.custom().loadTrustMaterial(null, acceptingTrustStrategy).build()

        PoolingHttpClientConnectionManager cm = PoolingHttpClientConnectionManagerBuilder.create()
                .setTlsSocketStrategy(
                        new DefaultClientTlsStrategy(
                                sslContext,
                                HostnameVerificationPolicy.CLIENT,
                                NoopHostnameVerifier.INSTANCE
                        )
                )
                .setMaxConnTotal(CONNECTION_POOL_SIZE)
                .setMaxConnPerRoute(MAX_CONNECTIONS_PER_HOST)
                .setPoolConcurrencyPolicy(PoolConcurrencyPolicy.STRICT)
                .setConnPoolPolicy(PoolReusePolicy.LIFO)
                .setDefaultConnectionConfig(
                        ConnectionConfig.custom()
                                .setConnectTimeout(BATCH_CONNECT_TIMEOUT_MS == 0 ? Timeout.DISABLED : Timeout.ofMilliseconds(BATCH_CONNECT_TIMEOUT_MS))
                                .setSocketTimeout(Timeout.ofMilliseconds(BATCH_READ_TIMEOUT_MS))
                                .setTimeToLive(TimeValue.ofMinutes(10))
                                .build()
                )
                .build()

        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectionRequestTimeout(Timeout.ofMilliseconds(CONNECT_TIMEOUT_MS))
                .setResponseTimeout(Timeout.ofMilliseconds(BATCH_READ_TIMEOUT_MS))
                .build()

        String auth = elasticUser + ":" + elasticPassword
        Header authHeader = new BasicHeader(HttpHeaders.AUTHORIZATION, "Basic " + auth.bytes.encodeBase64().toString())
        List<Header> headers = List.of(authHeader)

        CloseableHttpClient httpClient = HttpClients.custom()
                .setConnectionManager(cm)
                .setDefaultRequestConfig(requestConfig)
                .setDefaultHeaders(headers)
                .build()

        return new ElasticClient(httpClient, elasticHosts, false)
    }

    private ElasticClient(CloseableHttpClient httpClient, List<String> elasticHosts, boolean useCircuitBreaker) {
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
        Function<ClassicHttpRequest, Tuple2<Integer, String>> send

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

        private Tuple2<Integer, String> sendRequest(ClassicHttpRequest request) {
            try {
                return sendRequestRetry4XX(request)
            }
            catch (Exception e) {
                throw new RuntimeException(e.getMessage(), e)
            }
        }

        private Tuple2<Integer, String> sendRequestRetry4XX(ClassicHttpRequest request) {
            int backOffSeconds = 1
            while (true) {
                def result = httpClient.execute(request) { response ->
                    int statusCode = response.getCode()
                    String responseBody = EntityUtils.toString(response.getEntity())
                    return new Tuple2(statusCode, responseBody)
                }

                int statusCode = result.v1 as int

                if (statusCode != 429 && statusCode != 409) {
                    if (log.isDebugEnabled()) {
                        String r = result.v2
                        if (r.size() < 50_000) {
                            log.debug("Elastic response: $r")
                        }
                    }
                    return result
                } else {
                    if (backOffSeconds > MAX_BACKOFF_S) {
                        throw new RetriesExceededException("Max retries exceeded: HTTP 4XX from ElasticSearch")
                    }

                    log.info("Bulk indexing request to ElasticSearch was throttled (HTTP 429) waiting $backOffSeconds seconds before retry.")
                    Thread.sleep(backOffSeconds * 1000)

                    backOffSeconds *= 2
                }
            }
        }

        private ClassicHttpRequest buildRequest(String method, String path, String body, String contentType0 = null) {
            switch (method) {
                case 'GET':
                    return new HttpGet(host + path)
                case 'PUT':
                    HttpPut request = new HttpPut(host + path)
                    if (body)
                        request.setEntity(httpEntity(body, contentType0))
                    return request
                case 'POST':
                    HttpPost request = new HttpPost(host + path)
                    if (body)
                        request.setEntity(httpEntity(body, contentType0))
                    return request
                case 'DELETE':
                    HttpDeleteWithBody request = new HttpDeleteWithBody(host + path)
                    if (body)
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
    
    class HttpDeleteWithBody extends HttpPost { // LOL
        HttpDeleteWithBody(String uri) {
            super(uri)
        }

        @Override
        String getMethod() {
            return 'DELETE'
        }
    }

    class RetriesExceededException extends RuntimeException {
        RetriesExceededException(String msg) {
            super(msg)
        }
    }
}
