package whelk.component

import com.google.common.base.Preconditions
import groovy.util.logging.Log4j2 as Log
import org.apache.hc.core5.http.ClassicHttpResponse
import org.apache.hc.client5.http.config.RequestConfig
import org.apache.hc.client5.http.classic.methods.HttpDelete
import org.apache.hc.client5.http.classic.methods.HttpPut
import org.apache.hc.core5.http.ClassicHttpRequest
import org.apache.hc.client5.http.io.HttpClientConnectionManager
import org.apache.hc.core5.http.io.entity.StringEntity
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient
import org.apache.hc.client5.http.impl.classic.HttpClients
import org.apache.hc.core5.http.io.entity.EntityUtils
import org.apache.hc.core5.util.Timeout
import whelk.Document
import whelk.converter.JsonLdToTrigSerializer
import whelk.exception.UnexpectedHttpStatusException
import whelk.util.Metrics

import java.nio.charset.StandardCharsets

import static whelk.component.Virtuoso.Method.DELETE
import static whelk.component.Virtuoso.Method.PUT

@Log
class Virtuoso {
    enum Method { PUT, DELETE }
    
    // HTTP timeout parameters
    private static final int CONNECT_TIMEOUT_MS = 5 * 1000
    private static final int READ_TIMEOUT_MS = 5 * 1000
    
    private final String sparqlCrudEndpoint
    private final String sparqlUser
    private final String sparqlPass

    private final CloseableHttpClient httpClient
    private final Map ctx

    Virtuoso(Map jsonldContext, HttpClientConnectionManager cm, String endpoint, String user, String pass) {
        Preconditions.checkNotNull(jsonldContext)
        this.ctx = jsonldContext
        this.sparqlCrudEndpoint = Preconditions.checkNotNull(endpoint)
        Preconditions.checkNotNull(cm)
        Preconditions.checkNotNull(user, "user was null")
        Preconditions.checkNotNull(pass, "password was null")
        this.httpClient = buildHttpClient(cm)
        this.sparqlUser = user
        this.sparqlPass = pass
    }

    void deleteNamedGraph(Document doc) {
        updateNamedGraph(DELETE, doc)
    }

    void insertNamedGraph(Document doc) {
        updateNamedGraph(PUT, doc)
    }

    private void updateNamedGraph(Method method, Document doc) {
        ClassicHttpRequest request = buildRequest(method, doc)
        try {
            Metrics.clientTimer.labels(Virtuoso.class.getSimpleName(), method.toString()).time {
                String credentials = "${this.sparqlUser}:${this.sparqlPass}".bytes.encodeBase64().toString()
                request.setHeader("Authorization", "Basic " + credentials)
                ClassicHttpResponse response = httpClient.execute(request)
                try {
                    handleResponse(response, method, doc)
                }
                finally {
                    response.close()
                }
            }
        }
        catch(Exception e) {
            if (!(e instanceof UnexpectedHttpStatusException)) {
                Metrics.clientCounter.labels(Virtuoso.class.getSimpleName(), method.toString(), "${e.getMessage()}").inc()
            }
            throw e
        }
    }

    private String createGraphCrudURI(String docURI) {
        return sparqlCrudEndpoint + "?graph=" + docURI
    }

    private ClassicHttpRequest buildRequest(Method method, Document doc) {
        String graphCrudURI = createGraphCrudURI(doc.getCompleteId())

        if (method == DELETE) {
            return new HttpDelete(graphCrudURI)
        } else if (method == PUT) {
            HttpPut request = new HttpPut(graphCrudURI)
            request.addHeader("Content-Type", "text/turtle")
            String turtleDoc = convertToTurtle(doc)
            request.setEntity(new StringEntity(turtleDoc, StandardCharsets.UTF_8))
            return request
        } else {
            throw new IllegalArgumentException("Bad request method:" + method)
        }
    }

    private void handleResponse(ClassicHttpResponse response, Method method, Document doc) {
        int statusCode = response.getCode()
        String reasonPhrase = response.getReasonPhrase()
        Metrics.clientCounter.labels(Virtuoso.class.getSimpleName(), method.toString(), "$statusCode").inc()

        if ((statusCode >= 200 && statusCode < 300) || (method == DELETE && statusCode == 404)) {
            if (log.isDebugEnabled()) {
                log.debug("Succeeded to $method ${doc.getCompleteId()}, got: $statusCode $reasonPhrase")
            }
            EntityUtils.consume(response.getEntity())
        }
        else {
            String body = EntityUtils.toString(response.getEntity())
            String ttl = convertToTurtle(doc)
            String msg = "Failed to $method ${doc.getCompleteId()}, got: $statusCode $reasonPhrase\n$body\nsent:\n$ttl"

            // 401 should be retried (can be fixed by correcting credentials in configuration)
            // From experiments:
            // - Virtuoso responds with 500 for broken documents
            // - PUT fails sporadically with 404 NOT FOUND and 501 NOT IMPLEMENTED
            // - DELETE/PUT fails sporadically with 503 Service Unavailable
            if (statusCode == 401 || statusCode == 404 || statusCode == 501 || statusCode == 503) {
                throw new UnexpectedHttpStatusException(msg, statusCode)
            }
            else {
                // Cannot recover from these
                log.info("$msg")
            }
        }
    }

    private String convertToTurtle(Document doc) {
        def bytes = JsonLdToTrigSerializer.toTurtle(ctx, doc.data).toByteArray()
        return new String(bytes, StandardCharsets.UTF_8)
    }
    
    private static CloseableHttpClient buildHttpClient(HttpClientConnectionManager cm) {
        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(Timeout.ofMilliseconds(CONNECT_TIMEOUT_MS))
                .setConnectionRequestTimeout(Timeout.ofMilliseconds(CONNECT_TIMEOUT_MS))
                .setResponseTimeout(Timeout.ofMilliseconds(READ_TIMEOUT_MS))
                .build()

        CloseableHttpClient httpClient = HttpClients.custom()
                .setConnectionManager(cm)
                .setDefaultRequestConfig(requestConfig)
                .build()

        return httpClient
    }
}
