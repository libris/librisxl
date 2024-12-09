package whelk.component

import com.google.common.base.Preconditions
import groovy.util.logging.Log4j2 as Log
import org.apache.http.HttpResponse
import org.apache.http.StatusLine
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.CloseableHttpResponse
import org.apache.http.client.methods.HttpDelete
import org.apache.http.client.methods.HttpPut
import org.apache.http.client.methods.HttpRequestBase
import org.apache.http.conn.HttpClientConnectionManager
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
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
        HttpRequestBase request = buildRequest(method, doc)
        try {
            Metrics.clientTimer.labels(Virtuoso.class.getSimpleName(), method.toString()).time {
                String credentials = "${this.sparqlUser}:${this.sparqlPass}".bytes.encodeBase64().toString()
                request.setHeader("Authorization", "Basic " + credentials)
                CloseableHttpResponse response = httpClient.execute(request)
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
        finally {
            request.releaseConnection()
        }
    }

    private String createGraphCrudURI(String docURI) {
        return sparqlCrudEndpoint + "?graph=" + docURI
    }

    private HttpRequestBase buildRequest(Method method, Document doc) {
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

    private void handleResponse(HttpResponse response, Method method, Document doc) {
        StatusLine statusLine = response.getStatusLine()
        int statusCode = statusLine.getStatusCode()
        Metrics.clientCounter.labels(Virtuoso.class.getSimpleName(), method.toString(), "$statusCode").inc()

        if ((statusCode >= 200 && statusCode < 300) || (method == DELETE && statusCode == 404)) {
            if (log.isDebugEnabled()) {
                log.debug("Succeeded to $method ${doc.getCompleteId()}, got: $statusLine")
            }
            EntityUtils.consume(response.getEntity())
        }
        else {
            String body = EntityUtils.toString(response.getEntity())
            String ttl = convertToTurtle(doc)
            String msg = "Failed to $method ${doc.getCompleteId()}, got: $statusLine\n$body\nsent:\n$ttl"

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
                .setConnectTimeout(CONNECT_TIMEOUT_MS)
                .setConnectionRequestTimeout(CONNECT_TIMEOUT_MS)
                .setSocketTimeout(READ_TIMEOUT_MS)
                .build()

        CloseableHttpClient httpClient = HttpClients.custom()
                .setConnectionManager(cm)
                .setDefaultRequestConfig(requestConfig)
                .build()

        return httpClient
    }
}
