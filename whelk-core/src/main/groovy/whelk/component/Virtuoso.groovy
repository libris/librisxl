package whelk.component

import com.google.common.base.Preconditions
import groovy.util.logging.Log4j2 as Log
import org.apache.http.HttpResponse
import org.apache.http.StatusLine
import org.apache.http.auth.AuthScope
import org.apache.http.auth.UsernamePasswordCredentials
import org.apache.http.client.CredentialsProvider
import org.apache.http.client.HttpClient
import org.apache.http.client.methods.HttpDelete
import org.apache.http.client.methods.HttpPut
import org.apache.http.client.methods.HttpRequestBase
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.util.EntityUtils
import whelk.Document
import whelk.converter.JsonLdToTurtle
import whelk.exception.UnexpectedHttpStatusException
import whelk.util.Metrics

import java.nio.charset.StandardCharsets

import static whelk.component.Virtuoso.Method.DELETE
import static whelk.component.Virtuoso.Method.PUT

@Log
class Virtuoso {
    enum Method { PUT, DELETE }

    private final String sparqlCrudEndpoint

    private final HttpClient httpClient
    private final Map ctx

    Virtuoso(Map jsonldContext, HttpClient httpClient, String endpoint, String user, String pass) {
        Preconditions.checkNotNull(jsonldContext)
        this.ctx = JsonLdToTurtle.parseContext(['@context': jsonldContext])
        this.sparqlCrudEndpoint = Preconditions.checkNotNull(endpoint)
        Preconditions.checkNotNull(user, "user was null")
        Preconditions.checkNotNull(pass, "password was null")

        this.httpClient = Preconditions.checkNotNull(httpClient)
        setCredentials(httpClient, user, pass)
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
                handleResponse(httpClient.execute(request), method, doc)
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
        }
        else {
            String body = EntityUtils.toString(response.getEntity())
            String ttl = convertToTurtle(doc)
            String msg = "Failed to $method ${doc.getCompleteId()}, got: $statusLine\n$body\nsent:\n$ttl"

            // 401 should be retried (can be fixed by correcting credentials in configuration)
            // From experiments:
            // - Virtuoso responds with 500 for broken documents
            // - PUT fails sporadically with 404
            if (statusCode == 401 || statusCode == 404) {
                throw new UnexpectedHttpStatusException(msg, statusCode)
            }
            else {
                // Cannot recover from these
                log.warn("$msg")
            }
        }
    }

    private String convertToTurtle(Document doc) {
        final Map opts = [markEmptyBnode: true]
        ByteArrayOutputStream out = new ByteArrayOutputStream()
        JsonLdToTurtle serializer = new JsonLdToTurtle(ctx, out, opts)

        serializer.toTurtle(doc.data)

        return new String(out.toByteArray(), StandardCharsets.UTF_8)
    }

    private static void setCredentials(HttpClient client, String sparqlUser, String sparqlPassword) {
        CredentialsProvider provider = new BasicCredentialsProvider()
        provider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(sparqlUser, sparqlPassword))
        client.setCredentialsProvider(provider)
    }
}
