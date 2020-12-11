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
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.util.EntityUtils
import whelk.Document
import whelk.converter.JsonLdToTurtle
import whelk.exception.UnexpectedHttpStatusException
import whelk.util.Metrics

import java.nio.charset.StandardCharsets

@Log
class Virtuoso {
    private String sparqlCrudEndpoint

    private HttpClient httpClient
    private Map jsonldContext
    private String user
    private String pass

    Virtuoso(Map jsonldContext, String endpoint, String user, String pass) {
        this.jsonldContext = Preconditions.checkNotNull(jsonldContext)
        this.sparqlCrudEndpoint = Preconditions.checkNotNull(endpoint)
        this.user = Preconditions.checkNotNull(user)
        this.pass = Preconditions.checkNotNull(pass)

        setHttpClient(new DefaultHttpClient())
    }

    void setHttpClient(HttpClient httpClient) {
        this.httpClient = httpClient
        setCredentials(httpClient, user, pass)
    }

    void deleteNamedGraph(Document doc) {
        updateNamedGraph('DELETE', doc)
    }

    void insertNamedGraph(Document doc) {
        updateNamedGraph('PUT', doc)
    }

    private void updateNamedGraph(String method, Document doc) {
        HttpRequestBase request = buildRequest(method, doc)
        try {
            Metrics.clientTimer.labels(Virtuoso.class.getSimpleName(), method).time {
                handleResponse(httpClient.execute(request), method, doc.getCompleteId())
            }
        }
        catch(Exception e) {
            if (!(e instanceof UnexpectedHttpStatusException)) {
                Metrics.clientCounter.labels(Virtuoso.class.getSimpleName(), method, e.getMessage()).inc()
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

    private HttpRequestBase buildRequest(String method, Document doc) {
        String graphCrudURI = createGraphCrudURI(doc.getCompleteId())

        if (method == 'DELETE') {
            return new HttpDelete(graphCrudURI)
        } else if (method == 'PUT'){
            HttpPut request = new HttpPut(graphCrudURI)
            String turtleDoc = convertToTurtle(doc, jsonldContext)
            request.setEntity(new StringEntity(turtleDoc, StandardCharsets.UTF_8))
            return request
        } else {
            throw new IllegalArgumentException("Bad request method:" + method)
        }
    }

    private static void handleResponse(HttpResponse response, String method, String docURI) {
        StatusLine statusLine = response.getStatusLine()
        int statusCode = statusLine.getStatusCode()
        Metrics.clientCounter.labels(Virtuoso.class.getSimpleName(), method, "$statusCode").inc()

        if ((statusCode >= 200 && statusCode < 300) || (method == 'DELETE' && statusCode == 404)) {
            if (log.isDebugEnabled()) {
                log.debug("Succeeded to $method $docURI, got: $statusLine")
            }
        }
        else {
            String body = EntityUtils.toString(response.getEntity())
            String msg = "Failed to $method $docURI, got: $statusLine\n$body"

            if (statusCode >= 500 || statusCode == 429) {
                throw new UnexpectedHttpStatusException(msg, statusCode)
            }
            else {
                // Cannot recover from these
                log.warn(msg)
            }
        }
    }

    private static String convertToTurtle(Document doc, Map context) {
        Map ctx = JsonLdToTurtle.parseContext(['@context': context])
        Map opts = [markEmptyBnode: true]
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
