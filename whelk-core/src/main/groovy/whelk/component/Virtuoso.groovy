package whelk.component

import groovy.util.logging.Log4j2 as Log
import org.apache.http.HttpResponse
import org.apache.http.StatusLine
import org.apache.http.auth.AuthScope
import org.apache.http.auth.UsernamePasswordCredentials
import org.apache.http.client.CredentialsProvider
import org.apache.http.client.methods.HttpDelete
import org.apache.http.client.methods.HttpPut
import org.apache.http.client.methods.HttpRequestBase
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.client.HttpClient
import org.apache.http.util.EntityUtils
import whelk.Document
import whelk.converter.JsonLdToTurtle

import java.nio.charset.StandardCharsets

@Log
class Virtuoso {

    private String sparqlCrudEndpoint

    private HttpClient httpClient

    Map jsonldContext

    Virtuoso(String endpoint, String user, String pass) {
        this.sparqlCrudEndpoint = endpoint

        this.httpClient = new DefaultHttpClient()

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
        HttpResponse response = performRequest(request)
        handleResponse(response, method, doc.completeId())
    }

    private String createGraphCrudURI(String docURI) {
        return sparqlCrudEndpoint + "?graph=" + docURI
    }

    private HttpRequestBase buildRequest(String method, Document doc) {
        String graphCrudURI = createGraphCrudURI(doc.completeId())

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

    private HttpResponse performRequest(HttpRequestBase request) {
        HttpResponse response = httpClient.execute(request)
        request.releaseConnection()
        return response
    }

    private static void handleResponse(HttpResponse response, String method, String docURI) {
        StatusLine statusLine = response.getStatusLine()
        int statusCode = statusLine.getStatusCode()
        //String body = EntityUtils.toString(response.getEntity())
        if (method == 'PUT') {
            if (statusCode == 201) {
                log.info("New named graph " + docURI + " created successfully")
            } else if (statusCode == 200) {
                log.info("Named graph " + docURI + " updated successfully")
            } else {
                log.warn("Failed to named create/update graph " + docURI + ", response was " + statusLine.toString())
            }
        } else if (method == 'DELETE') {
            if (response.getStatusLine().getStatusCode() == 200) {
                log.info("Named graph " + docURI + " deleted successfully")
            } else {
                log.warn("Failed to delete named graph " + docURI + ", response was " + statusLine.toString())
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
