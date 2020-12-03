package whelk.component

import groovy.util.logging.Log4j2 as Log
import org.apache.http.HttpResponse
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
import whelk.Document
import whelk.converter.JsonLdToTurtle

@Log
class Virtuoso {

    private String sparqlCrudEndpoint
    private String sparqlUser
    private String sparqlPassword

    private HttpClient httpClient

    Map jsonldContext

    Virtuoso(Properties props) {
        this.sparqlCrudEndpoint = props.getProperty("sparqlCrudEndpoint")
        this.sparqlUser = props.getProperty("sparqlUser")
        this.sparqlPassword = props.getProperty("sparqlPassword")

        this.httpClient = new DefaultHttpClient()
    }

    void deleteGraph(Document doc) {
        updateGraph('DELETE', doc)
    }

    void insertGraph(Document doc) {
        updateGraph('PUT', doc)
    }

    private void updateGraph(String method, Document doc) {
        HttpRequestBase request = buildRequest(method, doc)
        HttpResponse response = performRequest(request)
        handleResponse(response, method, doc.completeId())
    }

    private String createGraphCrudURI(String docURI) {
        return sparqlCrudEndpoint + "?graph=" + docURI
    }

    private HttpRequestBase buildRequest(String method, Document doc) {
        String graphCrudURI = createGraphCrudURI(doc.completeId())
        switch (method) {
            case 'DELETE':
                return new HttpDelete(graphCrudURI)
            case 'PUT':
                HttpPut request = new HttpPut(graphCrudURI)
                String turtleDoc = convertToTurtle(doc, jsonldContext)
                request.setEntity(new StringEntity(turtleDoc, 'UTF-8'))
                return request
            default:
                throw new IllegalArgumentException("Bad request method:" + method)
        }
    }

    private HttpResponse performRequest(HttpRequestBase request) {
        basicAuthenticate()
        HttpResponse response = httpClient.execute(request)
        request.releaseConnection()
        return response
    }

    private void handleResponse(HttpResponse response, String method, String docURI) {
        if (method == 'PUT') {
            if (response.getStatusLine().getStatusCode() == 201) {
                log.info("New graph " + docURI + " created successfully")
            } else if (response.getStatusLine().getStatusCode() == 200) {
                log.info("Graph " + docURI + " updated successfully")
            } else {
                log.info("Failed to create/update graph " + docURI)
            }
        } else if (method == 'DELETE') {
            if (response.getStatusLine().getStatusCode() == 200) {
                log.info("Graph " + docURI + " deleted successfully")
            } else {
                log.info("Failed to delete graph " + docURI)
            }
        }
    }

    private String convertToTurtle(Document doc, Map context) {
        Map ctx = JsonLdToTurtle.parseContext([
                '@context': context
        ])

        Map opts = [markEmptyBnode: true]

        ByteArrayOutputStream out = new ByteArrayOutputStream()

        JsonLdToTurtle serializer = new JsonLdToTurtle(ctx, out, opts)

        serializer.toTurtle(doc.data)

        return new String(out.toByteArray())
    }

    private void basicAuthenticate() {
        CredentialsProvider provider = new BasicCredentialsProvider()
        provider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(sparqlUser, sparqlPassword))
        httpClient.setCredentialsProvider(provider)
    }
}
