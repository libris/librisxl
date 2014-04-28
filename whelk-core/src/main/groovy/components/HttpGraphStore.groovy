package se.kb.libris.whelks.component

import groovy.util.logging.Slf4j as Log

import org.apache.http.client.*
import org.apache.http.client.methods.*
import org.apache.http.client.utils.*
import org.apache.http.entity.*
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.util.EntityUtils
import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.http.message.BasicNameValuePair
import org.apache.http.impl.conn.*
import org.apache.http.impl.client.*
import org.apache.http.client.protocol.*

import se.kb.libris.whelks.*
import se.kb.libris.whelks.plugin.*
import se.kb.libris.whelks.result.*
import se.kb.libris.whelks.exception.*

@Log
class HttpEndpoint extends BasicPlugin implements SparqlEndpoint {
    PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager()
    HttpClient client = HttpClients.custom().setConnectionManager(cm).build()
    String queryURI
    String id = "httpSparqlEndpoint"

    HttpEndpoint(Map settings) {
        this.queryURI = settings['queryUri']
    }

    public InputStream sparql(String query) {

        log.info("Received SPARQL query: $query")

        def uri = new URIBuilder(queryURI).build()
        log.info("POSTing to URI $uri")
        HttpPost post = new HttpPost(uri)
        post.setEntity(new UrlEncodedFormEntity([new BasicNameValuePair("query", query)]))

        def response = client.execute(post)
        if (response.statusLine.statusCode != 200 && response.statusLine.statusCode != 400) {
            throw new WhelkRuntimeException("Bad status ($response.statusLine.statusCode) from $queryURI")
        }
        log.info("Response: $response")
        log.info("Sending response as bytearrayinputstream")

        return new ByteArrayInputStream(EntityUtils.toByteArray(response.getEntity()))
    }

    public void delete(URI uri) {
    }
}

@Log
class HttpGraphStore extends HttpEndpoint implements GraphStore {

    String graphStoreURI
    String id = "httpGraphStoreComponent"

    HttpGraphStore(Map settings) {
        super(settings)
        this.graphStoreURI = settings['graphStoreUri']
        this.queryURI = settings['queryUri']
    }

    void update(URI graphUri, RDFDescription doc) {
        def uri = new URIBuilder(graphStoreURI).addParameter("graph", graphUri.toString()).build()
        HttpPut put = new HttpPut(uri)
        def entity = new ByteArrayEntity(doc.data)
        entity.setContentType(doc.contentType)
        log.debug("PUT <${uri}> with content type '${doc.contentType}'")
        put.setEntity(entity)
        def response = client.execute(put, HttpClientContext.create())
        log.debug("Server response: ${response.statusLine.statusCode}")
        EntityUtils.consumeQuietly(response.getEntity())
    }

}

@Log
class HttpBatchGraphStore extends HttpGraphStore implements BatchGraphStore {

    String updateURI
    /**
     * A value larger than zero indicates that load speed can be improved by
     * using the batchUpdate method, with a batch of documents of the indicated
     * size.
     */
    int optimumBatchSize = 0

    HttpBatchGraphStore(Map settings) {
        super(settings)
        this.graphStoreURI = settings['graphStoreUri']
        this.queryURI = settings['queryUri']
        this.updateURI = settings['updateUri']
        this.optimumBatchSize = settings.get('optimumBatchSize', 0)
    }

    void batchUpdate(Map<URI, RDFDescription> batch) {
        def prefixes = ""
        def inserts = []
        batch.each { graphUri, doc ->
            def turtle = doc.dataAsString
            inserts << "CLEAR GRAPH <$graphUri> ;"
            inserts << "INSERT DATA { GRAPH <$graphUri> { $turtle } } ;"
        }
        def body = prefixes + inserts.join("\n")

        def uri = new URIBuilder(updateURI).build()
        def post = new HttpPost(uri)
        def params = new BasicNameValuePair("update", body)
        def entity = new UrlEncodedFormEntity([params])
        post.setEntity(entity)
        log.debug("posting: $body")
        def response = client.execute(post, HttpClientContext.create())
        log.debug("Server response: ${response.statusLine.statusCode}")
        EntityUtils.consumeQuietly(response.getEntity())
    }

}
