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

import se.kb.libris.whelks.*
import se.kb.libris.whelks.plugin.*
import se.kb.libris.whelks.result.*
import se.kb.libris.whelks.exception.*

@Log
class HttpEndpoint extends BasicPlugin implements SparqlEndpoint {
    HttpClient client
    String queryURI
    String id = "httpSparqlEndpoint"

    HttpEndpoint(Map settings) {
        this.queryURI = settings['queryUri']
        client = new DefaultHttpClient()
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
class HttpGraphStore extends HttpEndpoint implements SparqlEndpoint, GraphStore {

    String graphStoreURI
    String id = "httpGraphStoreComponent"

    HttpGraphStore(Map settings) {
        super(settings)
        this.graphStoreURI = settings['graphStoreUri']
        this.queryURI = settings['queryUri']
    }

    void update(URI graph, RDFDescription doc) {
        def uri = new URIBuilder(graphStoreURI).addParameter("graph", graph.toString()).build()
        HttpPut put = new HttpPut(uri)
        def entity = new ByteArrayEntity(doc.data)
        entity.setContentType(doc.contentType)
        log.debug("PUT <${uri}> with content type '${doc.contentType}'")
        put.setEntity(entity)
        def response = client.execute(put)
        log.debug("Server response: ${response.statusLine.statusCode}")
        EntityUtils.consumeQuietly(response.getEntity())
    }
}
