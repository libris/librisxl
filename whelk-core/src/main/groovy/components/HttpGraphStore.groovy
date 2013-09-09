package se.kb.libris.whelks.component

import groovy.util.logging.Slf4j as Log

import org.apache.http.client.*
import org.apache.http.client.methods.*
import org.apache.http.client.utils.*
import org.apache.http.entity.*
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.util.EntityUtils

import se.kb.libris.whelks.*
import se.kb.libris.whelks.plugin.*

@Log
class HttpGraphStore extends BasicPlugin implements GraphStore {

    HttpClient client
    String graphStoreURI
    String queryURI
    String id = "httpGraphStoreComponent"

    HttpGraphStore(Map settings) {
        this.graphStoreURI = settings['graphStoreUri']
        this.queryURI = settings['queryUri']
        client = new DefaultHttpClient()
    }

    void update(URI graph, RDFDescription doc) {
        def uri = new URIBuilder(graphStoreURI).addParameter("graph", graph.toString()).build()
        HttpPut put = new HttpPut(uri)
        def entity = new ByteArrayEntity(doc.data)
        entity.setContentType(doc.contentType)
        log.info("PUT <${uri}> with content type '${doc.contentType}'")
        put.setEntity(entity)
        def response = client.execute(put)
        log.info("Server response: ${response.statusLine.statusCode}")
        EntityUtils.consumeQuietly(response.getEntity())
    }

    public void delete(URI uri, String whelkId) {
    }

    public SparqlResult sparql(String query) {
        return null
    }
}
