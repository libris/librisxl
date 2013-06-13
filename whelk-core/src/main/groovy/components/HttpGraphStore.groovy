package se.kb.libris.whelks.component

import groovy.util.logging.Slf4j as Log

import org.apache.http.client.*
import org.apache.http.client.utils.*
import org.apache.http.client.methods.*
import org.apache.http.entity.*
import org.apache.http.impl.client.DefaultHttpClient

import se.kb.libris.whelks.*
import se.kb.libris.whelks.plugin.*

@Log
class HttpGraphStore extends BasicPlugin implements GraphStore {

    HttpClient client
    String graphStorePutURI = "http://localhost/rdfstore"
    String id = "httpGraphStoreComponent"

    HttpGraphStore() {
        client = new DefaultHttpClient()
    }

    void update(URI graph, RDFDescription doc) {
        HttpPut put = new HttpPut(new URIBuilder(graphStorePutURI).addParameter("graph", graph.toString()).build())
        def entity = new ByteArrayEntity(doc.data)
        entity.setContentType(doc.contentType)
        put.setEntity(entity)
        def response = client.execute(put)
        log.info("Server response: ${response.statusLine.statusCode}")
    }

    public void delete(URI uri, String whelkId) {
    }

    public SparqlResult sparql(String query) {
        return null
    }
}
