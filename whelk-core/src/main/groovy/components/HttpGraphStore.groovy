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

import org.codehaus.jackson.map.ObjectMapper

@Log
class HttpEndpoint extends BasicComponent implements SparqlEndpoint {
    PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager()
    HttpClient client = HttpClients.custom().setConnectionManager(cm).build()
    String queryURI
    String id = "httpSparqlEndpoint"

    HttpEndpoint(Map settings) {
        this.queryURI = settings['queryUri']
    }

    protected void batchLoad(List<Document> docs) {
        throw new UnsupportedOperationException("HttpEndpoint can only query graph store.")
    }

    public Document get(URI uri) {
        throw new UnsupportedOperationException("Not implemented yet.")
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

    public void remove(URI uri) {
        throw new UnsupportedOperationException("Not implemented yet.")
    }
}

@Log
class HttpGraphStore extends HttpEndpoint implements GraphStore {

    String graphStoreURI
    String id = "httpGraphStoreComponent"
    def context
    String baseUri

    List<String> acceptableContentTypes = [
        "text/turtle",
        "application/rdf+xml",
        "application/ld+json"
    ]

    boolean accepts(String contentType) {
        return acceptableContentTypes.contains(contentType)
    }

    boolean accepts(Document doc) {
        if (!accepts(doc.contentType)) {
            log.debug("Rejected doc: $doc.identifier (unacceptable content type $doc.contentType)")
            return false
        }
        return true
    }

    HttpGraphStore(Map settings) {
        super(settings)
        this.graphStoreURI = settings['graphStoreUri']
        this.queryURI = settings['queryUri']
        this.context = loadJsonLdContext(settings['contextPath'])
        this.baseUri = settings['baseUri']
    }

    def loadJsonLdContext(contextPath) {
        def mapper = new ObjectMapper()
        def loader = getClass().classLoader
        def contextData = loader.getResourceAsStream(contextPath).withStream {
            mapper.readValue(it, Map)
        }
        return JsonLdToTurtle.parseContext(contextData)
    }

    void update(URI graphUri, Document doc) {
        if (!accepts(doc)) {
            return
        }
        def uri = new URIBuilder(graphStoreURI).addParameter("graph", graphUri.toString()).build()
        HttpPut put = new HttpPut(uri)
        def contentType = doc.contentType
        def data = doc.data
        if (contentType == "application/ld+json") {
            def source = mapper.readValue(data, Map)
            def bytes = JsonLdToTurtle.toTurtle(context, source, baseUri).toByteArray()
            data = bytes
            contentType = "text/turtle"
        }
        def entity = new ByteArrayEntity(data)
        entity.setContentType(contentType)
        log.debug("PUT <${uri}> with content type '${contentType}'")
        put.setEntity(entity)
        def response = client.execute(put, HttpClientContext.create())
        log.debug("Server response: ${response.statusLine.statusCode}")
        EntityUtils.consumeQuietly(response.getEntity())
    }

    protected void batchLoad(List<Document> docs) {
        for (doc in docs) {
            update(new URI(doc.identifier), doc)
        }
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

    @Override
    void batchLoad(List<Document> docs) {
        batchUpdate(docs)
    }

    void batchUpdate(List<Document> batch) {
        def bos = new ByteArrayOutputStream()
        def serializer = new JsonLdToTurtle(context, bos, baseUri)
        serializer.prelude() // prefixes and base
        batch.each {
            serializer.uniqueBNodeSuffix = "-${System.nanoTime()}"
            if (!accepts(it)) {
                return
            }
            def graphUri = it.identifier
            serializer.writeln "CLEAR GRAPH <$graphUri> ;"
            serializer.writeln "INSERT DATA { GRAPH <$graphUri> {"
            serializer.flush()
            serializer.objectToTurtle(it.dataAsMap)
            serializer.writeln "} } ;"
            serializer.flush()
        }
        def body = bos.toString("UTF-8")

        def uri = new URIBuilder(updateURI).build()
        def post = new HttpPost(uri)
        def params = new BasicNameValuePair("update", body)
        def entity = new UrlEncodedFormEntity([params])
        post.setEntity(entity)
        if (log.isDebugEnabled()) {
            log.debug("Posting: $body")
        }
        def response = client.execute(post, HttpClientContext.create())
        log.debug("Server response: ${response.statusLine.statusCode}")
        if (response.statusLine.statusCode == 200) {
            EntityUtils.consumeQuietly(response.getEntity())
        } else if (response.statusLine.statusCode == 400) {
            log.warn("Error reponse 400: ${response.statusLine.reasonPhrase}")
        } else {
            throw new WhelkAddException("Batch update failed: ${EntityUtils.toString(response.getEntity(), "utf-8")}", [])
        }
    }

}
