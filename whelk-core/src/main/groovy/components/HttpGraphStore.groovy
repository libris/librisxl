package se.kb.libris.whelks.component

import org.apache.http.client.*

@Log
HttpGraphStore extends BasicPlugin implements GraphStore {

    HttpClient client
    String graphStorePutURI = "http://localhost/rdfstore"
    String id = "httpGraphStoreComponent"

    HttpGraphStore() {
        client = new DefaultHttpClient()
    }

    void update(URI graph, Document doc) {
        HttpPut put = new HttpPut(new URIBuilder(graphStorePutURI).addParameter("graph", graph.toString()).build())
        def entity = new ByteArrayEntity(doc.data)
        entity.setContentType(doc.contentType)
        put.setEntity(entity)
        def response = client.execute(put)
        log.info("Server response: ${response.statusLine.statusCode}")
    }
}
