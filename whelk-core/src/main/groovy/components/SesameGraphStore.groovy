package se.kb.libris.whelks.component

import groovy.util.logging.Slf4j as Log

import org.openrdf.model.Resource
import org.openrdf.repository.Repository
import org.openrdf.repository.http.HTTPRepository
import org.openrdf.rio.RDFFormat


import se.kb.libris.whelks.*
import se.kb.libris.whelks.plugin.*

@Log
class SesameGraphStore extends BasicPlugin implements GraphStore {

    String sesameServer
    String repositoryID
    Repository repo
    String id = "sesameGraphStoreComponent"

    SesameGraphStore(String sesameServer, String repositoryID) {
        repo = new HTTPRepository(sesameServer, repositoryID)
        repo.initialize()
    }

    void update(URI graphUri, RDFDescription doc) {
        // TODO: not new conn for each? pool?
        def conn = repo.getConnection()
        try {
            log.debug("Store <${graphUri}> with content type '${doc.contentType}'")
            def bis = new ByteArrayInputStream(doc.data)
            def graphId = conn.valueFactory.createURI(graphUri.toString())
            //conn.clear(graphId) // TODO: should do this, but it is too slow by default
            conn.add(bis, graphUri.toString(), RDFFormat.TURTLE, graphId)
        } catch (Exception e) {
            log.error("Error in document ${graphUri} - data:\n${new String(doc.data, 'utf-8')}\n\n")
            throw e
        } finally {
            conn.close()
        }
    }

    public void delete(URI graphUri, String whelkId) {
    }

    public SparqlResult sparql(String query) {
        return null
    }
}
