package se.kb.libris.whelks.component

import groovy.util.logging.Slf4j as Log

import org.openrdf.model.Resource
import org.openrdf.repository.Repository
import org.openrdf.repository.RepositoryConnection
import org.openrdf.repository.http.HTTPRepository
import org.openrdf.rio.RDFFormat


import se.kb.libris.whelks.*
import se.kb.libris.whelks.plugin.*
import se.kb.libris.whelks.result.*

@Log
class SesameGraphStore extends BasicPlugin implements GraphStore {

    String sesameServer
    String repositoryID
    Repository repo
    RepositoryConnection conn
    String id = "sesameGraphStoreComponent"

    SesameGraphStore(String sesameServer, String repositoryID) {
        repo = new HTTPRepository(sesameServer, repositoryID)
        repo.initialize()
        conn = repo.getConnection()
    }

    void update(URI graphUri, RDFDescription doc) {
        if (!conn.open) {
            log.debug("Connection is closed. Reopening.")
            conn = repo.getConnection()
        }
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

    public void delete(URI graphUri) {
    }

    public InputStream sparql(String query) {
        return null
    }
}
