package se.kb.libris.whelks.component

import groovy.util.logging.Slf4j as Log

import org.openrdf.model.Resource
import org.openrdf.repository.Repository
import org.openrdf.repository.RepositoryConnection
import org.openrdf.repository.http.HTTPRepository
import org.openrdf.rio.RDFFormat
import org.openrdf.rio.RDFWriter
import org.openrdf.rio.RDFWriterRegistry
import org.openrdf.query.Query
import org.openrdf.query.QueryLanguage
import org.openrdf.query.TupleQuery
import org.openrdf.query.TupleQueryResult
import org.openrdf.query.resultio.TupleQueryResultWriter
import org.openrdf.query.resultio.TupleQueryResultWriterRegistry
import org.openrdf.query.GraphQuery
import org.openrdf.query.GraphQueryResult
import org.openrdf.query.QueryEvaluationException


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

    public InputStream sparql(String sparql) {
        if (!conn.open) {
            log.debug("Connection is closed. Reopening.")
            conn = repo.getConnection()
        }
        log.debug("Received query: $sparql")
        Query query = conn.prepareQuery(QueryLanguage.SPARQL, sparql)
        log.debug("Query is ${query.getClass().getName()}")
        if (query instanceof GraphQuery) {
            final GraphQueryResult result = ((GraphQuery) query).evaluate()
            try {
                ByteArrayOutputStream out = new ByteArrayOutputStream()
                RDFWriter writer = RDFWriterRegistry.getInstance().get(
                    RDFFormat.RDFXML).getWriter(out)
                writer.startRDF()
                while (result.hasNext()) {
                    writer.handleStatement(result.next())
                }
                writer.endRDF()

            } finally {
                try {
                    result.close()
                } catch (QueryEvaluationException e) {
                    throw new AssertionError(e)
                }
            }
            log.debug("Sending inputstream")
            return new ByteArrayInputStream(out.toByteArray())
        }
        if (query instanceof TupleQuery) {
            final TupleQueryResult result = ((TupleQuery) query).evaluate()
            ByteArrayOutputStream out = new ByteArrayOutputStream()
            TupleQueryResultWriter writer = TupleQueryResultWriterRegistry.getInstance().get(TupleQueryResultFormat.SPARQL).getWriter(out)
            try {
                writer.startQueryResult(result.getBindingNames())
                while (result.hasNext()) {
                    writer.handleSolution(result.next())
                }
                writer.endQueryResult()
                log.debug("Sending tuple inputstream")
                return new ByteArrayInputStream(out.toByteArray()) {
                    @Override
                    public void close() throws IOException {
                        try {
                            super.close()
                        } finally {
                            try {
                                result.close()
                            } catch (QueryEvaluationException e) {
                                throw new AssertionError(e)
                            }
                        }
                    }
                }
            } catch (Exception e) {
                throw new AssertionError(e)
            }

        }
        log.debug("Sending crap")
        return new ByteArrayInputStream("This is crap".getBytes())
    }
}
