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
import org.openrdf.query.resultio.TupleQueryResultFormat
import org.openrdf.query.resultio.TupleQueryResultWriter
import org.openrdf.query.resultio.TupleQueryResultWriterRegistry
import org.openrdf.query.resultio.BooleanQueryResultFormat
import org.openrdf.query.resultio.BooleanQueryResultWriter
import org.openrdf.query.resultio.BooleanQueryResultWriterRegistry
import org.openrdf.query.GraphQuery
import org.openrdf.query.GraphQueryResult
import org.openrdf.query.BooleanQuery
import org.openrdf.query.QueryEvaluationException


import se.kb.libris.whelks.*
import se.kb.libris.whelks.plugin.*
import se.kb.libris.whelks.result.*

@Log
class SesameGraphStore extends BasicPlugin implements GraphStore,SparqlEndpoint {

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

    void update(URI graphUri, Document doc) {
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

    public void delete(URI graphUri, String whelkId) {
    }

    public InputStream sparql(String sparql) {
        if (!conn.open) {
            log.debug("Connection is closed. Reopening.")
            conn = repo.getConnection()
        }
        log.trace("Received query: $sparql")
        Query query = conn.prepareQuery(QueryLanguage.SPARQL, sparql)
        if (query instanceof GraphQuery) {
            log.trace("Executing graphquery")
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
            return new ByteArrayInputStream(out.toByteArray())
        }
        if (query instanceof TupleQuery) {
            log.trace("Executing tuplequery")
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
        if (query instanceof BooleanQuery) {
            log.trace("Executing booleanquery")
            final boolean result = ((BooleanQuery) query).evaluate()
            ByteArrayOutputStream out = new ByteArrayOutputStream()
            BooleanQueryResultWriter writer = BooleanQueryResultWriterRegistry.getInstance().get(BooleanQueryResultFormat.SPARQL).getWriter(out)
            try {
                writer.write(result)
                return new ByteArrayInputStream(out.toByteArray())
            } catch (Exception e) {
                throw new AssertionError(e)
            }

        }
        log.debug("Sending crap")
        return new ByteArrayInputStream("This is crap".getBytes())
    }
}
