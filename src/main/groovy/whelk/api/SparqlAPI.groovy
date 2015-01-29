package whelk.api

import groovy.util.logging.Slf4j as Log

import javax.servlet.http.*

import whelk.*
import whelk.component.*
import whelk.plugin.*
import whelk.exception.*

@Log
class SparqlAPI extends BasicAPI {

    String description = "Provides sparql endpoint to the underlying triple store."

    @Override
    void doHandle(HttpServletRequest request, HttpServletResponse response, List pathVars) {
        def query = request.getParameter("query")

        InputStream is
        try {
            is = whelk.sparql(query)
        } catch (Exception e) {
            is = new ByteArrayInputStream(e.message.bytes)
            log.warn("Query $query resulted in error: ${e.message}", e)
            query = null
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST)
        }

        response.setCharacterEncoding("UTF-8")
        if (!query) {
            response.setContentType("text/plain")
        }
        if (query.toUpperCase().contains("SELECT") || query.toUpperCase().contains("ASK")) {
            response.setContentType("application/sparql-results+xml")
        } else {
            response.setContentType("application/rdf+xml")
        }

        def output = response.getOutputStream()
        byte[] b = new byte[8]
        int read
        while ((read = is.read(b)) != -1) {
            output.write(b, 0, read)
            output.flush()
        }
    }
}
