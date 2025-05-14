package whelk.rest.api

import whelk.Document
import whelk.Whelk
import whelk.converter.marc.JsonLD2MarcXMLConverter
import whelk.util.http.WhelkHttpServlet

import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

class DuplicatesAPI extends WhelkHttpServlet {
    private JsonLD2MarcXMLConverter toMarcXmlConverter

    DuplicatesAPI() {
        // Do nothing - only here for Tomcat to have something to call
    }

    DuplicatesAPI(Whelk whelk) {
        this.whelk = whelk
        init(whelk)
    }

    @Override
    void init(Whelk whelk) {
        toMarcXmlConverter = new JsonLD2MarcXMLConverter(whelk.getMarcFrameConverter())
    }

    @Override
    void doGet(HttpServletRequest request, HttpServletResponse response) {

        PrintWriter out = response.getWriter()

        for (Document doc in whelk.getStorage().loadAll("bib")) {
            boolean includingTypedIDs = true
            List<Tuple2<String, String>> collisions = whelk.getIdCollisions(doc, includingTypedIDs)
            if (!collisions.isEmpty()) {
                out.println(doc.getShortId() + " has potential duplicates:")
                for (Tuple2<String, String> collision : collisions) {
                    out.println("\t" + collision.get(0) + " " + collision.get(1))
                }
                out.println()
            }
        }

        response.setHeader('Cache-Control', 'no-cache')
        out.close()
    }
}
