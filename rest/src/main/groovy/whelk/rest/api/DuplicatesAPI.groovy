package whelk.rest.api

import whelk.Document
import whelk.JsonLd
import whelk.Whelk
import whelk.component.PostgreSQLComponent
import whelk.converter.marc.JsonLD2MarcXMLConverter
import whelk.util.PropertyLoader

import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

class DuplicatesAPI extends HttpServlet {

    private Whelk whelk
    private JsonLd jsonld
    private JsonLD2MarcXMLConverter toMarcXmlConverter

    @Override
    void init() {
        Properties configuration = PropertyLoader.loadProperties("secret")
        PostgreSQLComponent storage = new PostgreSQLComponent(configuration.getProperty("sqlUrl"),
                configuration.getProperty("sqlMaintable"))
        whelk = new Whelk(storage)
        whelk.loadCoreData()
        jsonld = new JsonLd(whelk.displayData, whelk.vocabData)
        toMarcXmlConverter = new JsonLD2MarcXMLConverter()
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

        out.close()
    }
}
