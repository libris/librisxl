package whelk.rest.api

import whelk.Document
import whelk.Whelk
import whelk.component.PostgreSQLComponent
import whelk.util.*

import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

class RecordRelationAPI extends HttpServlet {

    private Whelk whelk

    RecordRelationAPI() {
        // Do nothing - only here for Tomcat to have something to call
    }

    RecordRelationAPI(Whelk whelk) {
        this.whelk = whelk
    }

    @Override
    void init() {
        if (!whelk) {
            whelk = Whelk.createLoadedCoreWhelk()
        }
    }

    @Override
    void doGet(HttpServletRequest request, HttpServletResponse response) {
        String id = request.getParameter("id")
        String relation = request.getParameter("relation")
        String reverseString = request.getParameter("reverse")

        boolean reverse = false
        if (reverseString != null && reverseString.equals("true"))
            reverse = true

        if (id == null) {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST,
                    "\"id\" parameter required.")
            return
        }

        id = LegacyIntegrationTools.fixUri(id)
        String systemId = whelk.storage.getSystemIdByIri(id)
        if (systemId == null) {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST,
                    "The supplied \"id\"-parameter must refer to an existing bibliographic record.")
            return
        }

        ArrayList<String> result = []
        if (relation == null) {
            List<Tuple2<String, String>> dependencySystemIDs
            if (reverse)
                dependencySystemIDs = whelk.storage.getDependers(systemId)
            else
                dependencySystemIDs = whelk.storage.getDependencies(systemId)

            for (Tuple2<String, String> dependencySystemId : dependencySystemIDs){
                result.add(Document.getBASE_URI().toString() + dependencySystemId.get(0))
            }
        }
        else {
            List<String> dependencySystemIDs
            if (reverse)
                dependencySystemIDs = whelk.storage.getDependersOfType(systemId, relation)
            else
                dependencySystemIDs = whelk.storage.getDependenciesOfType(systemId, relation)
            
            for (String dependencySystemId : dependencySystemIDs){
                result.add(Document.getBASE_URI().toString() + dependencySystemId)
            }
        }


        String jsonString = PostgreSQLComponent.mapper.writeValueAsString(result)
        response.setContentType("application/json")
        response.setHeader("Expires", "0")
        OutputStream out = response.getOutputStream()
        out.write(jsonString.getBytes("UTF-8"))
        out.close()
    }
}
