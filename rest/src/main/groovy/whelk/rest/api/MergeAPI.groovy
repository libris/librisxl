package whelk.rest.api

import whelk.Document
import whelk.JsonLd
import whelk.Whelk
import whelk.component.ElasticSearch
import whelk.component.PostgreSQLComponent
import whelk.triples.*
import whelk.util.LegacyIntegrationTools

import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

class MergeAPI extends HttpServlet {

    private Whelk m_whelk

    MergeAPI() {
        // Do nothing - only here for Tomcat to have something to call
    }

    MergeAPI(Whelk whelk) {
        this.whelk = whelk
    }

    @Override
    void init() {
        if (!whelk) {
            m_whelk = Whelk.createLoadedSearchWhelk()
        }
    }

    private String getRecordId(String id) {
        id = whelk.util.LegacyIntegrationTools.fixUri(id)
        return m_whelk.storage.getRecordId(id)
    }

    @Override
    void doGet(HttpServletRequest request, HttpServletResponse response) {
        boolean commit = false
        handleRequest(request, response, commit)
    }

    @Override
    void doPost(HttpServletRequest request, HttpServletResponse response) {
        boolean commit = true
        handleRequest(request, response, commit)
    }

    void handleRequest(HttpServletRequest request, HttpServletResponse response, boolean commit) {

        // Validate parameters

        String id1 = request.getParameter("id1")
        String id2 = request.getParameter("id2")
        String promoteId2Param = request.getParameter("promote_id2")

        boolean promoteId2 = false
        if (promoteId2Param != null && promoteId2Param.equalsIgnoreCase("true"))
            promoteId2 = true

        if (id1 == null || id2 == null) {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST,
                    "\"id1\" and \"id2\" parameters required.")
            return
        }

        id1 = getRecordId(id1)
        id2 = getRecordId(id2)

        if (id1 == null || id2 == null) {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST,
                    "\"id1\" and \"id2\" must both refer to valid records.")
            return
        }

        // Perform merge

        Document firstClassDocument = m_whelk.storage.loadDocumentByMainId(id1)
        Document secondClassDocument = m_whelk.storage.loadDocumentByMainId(id2)
        String remainingID = id1
        String disappearingID = id2

        String collection = LegacyIntegrationTools.determineLegacyCollection(firstClassDocument, m_whelk.getJsonld())
        if (!LegacyIntegrationTools.determineLegacyCollection(secondClassDocument, m_whelk.getJsonld()).equals(collection)) {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST,
                    "$id1 and $id2 are not in the same collection.")
        }

        if (promoteId2) {
            firstClassDocument.deepPromoteId(id2)
            remainingID = id2
            disappearingID = id1
        }
        else {
            secondClassDocument.deepPromoteId(id1)
        }

        Document merged = merge(firstClassDocument, secondClassDocument)

        // Make use of merged result

        if (commit) {
            Map userInfo = request.getAttribute("user")
            boolean hasPermission = false
            if (userInfo != null) {
                if (userInfo.permissions.any { item ->
                    item.get(whelk.rest.security.AccessControl.KAT_KEY)
                } || Crud.isSystemUser(userInfo))
                    hasPermission = true
            }
            if (!hasPermission) {
                response.sendError(HttpServletResponse.SC_FORBIDDEN,
                        "You must be authorized in order to commit merges.")
                return
            }

            m_whelk.mergeExisting(remainingID, disappearingID, merged, "xl", null, collection)
            response.setStatus(HttpServletResponse.SC_OK)
            return

        } else {
            response.setStatus(HttpServletResponse.SC_OK)
            response.getOutputStream().println(PostgreSQLComponent.mapper.writeValueAsString(merged.data))
            return
        }
    }

    private Document merge(Document firstClassDocument, Document secondClassDocument){
        JsonldSerializer serializer = new JsonldSerializer()
        List<String[]> withTriples = serializer.deserialize(secondClassDocument.data)
        List<String[]> originalTriples = serializer.deserialize(firstClassDocument.data)

        Graph originalGraph = new Graph(originalTriples)
        Graph withGraph = new Graph(withTriples)

        Map<String, Graph.PREDICATE_RULES> specialRules = new HashMap<>()
        for (String term : m_whelk.getJsonld().getForcedSetTerms())
            specialRules.put(term, Graph.PREDICATE_RULES.RULE_AGGREGATE)

        originalGraph.enrichWith(withGraph, specialRules)

        Map enrichedData = JsonldSerializer.serialize(originalGraph.getTriples(), m_whelk.getJsonld().getForcedSetTerms())
        boolean deleteUnreferencedData = true
        JsonldSerializer.normalize(enrichedData, firstClassDocument.getShortId(), deleteUnreferencedData)
        return new Document(enrichedData)
    }
}
