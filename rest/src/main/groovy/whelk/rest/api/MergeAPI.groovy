package whelk.rest.api

import whelk.Document
import whelk.Whelk
import whelk.component.PostgreSQLComponent
import whelk.triples.Graph
import whelk.triples.JsonldSerializer
import whelk.util.LegacyIntegrationTools

import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

class MergeAPI extends HttpServlet {

    private final static String[] ALLOWED_CATALOGING_SIGELS = ['SEK']
    private Whelk whelk

    MergeAPI() {
        // Do nothing - only here for Tomcat to have something to call
    }

    MergeAPI(Whelk whelk) {
        this.whelk = whelk
    }

    @Override
    void init() {
        if (!whelk) {
            whelk = Whelk.createLoadedSearchWhelk()
        }
    }

    private String getRecordId(String id) {
        id = whelk.util.LegacyIntegrationTools.fixUri(id)
        return whelk.storage.getRecordId(id)
    }

    @Override
    void doGet(HttpServletRequest request, HttpServletResponse response) {
        boolean commit = false
        handleRequest(request, response, commit)
    }

    @Override
    void doPost(HttpServletRequest request, HttpServletResponse response) {
        if (!hasPermission(request)) {
            response.sendError(HttpServletResponse.SC_FORBIDDEN,
                    "You must be authorized in order to commit merges.")
            return
        }

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

        Document firstClassDocument = whelk.storage.loadDocumentByMainId(id1)
        Document secondClassDocument = whelk.storage.loadDocumentByMainId(id2)
        String remainingID = id1
        String disappearingID = id2

        String collection = LegacyIntegrationTools.determineLegacyCollection(firstClassDocument, whelk.getJsonld())
        if (!LegacyIntegrationTools.determineLegacyCollection(secondClassDocument, whelk.getJsonld()).equals(collection)) {
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

        response.setHeader('Cache-Control', 'no-cache')

        // Make use of merged result

        if (commit) {
            whelk.mergeExisting(remainingID, disappearingID, merged, "xl", null, collection)
            response.setStatus(HttpServletResponse.SC_OK)
            return

        } else {
            response.setStatus(HttpServletResponse.SC_OK)
            response.getOutputStream().println(PostgreSQLComponent.mapper.writeValueAsString(merged.data))
            return
        }
    }

    private boolean hasPermission(request) {
        Map user = request.getAttribute('user')
        if (!user) {
            return false
        }
        if (Crud.isSystemUser(user)) {
            return true
        }

        user.permissions.any { permission ->
            if (permission.get('code') in ALLOWED_CATALOGING_SIGELS &&
                    permission.get(whelk.rest.security.AccessControl.KAT_KEY)) {
                return true
            }
        }
    }


    private Document merge(Document firstClassDocument, Document secondClassDocument){
        JsonldSerializer serializer = new JsonldSerializer()
        List<String[]> withTriples = serializer.deserialize(secondClassDocument.data)
        List<String[]> originalTriples = serializer.deserialize(firstClassDocument.data)

        Graph originalGraph = new Graph(originalTriples)
        Graph withGraph = new Graph(withTriples)

        Map<String, Graph.PREDICATE_RULES> specialRules = new HashMap<>()
        for (String term : whelk.getJsonld().getRepeatableTerms())
            specialRules.put(term, Graph.PREDICATE_RULES.RULE_AGGREGATE)

        originalGraph.enrichWith(withGraph, specialRules)

        Map enrichedData = JsonldSerializer.serialize(originalGraph.getTriples(), whelk.getJsonld().getRepeatableTerms())
        boolean deleteUnreferencedData = true
        JsonldSerializer.normalize(enrichedData, firstClassDocument.getShortId(), deleteUnreferencedData)
        return new Document(enrichedData)
    }
}
