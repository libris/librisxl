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
    private Set<String> m_forcedSetTerms
    private JsonLd m_jsonld

    @Override
    void init() {
        Properties configuration = whelk.util.PropertyLoader.loadProperties("secret")
        PostgreSQLComponent storage = new PostgreSQLComponent(configuration.getProperty("sqlUrl"),
                configuration.getProperty("sqlMaintable"))
        ElasticSearch elastic = new ElasticSearch(configuration)
        m_whelk = new Whelk(storage, elastic)

        m_whelk.loadCoreData()
        m_jsonld = new JsonLd(m_whelk.getDisplayData(), m_whelk.getVocabData())
        m_forcedSetTerms = m_jsonld.getForcedSetTerms()
    }

    private String getRecordId(String id) {
        id = whelk.util.LegacyIntegrationTools.fixUri(id)
        return m_whelk.storage.getRecordId(id)
    }

    @Override
    void doPost(HttpServletRequest request, HttpServletResponse response) {

        // Validate parameters

        String id1 = request.getParameter("id1")
        String id2 = request.getParameter("id2")
        String promoteId2Param = request.getParameter("promote_id2")
        String commitParam = request.getParameter("commit")

        boolean promoteId2 = false
        if (promoteId2Param != null && promoteId2Param.equalsIgnoreCase("true"))
            promoteId2 = true

        boolean commit = false
        if (commitParam != null && commitParam.equalsIgnoreCase("true"))
            commit = true

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

        String collection = LegacyIntegrationTools.determineLegacyCollection(firstClassDocument, m_jsonld)
        if (!LegacyIntegrationTools.determineLegacyCollection(secondClassDocument, m_jsonld).equals(collection)) {
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
            boolean hasPermission = userInfo.get(whelk.rest.security.AccessControl.KAT_KEY) || Crud.isSystemUser(userInfo)
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
        for (String term : m_forcedSetTerms)
            specialRules.put(term, Graph.PREDICATE_RULES.RULE_AGGREGATE)

        originalGraph.enrichWith(withGraph, specialRules)

        Map enrichedData = JsonldSerializer.serialize(originalGraph.getTriples(), m_forcedSetTerms)
        boolean deleteUnreferencedData = true
        JsonldSerializer.normalize(enrichedData, firstClassDocument.getShortId(), deleteUnreferencedData)
        return new Document(enrichedData)
    }
}
