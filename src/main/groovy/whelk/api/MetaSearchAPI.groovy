package whelk.api

import javax.servlet.http.*

import whelk.*

class MetaSearchAPI extends BasicAPI {

    String description = "Searches document metadata."

    void doHandle(HttpServletRequest request, HttpServletResponse response, List pathVars) {
        def jsonResult = this.whelk.index.query(new LinkedDataAPIQuery(request.parameterMap), whelk.storage.indexName, [whelk.storage.ELASTIC_STORAGE_TYPE] as String[]).toJson()

        sendResponse(response, jsonResult, "application/json")
    }
}
