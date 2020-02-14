import whelk.Document
import whelk.Whelk
import whelk.datatool.DocumentItem
import whelk.util.DocumentUtil


PrintWriter changeLog = getReportWriter("linking-local-things")


String findMainEntityId(Whelk whelk, String ctrlNumber) {
    String mainId = null
    try {
        mainId = findCanonicalId("${whelk.baseUri.resolve(ctrlNumber)}#it")
    } catch (IllegalArgumentException e) {
    }
    if (mainId) {
        return mainId
    }
    def legacyId = "http://libris.kb.se/resource/bib/${ctrlNumber}"
    mainId = findCanonicalId(legacyId)
    if (mainId) {
        return mainId
    }

    def byLibris3Ids = []
    // IMPORTANT: This REQUIRES an index on '@graph[0]identifiedBy*.value'.
    // If that is removed, this slows to a GLACIAL crawl!
    selectBySqlWhere("""
    data #> '{@graph,0,identifiedBy}' @> '[{"@type": "LibrisIIINumber", "value": "${ctrlNumber}"}]'::jsonb and collection = 'bib'
    """, silent: true) {
        byLibris3Ids << it.doc.id
    }
    if (byLibris3Ids) {
        // TODO: how bad is this?
        //assert byLibris3Ids.size() == 1
    }
    return byLibris3Ids ? "${byLibris3Ids[0]}#it" : null
}

String sanitize(String value) {
    return value.replaceAll(/\9/, '')
}

selectBySqlWhere("""
    data#>>'{@graph,1}' LIKE '%"controlNumber":%'
    OR data#>>'{@graph,2}' LIKE '%"controlNumber":%'
    AND collection = 'bib'
""") { DocumentItem data ->
    def (record, instance, work) = data.graph

    new DocumentUtil().traverse(data.graph[1..-1], { values, path ->
        boolean oneValue = false
        if (values instanceof Map) {
            values = [values]
        }

        String changedTo = null
        for (node in values) {
            if (!(node instanceof Map) || !('describedBy' in node)) {
                continue
            }
            boolean gotExtId = false

            def refRecords = node.describedBy
            if (refRecords instanceof Map) {
                refRecords = [refRecords]
            }

            String mainEntityId = null

            for (refRecord in refRecords) {
                def ctrlNumbers = refRecord.controlNumber
                if (ctrlNumbers instanceof String) {
                    ctrlNumbers = [ctrlNumbers]
                }

                if (!mainEntityId) {
                    mainEntityId = ctrlNumbers.findResult {
                        if (it instanceof String) {
                            findMainEntityId(data.whelk, it)
                        }
                    }
                }

                if (!mainEntityId) {
                    node.remove('describedBy')
                    def identifiedBy = node.get('identifiedBy', [])
                    gotExtId = true
                    identifiedBy.addAll(ctrlNumbers.collect {
                        [(TYPE): 'Local', value: sanitize(it)]
                    })
                    changedTo = 'LOCAL'
                }
            }

            if (mainEntityId) {
                assert !gotExtId, "Both controlNumber and external id used in $data.doc.dataAsString!"
                // TODO: Check if values of refMode "match" linked entity?
                node.clear()
                node[ID] = mainEntityId
                changedTo = 'LINK'
            }
        }

        if (changedTo) {
            changeLog.println("${data.doc.shortId} ${changedTo}")
            data.scheduleSave()
        }
    })
}
