PrintWriter scheduledForUpdating = getReportWriter("scheduled-updates")
PrintWriter failedUpdating = getReportWriter("failed-updates")

String where = """
    collection = 'auth' and data#>'{@graph,1,hasVariant}' @> '[{"@type": "ComplexSubject", "termComponentList": [{"@type":"Temporal"}]}]'
"""

selectBySqlWhere(where) { data ->

    boolean changed = false

    data.graph[1].hasVariant.each { subj ->
        if (subj["@type"] == "ComplexSubject") {
            for (int i = 1; i < subj.termComponentList.size(); ++i) {
                def termComponent = subj.termComponentList[i]
                if (termComponent instanceof Map) {
                    if ( termComponent["@type"] == "Temporal" ) {
                        termComponent["@type"] = "TemporalSubdivision"
                        changed = true
                    }
                }
            }

        }
    }

    if (changed) {
        scheduledForUpdating.println("${data.doc.getURI()}")
        data.scheduleSave(onError: { e ->
            failedUpdating.println("Failed to update ${data.doc.shortId} due to: $e")
        })
    }
}
