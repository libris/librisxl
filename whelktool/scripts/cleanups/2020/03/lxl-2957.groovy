PrintWriter scheduledForUpdating = getReportWriter("scheduled-updates")

String where = "collection = 'bib' and deleted = false and ( " +
        "data#>'{@graph,2,classification}' @> '[{\"edition\":\"No information provided\"}]' or " +
        "data#>'{@graph,2,classification}' @> '[{\"@type\":\"ClassificationLcc\"}]' or " +
        "data#>'{@graph,2,classification}' @> '[{\"@type\":\"ClassificationNlm\"}]'" +
        ")"

selectBySqlWhere(where) { data ->
    def (record, instance, work) = data.graph

    boolean changed = false
    work.classification.each {
        if (it['edition'] == "No information provided") {
            it.remove('edition')
            changed = true
        }

        if (it['@type'] == "ClassificationLcc" && it['marc:assignedByLC'] == false) {
            it.remove('marc:assignedByLC')
            changed = true
        }
        
        if (it['@type'] == "ClassificationNlm" && it['marc:assignedByNlm'] == false) {
            it.remove('marc:assignedByNlm')
            changed = true
        }
    }

    if (changed) {
        scheduledForUpdating.println("${data.doc.getURI()}")
        data.scheduleSave()
    }
}
