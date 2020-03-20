PrintWriter scheduledForUpdating = getReportWriter("scheduled-updates")

String where = "collection = 'bib' and deleted = false and ( " +
        "data#>'{@graph,2,classification}' @> '[{\"edition\":\"No information provided\"}]' or " +
        "data#>'{@graph,2,classification}' @> '[{\"@type\":\"ClassificationLcc\"}]' or " +
        "data#>'{@graph,2,classification}' @> '[{\"@type\":\"ClassificationNlm\"}]'" +
        ")"

selectBySqlWhere(where) { data ->
    def (record, instance, work) = data.graph

    boolean anythingChanged = work.classification.removeAll {
        it['edition'] == "No information provided" ||
                (it['@type'] == "ClassificationLcc" && it['marc:assignedByLC'] == false) ||
                (it['@type'] == "ClassificationNlm" && it['marc:assignedByNlm'] == false)
    }

    if (anythingChanged) {
        scheduledForUpdating.println("${data.doc.getURI()}")
        data.scheduleSave()
    }
}
