PrintWriter scheduledForUpdating = getReportWriter("scheduled-updates")
PrintWriter failedUpdating = getReportWriter("failed-updates")

String where = "data#>>'{@graph,1,@type}' = 'NotatedMusicInstance' and data#>>'{@graph,2,@type}' = 'NotatedMusic'"

selectBySqlWhere(where) { data ->
    def (record, instance, work) = data.graph
    boolean changed = false

    if (instance["marc:mediaTerm"] && instance["marc:mediaTerm"] instanceof String) {

        if (instance["marc:mediaTerm"].contains("tryck") ||
                instance["marc:mediaTerm"].contains("druck") ||
                instance["marc:mediaTerm"].contains("Noten") ||
                instance["marc:mediaTerm"].contains("tryk") ) {
            instance["@type"] = "Print"
            changed = true
        }

        if (instance["marc:mediaTerm"].contains("Elektronisk")) {
            instance["@type"] = "Electronic"
            changed = true
        }

        if (instance["marc:mediaTerm"].contains("handskrift")) {
            instance["@type"] = "Manuscript"
            changed = true
        }
    }

    if (!changed) {
        instance["@type"] = "Instance"
    }

    scheduledForUpdating.println("${data.doc.getURI()}")
    data.scheduleSave(onError: { e ->
        failedUpdating.println("Failed to update ${data.doc.shortId} due to: $e")
    })
}

where = "data#>'{@graph,1,hasPart}' @> '[{\"@type\":\"NotatedMusicInstance\"}]'"

selectBySqlWhere(where) { data ->
    def (record, instance) = data.graph
    boolean changed = false

    if (instance["hasPart"] != null) {
        Iterator it = instance["hasPart"].iterator()
        while (it.hasNext()) {
            Map part = (Map) it.next()
            if (part["@type"] == "NotatedMusicInstance") {
                it.remove()
            }
        }
    }

    scheduledForUpdating.println("${data.doc.getURI()}")
    data.scheduleSave(onError: { e ->
        failedUpdating.println("Failed to update ${data.doc.shortId} due to: $e")
    })
}