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
        changed = true
    }

    if (instance["hasType"] != null) {
        if (instance["hasType"]["@type"] == "NotatedMusicInstance") {
            instance.remove("hasType")
            changed = true
        }
    }

    if (changed) {
        scheduledForUpdating.println("${data.doc.getURI()}")
        data.scheduleSave(onError: { e ->
            failedUpdating.println("Failed to update ${data.doc.shortId} due to: $e")
        })
    }
}
