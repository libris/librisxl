PrintWriter scheduledForUpdating = getReportWriter("scheduled-updates")
PrintWriter failedUpdating = getReportWriter("failed-updates")

String where = "collection = 'bib' and data#>'{@graph,2,hasPart}' is not null"

selectBySqlWhere(where) { data ->
    def (record, mainEntity, work) = data.graph

    boolean changed = false

    Object parts = work.hasPart
    if ( parts == null || ! (parts instanceof List) )
        return

    Iterator it = parts.iterator()
    while (it.hasNext()) {
        Map part = (Map) it.next()

        if (part.get("@type") != null && part.size() == 1) {
            changed = true
            it.remove()
        }
    }

    if (parts.isEmpty())
        work.remove("hasPart")

    if (changed) {
        scheduledForUpdating.println("${data.doc.getURI()}")
        data.scheduleSave(onError: { e ->
            failedUpdating.println("Failed to update ${data.doc.shortId} due to: $e")
        })
    }
}
