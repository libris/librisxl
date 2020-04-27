PrintWriter scheduledForUpdating = getReportWriter("scheduled-updates")
PrintWriter failedUpdating = getReportWriter("failed-updates")

String where = "collection = 'bib' and deleted = false AND data#>>'\n" +
        "{@graph,2,illustrativeContent}' LIKE '%code%'"

selectBySqlWhere(where) { data ->
    boolean changed = false

    def (record, instance, work) = data.graph

    if (work.illustrativeContent instanceof List) {
        Iterator it = work.illustrativeContent.iterator()
        while (it.hasNext()) {
            Object entity = it.next()
            if (!entity.containsKey("@id")) {
                it.remove()
                changed = true
            }
        }
    } else if ( (!work.illustrativeContent instanceof Map) || !work.illustrativeContent.containsKey("@id")) {
        work.remove("illustrativeContent")
        changed = true
    }

    if (changed) {
        scheduledForUpdating.println("${data.doc.getURI()}")
        data.scheduleSave(onError: { e ->
            failedUpdating.println("Failed to update ${data.doc.shortId} due to: $e")
        })
    }
}
