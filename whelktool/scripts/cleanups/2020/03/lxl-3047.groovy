PrintWriter scheduledForUpdating = getReportWriter("scheduled-updates")

String where = "collection = 'bib' and data#>>'{@graph,2}' is not null"
selectBySqlWhere(where) { data ->

    def (record, instance, work) = data.graph

    if (work.note != null) {
        if (work.hasNote == null) {
            work["hasNote"] = []
        }
        work.hasNote.add(work.note)
        work.remove("note")
        scheduledForUpdating.println("${data.doc.getURI()}")
        data.scheduleSave()
    }
}