PrintWriter scheduledForUpdating = getReportWriter("scheduled-updates")

String where = "collection = 'bib' and data#>>'{@graph,2}' is not null"
selectBySqlWhere(where) { data ->

    def (record, instance, work) = data.graph

    if (work.note != null) {
        if (work.hasNote == null) {
            work["hasNote"] = []
        } else if ( ! (work.hasNote instanceof List) ) {
            def tmp = work.hasNote
            work["hasNote"] = [tmp]
        }

        if (work.note instanceof List) {
            for (String note : work.note)
                transformNote(note, work)
        } else {
            transformNote(work.note, work)
        }
        work.remove("note")

        scheduledForUpdating.println("${data.doc.getURI()}")
        data.scheduleSave()
    }
}

void transformNote( String note, Map work ) {
    work.hasNote.add(
            [
                    "@type" : "Note",
                    "label" : note
            ]
    )
}
