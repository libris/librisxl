PrintWriter failedBibIDs = getReportWriter("failed-to-delete-bibIDs")
PrintWriter scheduledForUpdate = getReportWriter("scheduled-for-update")

String where = "collection = 'bib' and data#>>'{@graph,2}' is not null"

selectBySqlWhere(where, silent: true) { bib ->

    def (record, mainEntity, oldWork) = bib.graph

    mainEntity.instanceOf = oldWork
    mainEntity.instanceOf.remove("@id")
    bib.graph.remove(2)

    scheduledForUpdate.println("${bib.doc.getURI()}")
    bib.scheduleSave(onError: { e ->
        failedBibIDs.println("Failed to udpate ${bib.doc.shortId} due to: $e")
    })

}
