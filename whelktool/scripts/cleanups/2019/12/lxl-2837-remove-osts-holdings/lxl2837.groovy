PrintWriter failedHoldIDs = getReportWriter("failed-to-delete-holdIDs")
PrintWriter scheduledForDeletion = getReportWriter("scheduled-for-deletion")
String BIB_ID_FILE = 'bibIDlist'
File bibids = new File(scriptDir, BIB_ID_FILE)

String bibidstring = bibids.readLines().join("','")

String where =
        "id in " +
        "( " +
        "select l.id from " +
        "lddb__identifiers i " +
        "left join lddb__dependencies d on d.dependsonid = i.id and d.relation = 'itemOf' " +
        "left join lddb l on d.id = l.id " +
        "where " +
        "i.iri in ('${bibidstring}') and l.data#>>'{@graph,1,heldBy,@id}' = 'https://libris.kb.se/library/Osts' " +
        ")"

selectBySqlWhere(where, silent: false, { hold ->
    scheduledForDeletion.println("${hold.doc.getURI()}")
    hold.scheduleDelete(onError: { e ->
        failedHoldIDs.println("Failed to delete ${hold.doc.shortId} due to: $e")
    })
})
