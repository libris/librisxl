
/*
 * This deletes holdings for sigel BOKR
 *
 * See LXL-2380 for more info.
 *
 */
String SIGEL_TO_DELETE = 'https://libris.kb.se/library/BOKR'

PrintWriter failedHoldIDs = getReportWriter("failed-to-delete-holdIDs")
PrintWriter scheduledForDeletion = getReportWriter("scheduled-for-deletion")

selectBySqlWhere("""
        collection = 'hold' AND
        deleted = 'false' AND
        data#>>'{@graph,1,heldBy,@id}' = '${SIGEL_TO_DELETE}' AND
        modified < '2019-04-16'
    """, silent: false) { hold ->
    scheduledForDeletion.println("${hold.doc.getURI()}")
    hold.scheduleDelete(onError: { e ->
        failedHoldIDs.println("Failed to delete ${hold.doc.shortId} due to: $e")
    })
}