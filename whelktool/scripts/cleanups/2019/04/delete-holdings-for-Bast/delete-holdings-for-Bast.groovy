/*
 * This deletes holdings for sigel Bast
 *
 * See LXL-2409 for more info.
 *
 */
String SIGEL_TO_DELETE = 'Bast'

PrintWriter failedHoldIDs = getReportWriter("failed-to-delete-holdIDs")
PrintWriter scheduledForDeletion = getReportWriter("scheduled-for-deletion")

selectBySqlWhere("""
        collection = 'hold' AND
        data#>>'{@graph,1,heldBy,@id}' = '${SIGEL_TO_DELETE}'
    """, silent: false) { hold ->
    scheduledForDeletion.println("${hold.doc.getURI()}")
    hold.scheduleDelete(onError: { e ->
        failedHoldIDs.println("Failed to delete ${hold.doc.shortId} due to: $e")
    })
}