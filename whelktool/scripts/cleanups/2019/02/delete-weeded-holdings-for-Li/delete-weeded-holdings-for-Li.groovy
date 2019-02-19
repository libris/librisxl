/*
 * This deletes all holdings for Li with availability 'UTGALLRAD'.
 *
 * See LXL-2275 for more info.
 *
 */
PrintWriter failedHoldIDs = getReportWriter("failed-to-delete-holdIDs")
PrintWriter scheduledForDeletion = getReportWriter("scheduled-for-deletion")

selectBySqlWhere("""
        collection = 'hold' AND
        data#>>'{@graph,1,heldBy,@id}' = 'https://libris.kb.se/library/Li' AND
        data#>>'{@graph,1,availability,0,@type}' = 'ItemAvailability' AND
        data#>>'{@graph,1,availability,0,label}' = 'UTGALLRAD'
    """, silent: false) { hold ->
    scheduledForDeletion.println("${hold.doc.getURI()}")
    hold.scheduleDelete(onError: { e ->
        failedHoldIDs.println("Failed to delete ${hold.doc.shortId} due to: $e")
    })
}
