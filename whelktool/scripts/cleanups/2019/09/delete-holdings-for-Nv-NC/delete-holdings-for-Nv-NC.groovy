/*
 * This deletes holdings for sigel Nv with shelfMark starting with "NC "
 *
 * See LXL-2344 for more info.
 *
 */

PrintWriter failedHoldIDs = getReportWriter("failed-to-delete-holdIDs")
PrintWriter scheduledForDeletion = getReportWriter("scheduled-for-deletion")

where = """
collection = 'hold'
and data #>>'{@graph,1,heldBy,@id}' = 'https://libris.kb.se/library/Nv'
and data #>>'{@graph,1,shelfMark,label}' like 'NC %'
"""

selectBySqlWhere(where, silent: false) { hold ->
    scheduledForDeletion.println("${hold.doc.getURI()}")
    hold.scheduleDelete(onError: { e ->
        failedHoldIDs.println("Failed to delete ${hold.doc.shortId} due to: $e")
    })
}