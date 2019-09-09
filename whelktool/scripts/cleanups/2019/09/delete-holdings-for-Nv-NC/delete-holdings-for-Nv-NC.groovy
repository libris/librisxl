/*
 * This deletes holdings for sigel Nv with shelfMark starting with "NC "
 *
 * See LXL-2344 for more info.
 *
 */

PrintWriter failedHoldIDs = getReportWriter("failed-to-delete-holdIDs")
PrintWriter scheduledForDeletion = getReportWriter("scheduled-for-deletion")
PrintWriter scheduledForUpdate = getReportWriter("scheduled-for-update")

where = """
collection = 'hold'
and data #>>'{@graph,1,heldBy,@id}' = 'https://libris.kb.se/library/Nv'
"""

selectBySqlWhere(where, silent: false) { hold ->
    def delete = {
        scheduledForDeletion.println("${hold.doc.getURI()}")
        hold.scheduleDelete(onError: { e ->
            failedHoldIDs.println("Failed to delete ${hold.doc.shortId} due to: $e")
        })
    }

    item = hold.doc.data['@graph'][1]
    if(isNcShelf(item)) {
        delete()
    }
    else if (item['hasComponent']) {
        List components = item['hasComponent']
        if (components.removeAll(this.&isNcShelf)) {
            if (components.isEmpty()) {
                delete()
            } else {
                scheduledForUpdate.println("${hold.doc.getURI()}")
                hold.scheduleSave()
            }
        }
    }
}

boolean isNcShelf(holdItem) {
    (holdItem['shelfMark'] && holdItem['shelfMark']['label'].toString().startsWith('NC')) ||
    (holdItem['physicalLocation']
            && holdItem['physicalLocation'].size() == 1
            && holdItem['physicalLocation'][0].startsWith('NC')
    )

}