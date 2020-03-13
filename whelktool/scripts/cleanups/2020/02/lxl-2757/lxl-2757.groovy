PrintWriter failedHoldIDs = getReportWriter("failed-to-delete-holdIDs")
PrintWriter scheduledForDeletion = getReportWriter("scheduled-for-deletion")
String BIB_ID_FILE = 'bibIDlist'
File bibids = new File(scriptDir, BIB_ID_FILE)

String bibidstring = bibids.readLines().collect{
    "lb.data#>'{@graph,1,identifiedBy}' @> '[{\"@type\":\"ISBN\", \"value\":\"$it\"}]'"
}.join(" \nor\n ")

String where =
        "id in ( " +
                "select lh.id from " +
                "lddb lb " +
                "join " +
                "lddb lh on lh.data#>>'{@graph,1,itemOf,@id}' = lb.data#>>'{@graph,1,@id}' " +
                "where ($bibidstring) " +
                "and " +
                "lh.data#>>'{@graph,1,heldBy,@id}' = 'https://libris.kb.se/library/Mde'" +
        ")"

selectBySqlWhere(where, silent: false, { hold ->
    scheduledForDeletion.println("${hold.doc.getURI()}")
    hold.scheduleDelete(onError: { e ->
        failedHoldIDs.println("Failed to delete ${hold.doc.shortId} due to: $e")
    })
})
