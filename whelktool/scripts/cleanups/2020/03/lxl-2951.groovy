PrintWriter failedHoldIDs = getReportWriter("failed-to-delete-holdIDs")
PrintWriter scheduledForDeletion = getReportWriter("scheduled-for-deletion")

String where = "data#>>'{@graph,1,heldBy,@id}' = 'https://libris.kb.se/library/Ff'"

selectBySqlWhere(where, silent: true) { hold ->

    scheduledForDeletion.println("${hold.doc.getURI()}")
    hold.scheduleDelete(onError: { e ->
        failedHoldIDs.println("Failed to delete ${hold.doc.shortId} due to: $e")
    })
}
