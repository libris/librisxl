PrintWriter failedHoldIDs = getReportWriter("failed-to-delete-holdIDs")
PrintWriter scheduledForDeletion = getReportWriter("scheduled-for-deletion")
String query = "data#>>'{@graph,1,heldBy,@id}' = 'https://libris.kb.se/library/Sweb')"

selectBySqlWhere(query, silent: false) { hold ->
    if (hold.doc.sigel == "Sweb") {
        scheduledForDeletion.println("${hold.doc.getURI()}")
        hold.scheduleDelete(onError: { e ->
            failedHoldIDs.println("Failed to delete ${hold.doc.shortId} due to: $e")
        })
    }
}
