String LIBRARY_TO_MOVE = "https://libris.kb.se/library/Okbu"
String MOVE_TO = "https://libris.kb.se/library/Okb"

PrintWriter failedHoldIDs = getReportWriter("failed-holdIDs")
PrintWriter scheduledForUpdating = getReportWriter("scheduled-updates")

String where = "data#>>'{@graph,1,heldBy,@id}' = '${LIBRARY_TO_MOVE}'"

selectBySqlWhere(where, silent: false) { hold ->
    def heldBy = hold.graph[1].heldBy
    heldBy["@id"] = MOVE_TO
    scheduledForUpdating.println("${hold.doc.getURI()}")
    hold.scheduleSave(onError: { e ->
        failedHoldIDs.println("Failed to update ${hold.doc.shortId} due to: $e")
    })
}

