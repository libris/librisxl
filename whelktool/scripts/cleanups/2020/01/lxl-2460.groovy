PrintWriter failedHoldIDs = getReportWriter("failed-holdIDs")
PrintWriter scheduledForUpdating = getReportWriter("scheduled-updates")

String where = "data#>'{@graph,1,hasComponent}' @> '[{\"@type\": \"Item\", \"heldBy\": {\"@id\": \"https://libris.kb.se/library/Jox\"}}]' or data#>>'{@graph,1,heldBy,@id}' = 'https://libris.kb.se/library/Jox'"

selectBySqlWhere(where, silent: false) { hold ->
    def heldBy = hold.graph[1].heldBy

    if (heldBy["@id"] == "https://libris.kb.se/library/Jox")
        heldBy["@id"] = "https://libris.kb.se/library/Jo"

    def components = hold.graph[1]["hasComponent"]
    components.each {
        if (it["heldBy"]["@id"] == "https://libris.kb.se/library/Jox")
            it["heldBy"]["@id"] = "https://libris.kb.se/library/Jo"
    }

    scheduledForUpdating.println("${hold.doc.getURI()}")
    hold.scheduleSave(loud: true, onError: { e ->
        failedHoldIDs.println("Failed to update ${hold.doc.shortId} due to: $e")
    })
}

