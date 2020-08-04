PrintWriter failedHoldIDs = getReportWriter("failed-to-update-holdIDs")
PrintWriter scheduledForUpdate = getReportWriter("scheduled-for-update")

selectBySqlWhere(" collection = 'hold' and data#>>'{@graph,1,heldBy,@id}' = 'https://libris.kb.se/library/£FROMSIGEL'", silent: false, { hold ->
    scheduledForUpdate.println("${hold.doc.getURI()}")
    def heldBy = hold.graph[1].heldBy
    heldBy["@id"] = 'https://libris.kb.se/library/£TOSIGEL'
    hold.scheduleSave(loud: true, onError: { e ->
        failedHoldIDs.println("Failed to update ${hold.doc.shortId} due to: $e")
    })
})
