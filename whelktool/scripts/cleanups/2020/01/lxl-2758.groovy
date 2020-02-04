PrintWriter failedHoldIDs = getReportWriter("failed-to-delete-holdIDs")
PrintWriter scheduledForDeletion = getReportWriter("scheduled-for-deletion")

String where =
                "collection = 'hold'\n" +
                "and\n" +
                "data#>>'{@graph,1,cataloguersNote}' like '%rigin:Elib%'\n" +
                "and\n" +
                "data#>>'{@graph,1,cataloguersNote}' like '%eleted%'\n" +
                "and\n" +
                "(\n" +
                "data#>>'{@graph,1,shelfMark}' like '%j tillgänglig%'\n" +
                "or\n" +
                "data#>>'{@graph,1,hasComponent}' like '%j tillgänglig%'\n" +
                ")"

selectBySqlWhere(where, silent: false, { hold ->
    scheduledForDeletion.println("${hold.doc.getURI()}")
    hold.scheduleDelete(onError: { e ->
        failedHoldIDs.println("Failed to delete ${hold.doc.shortId} due to: $e")
    })
})
