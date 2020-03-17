PrintWriter failedBibIDs = getReportWriter("failed-to-delete-bibIDs")
PrintWriter scheduledForDeletion = getReportWriter("scheduled-for-deletion")

String where =
        "created > '2020-03-13 10:00:00' and\n" +
                "created < '2020-03-13 16:00:00' and\n" +
                "data#>>'{@graph,0,recordStatus}' = 'marc:New' and\n" +
                "data#>>'{@graph,0,encodingLevel}' = 'marc:PartialPreliminaryLevel' and\n" +
                "data#>>'{@graph,1,hasTitle}' is null"

selectBySqlWhere(where, silent: false, { bib ->
    scheduledForDeletion.println("${bib.doc.getURI()}")
    bib.scheduleDelete(onError: { e ->
        failedBibIDs.println("Failed to delete ${bib.doc.shortId} due to: $e")
    })
})
