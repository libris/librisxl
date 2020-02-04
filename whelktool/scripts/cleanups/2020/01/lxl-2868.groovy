PrintWriter failedBibIDs = getReportWriter("failed-to-delete-holdIDs")
PrintWriter scheduledForDeletion = getReportWriter("scheduled-for-deletion")

String where =
        "id in (" +
                "select id\n" +
                "from lddb l\n" +
                "where (\n" +
                "data#>'{@graph,0,technicalNote,0,label}' @> '[\"ALMA\"]'::jsonb\n" +
                "or data#>>'{@graph,0,technicalNote,0,label}' = 'ALMA'\n" +
                ")\n" +
                "and collection = 'bib'\n" +
                "and deleted = 'false'\n" +
                "and data#>>'{@graph,0,encodingLevel}' = 'marc:PartialPreliminaryLevel'\n" +
                "and not exists(\n" +
                "select d.dependsonid\n" +
                "from lddb__dependencies d\n" +
                "where l.id = d.dependsonid\n" +
                "and d.relation = 'itemOf'\n" +
                "))"

selectBySqlWhere(where, silent: false, { bib ->
    scheduledForDeletion.println("${bib.doc.getURI()}")
    bib.scheduleDelete(onError: { e ->
        failedBibIDs.println("Failed to delete ${bib.doc.shortId} due to: $e")
    })
})
