PrintWriter failedBibIDs = getReportWriter("failed-to-update-bibIDs")
PrintWriter scheduledForUpdate = getReportWriter("scheduled-for-update")

String where = "collection = 'bib' and data#>>'{@graph,2}' is null"

selectBySqlWhere(where, silent: true) { bib ->

    if (bib.graph.size() != 2) { // record and thing
        failedBibIDs.println("Failed to udpate ${bib.doc.shortId} due to: Did not have exactly 2 elements in @graph list")
        return
    }

    def (record, mainEntity) = bib.graph

    String recordPrimaryUri = record["@id"]
    String workUri = recordPrimaryUri + "#work"

    bib.graph.add(mainEntity.instanceOf)
    if (bib.graph.size() != 3) { // record, thing and work
        failedBibIDs.println("Failed to udpate ${bib.doc.shortId} due to: Did not have exactly 3 elements in @graph list after adding work")
        return
    }

    bib.graph[2]["@id"] = workUri
    mainEntity.instanceOf = [ "@id" : workUri ]

    scheduledForUpdate.println("${bib.doc.getURI()}")
    bib.scheduleSave(onError: { e ->
        failedBibIDs.println("Failed to udpate ${bib.doc.shortId} due to: $e")
    })

}
