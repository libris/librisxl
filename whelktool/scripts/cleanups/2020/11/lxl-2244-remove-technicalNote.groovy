PrintWriter scheduledForUpdating = getReportWriter("scheduled-updates")
PrintWriter failedUpdating = getReportWriter("failed-updates")

String where = """
        collection = 'bib'
        and deleted = false
        and data#>>'{@graph,0,technicalNote}' like '%Maskinellt genererad post%'
        and (data#>>'{@graph,0,encodingLevel}' = 'marc:FullLevel'
        or data#>>'{@graph,0,encodingLevel}' = 'marc:MinimalLevel'
        or data#>>'{@graph,0,encodingLevel}' = 'marc:AbbreviatedLevel')
        """


selectBySqlWhere(where) { data ->
    List technicalNotes = data.graph[0].technicalNote

    // Remove element from {@graph,0,technicalNote} if substring 'Maskinellt genererad post' is found in its label
    technicalNotes.removeAll { tNote ->
        // The label is either a string or a list of strings
        tNote.label instanceof String && tNote.label.contains('Maskinellt genererad post') ||
                tNote.label instanceof List && tNote.label.any { it.contains('Maskinellt genererad post') }
    }

    // Remove 'technicalNote' from {@graph,0} if it links to nothing
    if (technicalNotes.isEmpty()) {
        data.graph[0].remove('technicalNote')
    }

    scheduledForUpdating.println("${data.doc.getURI()}")
    data.scheduleSave(onError: { e ->
        failedUpdating.println("Failed to update ${data.doc.shortId} due to: $e")
    })
}
