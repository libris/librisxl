PrintWriter failedHoldIDs = getReportWriter("failed-holdIDs")
PrintWriter unexpectedRecordState = getReportWriter("unexpeted-record-states")
PrintWriter scheduledForUpdating = getReportWriter("scheduled-updates")

File bibids = new File(scriptDir, "informalProgram.csv")
List<String> ProgramLines = bibids.readLines()

for (String operation : ProgramLines) {
    // Operation format is Libris-ID,oldShelfMark,NewShelfMark
    // Examples of operations:
    // 13605827,WE 103,152
    // 4dc46ml42t5k2fc3,WM 420,616.891

    String[] part = operation.split(",")
    String LibrisID = part[0]
    String oldShelfMark = part[1]
    String newShelfMark = part[2]

    String iri
    if (LibrisID.matches("[0-9]{1,13}")) // Voyager ID
        iri = "http://libris.kb.se/bib/$LibrisID"
    else // XL ID
        iri = baseUri.resolve(LibrisID).toString()

    String where = "id in (\n" +
            "select l.id\n" +
            "from lddb__identifiers i\n" +
            "left join lddb__dependencies d on i.id = d.dependsonid\n" +
            "left join lddb l on d.id = l.id\n" +

            // If this is run again in the future, the change which makes library-URIs env-specific will have gone through. At the
            // time of writing, this is only in effect on dev, and so the hardcoded URI must be used.
            //"where i.iri = '$iri' and d.relation = 'itemOf' and l.data#>>'{@graph,1,heldBy,@id}' = '${baseUri.resolve("library/Ors")}'\n" +
            "where i.iri = '$iri' and d.relation = 'itemOf' and l.data#>>'{@graph,1,heldBy,@id}' = 'https://libris.kb.se/library/Ors'\n" +

            ")"

    selectBySqlWhere(where, silent: true) { hold ->

        List components = hold.graph[1].hasComponent.findAll { it["@type"] == "Item" &&

                // Same as above, library URIs may be env-specific or not, depending on when/where this is run
                // it.heldBy["@id"] == baseUri.resolve("library/Ors")}
                it.heldBy["@id"] == "https://libris.kb.se/library/Ors"}

        components.add(hold.graph[1]) // Shelf mark may also be on the outer Item (instead of component list)
        for (Map component : components) {
            if (component.shelfMark) {
                def labels = component.shelfMark.label
                if (labels instanceof List) {
                    if (labels.size != 1 || !labels[0].equals(oldShelfMark))
                        unexpectedRecordState.println("$iri had shelfMark: ${labels}, expected $oldShelfMark")
                    labels.clear()
                    labels.add(newShelfMark)
                } else {
                    if (labels instanceof String && !labels.equals(oldShelfMark))
                        unexpectedRecordState.println("$iri had shelfMark: ${labels}, expected $oldShelfMark")
                    component.shelfMark.label = newShelfMark
                }
            }
        }

        scheduledForUpdating.println("${hold.doc.getURI()}")
        hold.scheduleSave(loud: true, onError: { e ->
            failedHoldIDs.println("Failed to update ${hold.doc.shortId} due to: $e")
        })
    }
}
