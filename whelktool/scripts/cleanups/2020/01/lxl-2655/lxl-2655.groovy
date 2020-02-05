PrintWriter failedHoldIDs = getReportWriter("failed-holdIDs")
PrintWriter unexpectedRecordState = getReportWriter("unexpeted-record-states")
PrintWriter scheduledForUpdating = getReportWriter("scheduled-updates")

File bibids = new File(scriptDir, "informalProgram.tsv")
List<String> ProgramLines = bibids.readLines()

for (String operation : ProgramLines) {
    // Operation format is Libris-ID,oldShelfMark,NewShelfMark
    // Examples of operations:
    // 11891132	Jon	SKON	Anthologies BEST
    // 918541283X	Jon	SKON	Anthologies BIG

    String[] part = operation.split("\\t")
    String fuzzyID = part[0]
    String sigel = part[1] // "Jon" on all lines, ignore this value
    String physicalLocation = part[2].trim()
    String fuzzyShelfSomething = part[3]

    String[] shelfParts = fuzzyShelfSomething.split(" ")
    String newShelfMark = shelfParts[shelfParts.length-1].trim() // The last part, separated by a space
    String newShelfLabel = fuzzyShelfSomething.substring(0, fuzzyShelfSomething.length() - newShelfMark.length()).trim() // All but the last part

    String iri
    if (fuzzyID.matches("[0-9]{1,13}")) // Voyager ID
        iri = "http://libris.kb.se/bib/$fuzzyID"
    else // XL ID
        iri = baseUri.resolve(fuzzyID).toString()

    String where = "id in (\n" +
            "select l.id\n" +
            "from lddb__identifiers i\n" +
            "left join lddb__dependencies d on i.id = d.dependsonid\n" +
            "left join lddb l on d.id = l.id\n" +

            // If this is run again in the future, the change which makes library-URIs env-specific will have gone through. At the
            // time of writing, this is only in effect on dev, and so the hardcoded URI must be used.
            //"where i.iri = '$iri' and d.relation = 'itemOf' and l.data#>>'{@graph,1,heldBy,@id}' = '${baseUri.resolve("library/Jon")}'\n" +
            "where i.iri = '$iri' and d.relation = 'itemOf' and l.data#>>'{@graph,1,heldBy,@id}' = 'https://libris.kb.se/library/Jon'\n" +

            ")"

    if (!iri || !physicalLocation || !newShelfMark || !newShelfLabel) { // null or empty-string
        System.err.println("Malformed operation: " + operation)
        continue
    }

    selectBySqlWhere(where, silent: true) { hold ->

        // Update shelfMark
        List components = hold.graph[1].hasComponent.findAll {
            it["@type"] == "Item" &&

                    // Same as above, library URIs may be env-specific or not, depending on when/where this is run
                    // it.heldBy["@id"] == baseUri.resolve("library/Ors")}
                    it.heldBy["@id"] == "https://libris.kb.se/library/Jon"
        }

        components.add(hold.graph[1]) // Shelf mark may also be on the outer Item (instead of component list)
        for (Map component : components) {
            if (component.shelfMark) {
                def labels = component.shelfMark.label
                if (labels instanceof List) {
                    labels.clear()
                    labels.add(newShelfMark)
                } else {
                    component.shelfMark.label = newShelfMark
                }
            }
        }

        hold.graph[1].shelfLabel = newShelfLabel

        scheduledForUpdating.println("${hold.doc.getURI()}")
        hold.scheduleSave(loud: true, onError: { e ->
            failedHoldIDs.println("Failed to update ${hold.doc.shortId} due to: $e")
        })
    }

}
