PrintWriter failedHoldIDs = getReportWriter("failed-holdIDs")
PrintWriter unexpectedRecordState = getReportWriter("unexpeted-record-states")
PrintWriter scheduledForUpdating = getReportWriter("scheduled-updates")

File bibids = new File(scriptDir, "informalProgram.tsv")
List<String> ProgramLines = bibids.readLines()

// If this is run again in the future, the change which makes library-URIs env-specific will have gone through. At the
// time of writing, this is only in effect on dev, and so the hardcoded URI must be used.
String libraryURI = "https://libris.kb.se/library/Jon"
//String libraryURI = baseUri.resolve("library/Jon")

for (String operation : ProgramLines) {
    // Operation format is Libris-ID,oldShelfMark,NewShelfMark
    // Examples of operations:
    // 11891132	Jon	SKON	Anthologies BEST
    // 918541283X	Jon	SKON	Anthologies BIG

    String[] part = operation.split("\\t")
    String fuzzyID = part[0]
    String sigel = part[1] // "Jon" on all lines, ignore this value
    String newPhysicalLocation = part[2].trim()
    String fuzzyShelfSomething = part[3]

    String[] shelfParts = fuzzyShelfSomething.split(" ")
    String newShelfMark = shelfParts[shelfParts.length-1].trim() // The last part, separated by a space
    String newShelfLabel = fuzzyShelfSomething.substring(0, fuzzyShelfSomething.length() - newShelfMark.length()).trim() // All but the last part

    String where = null

    // Looks like an ISBN
    if (fuzzyID.matches("[0-9X]{9,14}")) {
        where = "id in (\n" +
                "select lh.id from lddb lb\n" +
                "join lddb lh on lb.data#>>'{@graph,1,@id}' = lh.data#>>'{@graph,1,itemOf,@id}'\n" +
                "where lb.data#>'{@graph,1,identifiedBy}' @> '[{\"@type\":\"ISBN\", \"value\":\"$fuzzyID\"}]'\n" +
                "and lh.data#>>'{@graph,1,heldBy,@id}' = '$libraryURI'\n" +
                ")"
    }
    else {
        String iri
        if (fuzzyID.matches("[0-9]{1,8}")) // Voyager ID, of which the HIGHEST ever assigned was, 22710053 (8 digits)
            iri = "http://libris.kb.se/bib/$fuzzyID"
        else // XL ID
            iri = baseUri.resolve(fuzzyID).toString()

        where = "id in (\n" +
                "select l.id\n" +
                "from lddb__identifiers i\n" +
                "left join lddb__dependencies d on i.id = d.dependsonid\n" +
                "left join lddb l on d.id = l.id\n" +
                "where i.iri = '$iri' and d.relation = 'itemOf' and l.data#>>'{@graph,1,heldBy,@id}' = '$libraryURI'\n" +
                ")"
    }

    if (!newPhysicalLocation || !newShelfMark || !newShelfLabel) { // null or empty-string
        System.err.println("Malformed operation: " + operation)
        continue
    }

    selectBySqlWhere(where, silent: true) { hold ->

        // Update shelfMark
        List components = hold.graph[1].hasComponent.findAll {
            it["@type"] == "Item" &&
                    it.heldBy["@id"] == libraryURI
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
