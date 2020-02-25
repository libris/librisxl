PrintWriter failedHoldIDs = getReportWriter("failed-holdIDs")
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
    String fuzzyID = part[0].trim()
    String sigel = part[1] // "Jon" on all lines, ignore this value
    String newPhysicalLocation = part[2].trim()
    String fuzzyShelfSomething = part[3].trim()
    
    int breakShelfMessAt = fuzzyShelfSomething.indexOf(" ") // _first_ space
    String newShelfMark = fuzzyShelfSomething.substring(0, breakShelfMessAt).trim() // The first part, separated by space
    String newShelfLabel = fuzzyShelfSomething.substring(breakShelfMessAt, fuzzyShelfSomething.length()).trim() // The rest of the string

    String where = null

    // Looks like an ISBN ... OR A FU**ING LIBRISIIINUMBER, THIS IS INSANE.
    if (fuzzyID.matches("[0-9X]{9,14}")) {
        // Postgres unfortunately fails to optimize the non-CTE version of this. The same thing happens if you attempt
        // to join in the holdings to filter out non-Jon-ones.
        where = "id in (\n" +
                "with bibIds as\n" +
                "(\n" +
                "select id from lddb where\n" +
                "data#>'{@graph,1,identifiedBy}' @> '[{\"value\":\"$fuzzyID\"}]'\n" +
                "or\n" +
                "data#>'{@graph,0,identifiedBy}' @> '[{\"value\":\"$fuzzyID\"}]'\n" +
                ")\n" +
                "select d.id from lddb__dependencies d\n" +
                "where d.dependsonid in (select * from bibIds)\n" +
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

    boolean foundSomeThing = false
    selectBySqlWhere(where, silent: true) { hold ->
        if (hold.doc.getSigel() == "Jon") {

            foundSomeThing = true

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

                    component.shelfLabel = newShelfLabel
                }
            }

            scheduledForUpdating.println("${hold.doc.getURI()}")
            hold.scheduleSave(loud: true, onError: { e ->
                failedHoldIDs.println("Failed to update ${hold.doc.shortId} due to: $e")
            })
        }
    }

    if (!foundSomeThing)
        System.err.println("Could not find a record matching the \"id\" in: " + operation)

}
