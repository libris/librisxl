/*
 *
 * Deletes the key-value pair marc:nonfilingChars = "X" when X equals " " (space)
 * for bib records.
 *
 * See LXL-2598 for more info.
 *
 */

failedIDs = getReportWriter("failed-to-delete-Ids")
scheduledForChange = getReportWriter("scheduled-for-change")

selectBySqlWhere("collection = 'bib'", silent: false) { docItem ->
    docItem.doc.data["@graph"][1..-1].each() {
        findAndRemoveNonfilingChars(docItem, it)
    }
}

void findAndRemoveNonfilingChars(docItem, object) {
    if (object instanceof Map) {
        //For each map, find and remove before iterating to avoid concurrentModficationException
        def somethingWasRemoved = object.entrySet().removeIf { it.getKey() == "marc:nonfilingChars" && it.getValue() == " "}
        if (somethingWasRemoved) {
            scheduledForChange.println "Remove marc:nonfilingChars property for ${docItem.doc.getURI()}"
            docItem.scheduleSave(onError: { e ->
                failedIDs.println("Failed to save ${docItem.doc.getURI()} due to: $e")
            })
        }
        object.each { entry ->
            if (entry.value instanceof Map || entry.value instanceof List) {
                findAndRemoveNonfilingChars(docItem, entry.value)
            }
        }
    }
    if (object instanceof List) {
        object.each { element ->
            findAndRemoveNonfilingChars(docItem, element)
        }
    }
}