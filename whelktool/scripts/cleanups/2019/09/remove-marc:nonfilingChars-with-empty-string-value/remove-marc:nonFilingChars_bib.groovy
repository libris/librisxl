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

private void findAndRemoveNonfilingChars(docItem, object) {
        if (object instanceof Map) {
            object.each { key, value ->
                if (value instanceof String) {
                    checkStringAndfixValue(docItem, object, key, value)
                } else {
                    findAndRemoveNonfilingChars(docItem, value)
                }
            }
        }
        if (object instanceof List) {
            object.each { element ->
                findAndRemoveNonfilingChars(docItem, element)
            }
        }
}

private void checkStringAndfixValue(docItem, object, key, value) {
    if (key == "marc:nonfilingChars" && value == " ") {
        object.remove(key)
        scheduledForChange.println "Remove marc:nonfilingChars property for ${docItem.doc.getURI()}"
        docItem.scheduleSave(onError: { e ->
            failedIDs.println("Failed to save ${docItem.doc.getURI()} due to: $e")
        })
    }
}