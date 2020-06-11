/*
 *
 * Deletes the key-value pair marc:nonfilingChars = "X" when X equals " " (space)
 * for auth records.
 *
 * See LXL-2598 for more info.
 *
 */

failedIDs = getReportWriter("failed-to-delete-Ids")
scheduledForChange = getReportWriter("scheduled-for-change")

where = "collection = 'auth' AND data#>>'{@graph,1,hasTitle,0,marc:nonfilingChars}' = ' '"

selectBySqlWhere(where, silent: false) { docItem ->
    scheduledForChange.println "Remove marc:nonfilingChars property for ${docItem.doc.getURI()}"
    Map hasTitle = docItem.doc.data["@graph"][1]["hasTitle"][0]

    hasTitle.remove("marc:nonfilingChars")

    docItem.scheduleSave(onError: { e ->
        failedIDs.println("Failed to save ${docItem.doc.getURI()} due to: $e")
    })
}