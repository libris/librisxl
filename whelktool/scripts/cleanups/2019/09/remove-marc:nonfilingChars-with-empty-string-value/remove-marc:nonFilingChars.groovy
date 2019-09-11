/*
 *
 * Deletes the key-value pair marc:nonfilingChars = "X" when X equals " " (space)
 * for auth, bib and hold records.
 *
 * Number of affected records from STG:
 * select count(id) from lddb where data#>>'{@graph,1,hasTitle,0,marc:nonfilingChars}' = ' ';
 * 1938
 *
 * See LXL-2598 for more info.
 *
 */

PrintWriter failedIDs = getReportWriter("failed-to-delete-Ids")
PrintWriter scheduledForChange = getReportWriter("scheduled-for-change")

where = "data#>>'{@graph,1,hasTitle,0,marc:nonfilingChars}' = ' '"
def absentNonfilingChars = ["marc:nonfilingChars": " "]

selectBySqlWhere(where, silent: false) { docItem ->
    Map hasTitle = docItem.doc.data["@graph"][1]["hasTitle"][0]

    hasTitle.remove(absentNonfilingChars)
    scheduledForChange.println "Remove marc:nonfilingChars property for ${docItem.doc.getURI()}"

    docItem.scheduleSave(onError: { e ->
        failedIDs.println("Failed to save ${docItem.doc.getURI()} due to: $e")
    })
}