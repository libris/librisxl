/*
See LXL-2639 for more info.
*/

PrintWriter failedIDs = getReportWriter("failed-to-delete-bibIDs")
PrintWriter scheduledForChange = getReportWriter("scheduledForChange")

selectBySqlWhere("collection = 'auth'", silent: false) { documentItem ->
    def record = documentItem.doc.data.get('@graph')[0]

    removeFieldFromRecord(record, "marc:subdivision", scheduledForChange)
    removeFieldFromRecord(record, "marc:languageOfCatalog", scheduledForChange)
    removeFieldFromRecord(record, "marc:subjectHeading", scheduledForChange)
    removeFieldFromRecord(record, "marc:headingMain", scheduledForChange)
    removeFieldFromRecord(record, "marc:headingSubject", scheduledForChange)
    removeFieldFromRecord(record, "marc:subjectSubdivision", scheduledForChange)
    removeFieldFromRecord(record, "marc:recordUpdate", scheduledForChange)
    removeFieldFromRecord(record, "marc:modifiedRecord", scheduledForChange)

    documentItem.scheduleSave(onError: { e ->
        failedIDs.println("Failed to save ${record[ID]} due to: $e")
    })
}

private void removeFieldFromRecord(record, String fieldName, scheduledForChange) {
    if (record.remove(fieldName) == null) {
        scheduledForChange.println "Field $fieldName not present for ${record[ID]}"
    } else {
        scheduledForChange.println "Remove field $fieldName from ${record[ID]}"
    }
}