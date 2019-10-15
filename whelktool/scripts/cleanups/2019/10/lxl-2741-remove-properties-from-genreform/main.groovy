/*
See LXL-2741 & LXL-2395 for more info.
*/

PrintWriter failedIDs = getReportWriter("failed-to-delete-bibIDs")
PrintWriter scheduledForChange = getReportWriter("scheduledForChange")

selectBySqlWhere("collection = 'auth' AND data#>>'{@graph,1,@type}' = 'GenreForm'", silent: false) { documentItem ->
    def record = documentItem.doc.data.get('@graph')[0]

    removeFieldFromRecord(record, "descriptionConventions", scheduledForChange)
    removeFieldFromRecord(record, "marc:catalogingSource", scheduledForChange)
    removeFieldFromRecord(record, "marc:kindOfRecord", scheduledForChange)
    removeFieldFromRecord(record, "marc:level", scheduledForChange)
    removeFieldFromRecord(record, "marc:numberedSeries", scheduledForChange)
    removeFieldFromRecord(record, "marc:personalName", scheduledForChange)
    removeFieldFromRecord(record, "marc:reference", scheduledForChange)
    removeFieldFromRecord(record, "marc:romanization", scheduledForChange)
    removeFieldFromRecord(record, "marc:transcribingAgency", scheduledForChange)
    removeFieldFromRecord(record, "marc:typeOfSeries", scheduledForChange)

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