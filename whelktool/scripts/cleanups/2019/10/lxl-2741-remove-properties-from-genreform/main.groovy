/*
See LXL-2741 & LXL-2395 for more info.
*/

PrintWriter failedIDs = getReportWriter("failed-to-delete-bibIDs")
scheduledForChange = getReportWriter("scheduledForChange")

selectBySqlWhere("collection = 'auth' AND data#>>'{@graph,1,@type}' = 'GenreForm'", silent: false) { documentItem ->
    def record = documentItem.doc.data.get('@graph')[0]

    removeFieldFromRecord(record, "descriptionConventions")
    removeFieldFromRecord(record, "marc:catalogingSource")
    removeFieldFromRecord(record, "marc:kindOfRecord")
    removeFieldFromRecord(record, "marc:level")
    removeFieldFromRecord(record, "marc:numberedSeries")
    removeFieldFromRecord(record, "marc:personalName")
    removeFieldFromRecord(record, "marc:reference")
    removeFieldFromRecord(record, "marc:romanization")
    removeFieldFromRecord(record, "marc:transcribingAgency")
    removeFieldFromRecord(record, "marc:typeOfSeries")

    documentItem.scheduleSave(onError: { e ->
        failedIDs.println("Failed to save ${record[ID]} due to: $e")
    })
}

private void removeFieldFromRecord(record, String fieldName) {
    if (record.remove(fieldName) == null) {
        scheduledForChange.println "Field $fieldName not present for ${record[ID]}"
    } else {
        scheduledForChange.println "Remove field $fieldName from ${record[ID]}"
    }
}