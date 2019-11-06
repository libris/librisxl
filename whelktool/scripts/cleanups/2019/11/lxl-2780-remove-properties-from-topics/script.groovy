/*
See LXL-2780 & LXL-2395 for more info.
*/

PrintWriter failedIDs = getReportWriter("failed-to-delete-bibIDs")
scheduledForChange = getReportWriter("scheduledForChange")

selectBySqlWhere("collection = 'auth' AND data#>>'{@graph,1,@type}' = 'Geographic' OR data#>>'{@graph,1,@type}' = 'Topic' OR data#>>'{@graph,1,@type}' = 'TopicSubdivision' OR data#>>'{@graph,1,@type}' = 'Temporal' OR data#>>'{@graph,1,@type}' = 'ComplexSubject'", silent: false) { documentItem ->

    removeFieldFromRecord(documentItem, "descriptionConventions")
    removeFieldFromRecord(documentItem, "descriptionUpgrader")
    removeFieldFromRecord(documentItem, "marc:catalogingSource")
    removeFieldFromRecord(documentItem, "marc:kindOfRecord")
    removeFieldFromRecord(documentItem, "marc:level")
    removeFieldFromRecord(documentItem, "marc:numberedSeries")
    removeFieldFromRecord(documentItem, "marc:personalName")
    removeFieldFromRecord(documentItem, "marc:reference")
    removeFieldFromRecord(documentItem, "marc:romanization")
    removeFieldFromRecord(documentItem, "marc:transcribingAgency")
    removeFieldFromRecord(documentItem, "marc:typeOfSeries")

}

private void removeFieldFromRecord(documentItem, String fieldName) {
    def record = documentItem.doc.data.get('@graph')[0]
    if (record.remove(fieldName) == null) {
        scheduledForChange.println "Field $fieldName not present for ${record[ID]}"
    } else {
        documentItem.scheduleSave(onError: { e ->
          failedIDs.println("Failed to save ${record[ID]} due to: $e")
        })
        scheduledForChange.println "Remove field $fieldName from ${record[ID]}"
    }
}