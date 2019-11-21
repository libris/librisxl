/*
See LXL-2809 & LXL-2395 for more info.
*/

PrintWriter failedIDs = getReportWriter("failed-to-delete-authIDs")
scheduledForChange = getReportWriter("scheduledForChange")

selectBySqlWhere("""
collection = 'auth' AND 
data#>>'{@graph,1,@type}' = 'Family'
 """, silent: false) { documentItem ->

   removeFieldFromRecord(documentItem, "marc:kindOfRecord")
   removeFieldFromRecord(documentItem, "marc:govtAgency")
   removeFieldFromRecord(documentItem, "marc:headingSeries")
   removeFieldFromRecord(documentItem, "marc:level")
   removeFieldFromRecord(documentItem, "marc:numberedSeries")
   removeFieldFromRecord(documentItem, "marc:personalName")
   removeFieldFromRecord(documentItem, "marc:reference")
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