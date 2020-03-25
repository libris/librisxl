failedIDs = getReportWriter("failed-to-delete-bibIDs")
scheduledForChange = getReportWriter("scheduledForChange")
fieldsToRemove = ["marc:aspect", "marc:fileAspect", "marc:soundAspect", "marc:issn" , "marc:matter"]

selectByCollection('bib') { bib ->
    fieldsToRemove.each {
        removeFieldFromRecord(bib, it)
    }
    removeFieldFromPath(bib, "marc:matter")
}

private void removeFieldFromRecord(documentItem, fieldName) {
    def record = documentItem.doc.data['@graph'][0]
    if (record.remove(fieldName)) {
        documentItem.scheduleSave(onError: { e ->
            failedIDs.println("Failed to save ${record[ID]} due to: $e")
        })
        scheduledForChange.println "Remove field $fieldName from ${record[ID]}"
    }
}

private void removeFieldFromPath(documentItem, fieldName) {
    def record = documentItem.doc.data['@graph'][0]

    // There are 361 "marc:matter" occurrences in this path
    def hasPartStillImage = record?.
            instanceOf?.
            StillImage?.
            hasPart?.
            StillImage

    if (hasPartStillImage && hasPartStillImage.remove(fieldName)) {
        documentItem.scheduleSave(onError: { e ->
            failedIDs.println("Failed to save ${record[ID]} due to: $e")
        })
        scheduledForChange.println "Remove field $fieldName from ${record[ID]} on $hasPartStillImage"
    }
}