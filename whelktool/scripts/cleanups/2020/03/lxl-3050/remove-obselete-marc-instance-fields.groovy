failedIDs = getReportWriter("failed-to-delete-bibIDs")
scheduledForChange = getReportWriter("scheduledForChange")
fieldsToRemove = ["marc:aspect", "marc:fileAspect", "marc:soundAspect", "marc:issn" , "marc:matter"]

selectByCollection('bib') { bib ->
    fieldsToRemove.each {
        removeFieldFromInstance(bib, it)
    }
    removeFieldFromWork(bib, "marc:matter")
}

private void removeFieldFromInstance(documentItem, fieldName) {
    def instance = documentItem.doc.data['@graph'][1]
    if (instance.remove(fieldName)) {
        documentItem.scheduleSave(onError: { e ->
            failedIDs.println("Failed to save ${instance[ID]} due to: $e")
        })
        scheduledForChange.println "Remove field $fieldName from ${instance[ID]}"
    }
}

private void removeFieldFromWork(documentItem, fieldName) {
    def work = documentItem.doc.data['@graph'][2]
    if (work?.hasPart) {
        work.hasPart.each {
            if (it.remove(fieldName)) {
                documentItem.scheduleSave(onError: { e ->
                    failedIDs.println("Failed to save ${work[ID]} due to: $e")
                })
                scheduledForChange.println "Remove field $fieldName from ${work[ID]}"
            }
        }
    }
}