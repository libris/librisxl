import whelk.util.Unicode

PrintWriter failedIDs = getReportWriter("failed-IDs")
PrintWriter scheduledForUpdate = getReportWriter("scheduled-for-update")

selectBySqlWhere("deleted is false", silent: false, { docItem ->
    if (!Unicode.isNormalized(docItem.doc.getDataAsString())) {
        scheduledForUpdate.println("${docItem.doc.getURI()}")
        docItem.scheduleSave()
    }
})
