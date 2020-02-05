import java.text.Normalizer

PrintWriter failedIDs = getReportWriter("failed-IDs")
PrintWriter scheduledForUpdate = getReportWriter("scheduled-for-update")

selectBySqlWhere("deleted is false", silent: false, { docItem ->
    if (!Normalizer.isNormalized(docItem.doc.getDataAsString(), Normalizer.Form.NFC)) {
        scheduledForUpdate.println("${docItem.doc.getURI()}")
        docItem.scheduleSave()
    }
})
