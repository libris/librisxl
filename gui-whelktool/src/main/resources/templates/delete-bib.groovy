PrintWriter failedBibIDs = getReportWriter("failed-to-delete-bibIDs")
PrintWriter scheduledForDeletion = getReportWriter("scheduled-for-deletion")
File bibids = new File('INPUT')

for (String bibId : bibids) {
    selectBySqlWhere("collection = 'bib' and data#>>'{@graph,0,controlNumber}' = '$bibId' ", silent: false, { bib ->
        scheduledForDeletion.println("${bib.doc.getURI()}")
        bib.scheduleDelete(onError: { e ->
            failedBibIDs.println("Failed to delete ${bib.doc.shortId} due to: $e")
        })
    })
}