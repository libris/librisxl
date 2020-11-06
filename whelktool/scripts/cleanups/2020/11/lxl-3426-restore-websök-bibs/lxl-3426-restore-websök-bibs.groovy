// MUST be run with --allow-loud

PrintWriter failed = getReportWriter("failed")
PrintWriter scheduledForDeletion = getReportWriter("scheduled-deletes")
PrintWriter scheduledForResave = getReportWriter("scheduled-updates")


File bibsToResave = new File(scriptDir, "bibs-to-resave")
File holdsToDelete = new File(scriptDir, "holds-to-delete")


/*
  Delete the whole set of holdings that were created by the bad script.
  A significant number of them became holdings on other holdings or auths.
  The whole set will be re-added later, using the original input data.
 */
selectByIds( holdsToDelete.readLines() ) { data ->
    scheduledForDeletion.println("${data.doc.getURI()}")
    data.scheduleDelete(onError: { e ->
        failed.println("Failed to delete ${data.doc.shortId} due to: $e")
    })
}

/*
  Resave those bib records that shared a controlnumber with a holding/auth,
  on which we additionally added a new bad holding. These bib records
  were overwritten in websök with holdings, and need to be resaved/restored.
 */
selectByIds( bibsToResave.readLines() ) { data ->
    scheduledForResave.println("${data.doc.getURI()}")
    data.scheduleSave(loud: true, onError: { e ->
        failed.println("Failed to update ${data.doc.shortId} due to: $e")
    })
}
