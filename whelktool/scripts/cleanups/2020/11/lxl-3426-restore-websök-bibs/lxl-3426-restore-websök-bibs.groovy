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
