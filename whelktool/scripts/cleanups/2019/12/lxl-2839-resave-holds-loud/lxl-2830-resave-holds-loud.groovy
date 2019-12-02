// MUST be run with --allow-loud

PrintWriter failedHoldIDs = getReportWriter("failed-to-update-holdIDs")
PrintWriter scheduledForUpdate = getReportWriter("scheduled-for-update")
File bibIDsFile = new File(scriptDir, "records-to-resave")

selectByIds( bibIDsFile.readLines() ) { hold ->
    scheduledForUpdate.println("${hold.doc.getURI()}")
    hold.scheduleSave(loud: true, onError: { e ->
        failedHoldIDs.println("Failed to delete ${hold.doc.shortId} due to: $e")
    })
}
