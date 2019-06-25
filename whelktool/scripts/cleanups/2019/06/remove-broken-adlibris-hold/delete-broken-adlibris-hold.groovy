PrintWriter failedHoldIDs = getReportWriter("failed-to-delete-holdIDs")
PrintWriter scheduledForDeletion = getReportWriter("scheduled-for-deletion")
String HOLD_ID_FILE = 'lxl-2327-holdIds.txt'
File holdIds = new File(scriptDir, HOLD_ID_FILE)

selectByIds(holdIds.readLines()) { hold ->
        scheduledForDeletion.println("${hold.doc.getURI()}")
        hold.scheduleDelete(onError: { e ->
            failedHoldIDs.println("Failed to delete ${hold.doc.shortId} due to: $e")
        })
}