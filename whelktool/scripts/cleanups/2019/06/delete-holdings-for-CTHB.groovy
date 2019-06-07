/*
 * This script deletes selected holdings for CTHB based on file input.
 *
 * See LXL-2543 for more info.
 *
 */
List SIGEL_FOR_CTHB = ["Z", "Za", "Zl", "Enll"]
PrintWriter failedHoldIDs = getReportWriter("failed-to-delete-holdIDs")
PrintWriter scheduledForDeletion = getReportWriter("scheduled-for-deletion")
String HOLD_ID_FILE = 'lxl-2543-holdIds.txt'
File holdIds = new File(scriptDir, HOLD_ID_FILE)

selectByIds(holdIds.readLines()) { hold ->
    if (SIGEL_FOR_CTHB.contains(hold.doc.sigel)) {
        scheduledForDeletion.println("${hold.doc.getURI()}")
        hold.scheduleDelete(onError: { e ->
            failedHoldIDs.println("Failed to delete ${hold.doc.shortId} due to: $e")
        })
    } else {
        failedHoldIDs.println("Failed to delete ${hold.doc.shortId}, " +
                "item is held by ${hold.doc.sigel} not CTHB")
    }
}