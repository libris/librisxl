PrintWriter failedHoldIDs = getReportWriter("Failed-to-delete-holdIDs.txt")
File HoldIDs = new File(scriptDir, "KB_ebok_ID.txt")

selectByIds(HoldIDs.readLines() )  { hold ->
    hold.scheduleDelete(onError: { e ->
                failedHoldIDs.println("Failed to delete ${hold.doc.shortId} due to: $e")
            })
}
