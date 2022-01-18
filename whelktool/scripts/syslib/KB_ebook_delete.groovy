PrintWriter failedHoldIDs = getReportWriter("Failed-to-delete-holdIDs.txt")
File HoldIDs = new File(scriptDir, "KB_ebok_ID.txt")

selectByIds(HoldIDs.readLines() )  { hold ->
    if (hold.doc.sigel == "https://libris.kb.se/library/S") {
    hold.scheduleDelete(onError: { e ->
                failedHoldIDs.println("Failed to delete ${hold.doc.shortId} due to: $e")
            })
    }
}
