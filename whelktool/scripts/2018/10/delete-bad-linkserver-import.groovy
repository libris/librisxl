// Assuming the full list in: länkserver-borttagning_MASTER.tsv (but with column names removed)
// the holdids are retrieved as such:
// cat länkserver-borttagning_MASTER.tsv | cut -f2 > hold-IDs
// the bibids are retrieved as such:
// cat länkserver-borttagning_MASTER.tsv | cut -f1 | sort -u > bib-IDs

PrintWriter failedHoldIDs = getReportWriter("failed-to-delete-holdIDs")
File holdIDsFile = new File(scriptDir, "linkserv-holdIDs")
selectByIds( holdIDsFile.readLines() ) {
    it.scheduleDelete(onError: { e -> failedHoldIDs.println("Failed to delete ${it.doc.shortId} due to: $e") })
}

PrintWriter failedBibIDs = getReportWriter("failed-to-delete-bibIDs")
File bibIDsFile = new File(scriptDir, "linkserv-bibIDs")
selectByIds( bibIDsFile.readLines() ) {
    it.scheduleDelete(onError: { e -> failedBibIDs.println("Failed to delete ${it.doc.shortId} due to: $e") })
}
