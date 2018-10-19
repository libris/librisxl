// Assuming the full list in: länkserver-borttagning_MASTER.tsv (but with column names removed)
// the holdids are retrieved as such:
// cat länkserver-borttagning_MASTER.tsv | cut -f2 > hold-IDs
// the bibids are retrieved as such:
// cat länkserver-borttagning_MASTER.tsv | cut -f1 | sort -u > bib-IDs

PrintWriter failedIDs = getReportWriter("failed-to-delete-IDs")//new PrintWriter(new File ("failed-to-delete-IDs"))
File holdIDsFile = new File(scriptDir, "linkserv-holdIDs")
selectByIds( holdIDsFile.readLines() ) {
    it.scheduleDelete(onError: { e -> failedIDs.println("Failed to delete ${it.doc.shortId} due to: $e") })
}

File bibIDsFile = new File(scriptDir, "linkserv-bibIDs")
selectByIds( bibIDsFile.readLines() ) {
    it.scheduleDelete(onError: { e ->
        failedIDs.println("Failed to delete ${it.doc.shortId} due to: $e")
    })
}
