PrintWriter failedBibIDs = getReportWriter("failed-to-restore-bibIDs")
File bibIDsFile = new File(scriptDir, "bibIDsToRestore")
selectByIds( bibIDsFile.readLines() ) { bib ->

    if (bib.doc.encodingLevel == "marc:PartialPreliminaryLevel") {
        bib.scheduleRevertTo(loud: false, time: "2018-07-03T05:00:00Z", onError:
                { e -> failedBibIDs.println("Failed to restore ${bib.doc.shortId} due to: $e") })
    }
}
