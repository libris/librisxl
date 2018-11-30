PrintWriter failedBibIDs = getReportWriter("failed-to-delete-bibIDs")
File bibIDsFile = new File(scriptDir, "isbn1013DuplicateBibIDs")
selectByIds( bibIDsFile.readLines() ) { bib ->

    selectBySqlWhere("id in (select id from lddb__dependencies where dependsonid = '${bib.doc.shortId}' and relation = 'itemOf')") { hold ->
        if (hold.doc.sigel == "BOKR")
            hold.scheduleDelete()
    }

    bib.scheduleDelete(onError: { e -> failedBibIDs.println("Failed to delete ${bib.doc.shortId} due to: $e") })
}
