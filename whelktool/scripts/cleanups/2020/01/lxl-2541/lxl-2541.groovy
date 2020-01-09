PrintWriter failedHoldIDs = getReportWriter("failed-to-delete-holdIDs")
PrintWriter failedBibIDs = getReportWriter("failed-to-delete-bibIDs")
PrintWriter scheduledForDeletion = getReportWriter("scheduled-for-deletion")
String BIB_ID_FILE = 'bibIDs'
File bibids = new File(scriptDir, BIB_ID_FILE)

String bibidstring = bibids.readLines().join("','")

String holdWhere =
        "id in (" +
                "select id from lddb__dependencies " +
                "where dependsonid in " +
                "( '${bibidstring}' ) )"

selectBySqlWhere(holdWhere, silent: false, { hold ->
    scheduledForDeletion.println("${hold.doc.getURI()}")
    hold.scheduleDelete(onError: { e ->
        failedHoldIDs.println("Failed to delete ${hold.doc.shortId} due to: $e")
    })
})

String bibWhere =
        "id in ('${bibidstring}')"

selectBySqlWhere(bibWhere, silent: false, { bib ->
    scheduledForDeletion.println("${bib.doc.getURI()}")
    bib.scheduleDelete(onError: { e ->
        failedBibIDs.println("Failed to delete ${bib.doc.shortId} due to: $e")
    })
})
