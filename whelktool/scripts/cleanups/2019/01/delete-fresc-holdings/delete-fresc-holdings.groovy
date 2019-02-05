/*
 * This deletes holdings for a specified sigel.
 *
 * `BIB_ID_FILE` must contain one bib ID per row.
 */
String SIGEL_TO_DELETE = 'H'
String BIB_ID_FILE = 'fresc_ids.txt'

PrintWriter failedHoldIDs = getReportWriter("failed-to-delete-holdIDs")
PrintWriter scheduledForDeletion = getReportWriter("scheduled-for-deletion")
File bibIDsFile = new File(scriptDir, BIB_ID_FILE)

selectByIds( bibIDsFile.readLines() ) { bib ->
    String query = "id in (select id from lddb " +
        "where data#>>'{@graph,1,itemOf,@id}' = '${bib.graph[1][ID]}')"

    selectBySqlWhere(query, silent: false) { hold ->
        if (hold.doc.sigel == SIGEL_TO_DELETE) {
            scheduledForDeletion.println("${hold.doc.getURI()}")
            hold.scheduleDelete(onError: { e ->
                failedHoldIDs.println("Failed to delete ${hold.doc.shortId} due to: $e")
            })
        }
    }
}
