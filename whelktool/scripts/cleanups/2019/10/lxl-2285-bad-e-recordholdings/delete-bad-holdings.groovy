PrintWriter failedHoldIDs = getReportWriter("failed-to-delete-holdIDs")
PrintWriter scheduledForDeletion = getReportWriter("scheduled-for-deletion")
File bibIDsFile = new File(scriptDir, "xl-bib-ids-for-lxl2285")
Set sigelSet = ["LIUD","Ldix","Albe","Ddig","Gdix","HdE","Hdig","Mdhd","Kdig","Odig","LnuE","Udix","Bthd","Krhd","Qdig","Shad"]

selectByIds( bibIDsFile.readLines() ) { bib ->
    String query = "id in (select id from lddb " +
            "where data#>>'{@graph,1,itemOf,@id}' = '${bib.graph[1][ID]}')"

    selectBySqlWhere(query, silent: true) { hold ->
        if ( sigelSet.contains(hold.doc.sigel) ) {
            scheduledForDeletion.println("${hold.doc.getURI()}")
            hold.scheduleDelete(onError: { e ->
                failedHoldIDs.println("Failed to delete ${hold.doc.shortId} due to: $e")
            })
        }
    }
}
