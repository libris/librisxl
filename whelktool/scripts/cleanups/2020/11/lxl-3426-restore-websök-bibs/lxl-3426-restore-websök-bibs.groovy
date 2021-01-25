PrintWriter failed = getReportWriter("failed")
PrintWriter scheduledForDeletion = getReportWriter("scheduled-deletes")
PrintWriter scheduledForResave = getReportWriter("scheduled-updates")

/*
 Obtainted using:
 select id from lddb where data#>>'{@graph,0,generationProcess,@id}' = 'https://libris.kb.se/sys/globalchanges/r/folders/br/7my06pfs7wn0lmglq3_tmk380000gp/T/xl_script5190490847460028046/script.groovy'
 */
File holdsToDelete = new File(scriptDir, "holds-to-delete")

/*
  Delete the whole set of holdings that were created by the bad script.
  A significant number of them became holdings on other holdings or auths.
  The whole set will be re-added later, using the original input data.
 */
selectByIds( holdsToDelete.readLines() ) { data ->
    scheduledForDeletion.println("${data.doc.getURI()}")
    data.scheduleDelete(onError: { e ->
        failed.println("Failed to delete ${data.doc.shortId} due to: $e")
    })
}
