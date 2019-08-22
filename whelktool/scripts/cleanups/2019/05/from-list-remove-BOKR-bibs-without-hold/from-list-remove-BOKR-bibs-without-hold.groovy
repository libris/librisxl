/*
 * This script deletes bib records without hold based on file input.
 *
 * See LXL-2419 for more info.
 */

String BIB_ID_FILE = 'lxl-2419-ids.txt'

PrintWriter notDeletedIds = getReportWriter("not-deleted-bibIds")
PrintWriter failedBibIDs = getReportWriter("failed-to-delete-bibIDs")
PrintWriter scheduledForDeletion = getReportWriter("scheduled-for-deletion")
File bibIDsFile = new File(scriptDir, BIB_ID_FILE)

selectByIds(bibIDsFile.readLines()) { bib ->
    def bibId = bib.graph[1][ID] as String;

    if (hasHoldForBib(bibId)) {
        notDeletedIds.println("Ignoring ${bibId}, record has holdings")
        return
    }

    if (hasBibliographyCode(bib)) {
        notDeletedIds.println("Ignoring ${bibId}, record has bibliography code")
        return
    }

    if (!isEncodingLevelPreliminary(bib)) {
        notDeletedIds.println("Ignoring ${bibId}, encoding level is not preliminary")
        return
    }

    scheduledForDeletion.println("${bibId}")
    bib.scheduleDelete(onError: { e ->
        failedBibIDs.println("Failed to delete ${bibId} due to: $e")
    })
}

private boolean hasHoldForBib(String bibId) {
    String query = """id in (select id from lddb 
            where data#>>'{@graph,1,itemOf,@id}' = '${bibId}') AND 
            collection = 'hold'"""

    def holdsForBib = []
    selectBySqlWhere(query, silent: false) { hold ->
        holdsForBib.add(hold.doc.getURI())
    }

    return !holdsForBib.isEmpty()
}

private boolean hasBibliographyCode(bib) {
    return bib.graph[0]['bibliography'] != null
}

private boolean isEncodingLevelPreliminary(bib) {
    def encodingLevel = bib.graph[0]['encodingLevel']
    return (encodingLevel == "marc:PartialPreliminaryLevel" ||
            encodingLevel == "marc:PrepublicationLevel")
}