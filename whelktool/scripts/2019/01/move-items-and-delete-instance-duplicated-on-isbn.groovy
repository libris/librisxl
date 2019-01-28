PrintWriter failedBibIDs = getReportWriter("failed-to-delete-bibIDs")
PrintWriter scheduledForChange = getReportWriter("scheduledForChange")

List recordsToKeep = []
Map<String, Map<String, String>> conflictsByIsbn = [:]
Map conflictsByBadId = [:]

new File(scriptDir, "isbn-dubletter.txt").each { row ->
    def (isbn, xlId, sigelCodes) = row.split(/\t/).collect { it.trim() }
    Set sigels = sigelCodes.split(/,/).collect { it.trim() }
    def conflict = conflictsByIsbn[isbn]
    if (conflict) {
        conflict.badId = xlId
        conflict.misplacedSigels = sigels
        conflictsByBadId[conflict.badId] = conflict
    } else {
        conflict = conflictsByIsbn[isbn] = [goodId: xlId, correctSigels: sigels, isbn: isbn]
    }
}

// Select all bad bib records to be deleted
selectByIds(conflictsByBadId.keySet() as List) { badBib ->
    def conflict = conflictsByBadId[badBib.doc.shortId]
    assert !badBib.graph[1].containsKey('identifiedBy') ||
        badBib.graph[1].identifiedBy?.find {
            it[TYPE] == 'ISBN' && (it.value.size() == 13
                    ? it.value == conflict.isbn
                    : conflict.isbn[0..-2].contains(it.value[0..-2]))
        }
    // select all (eventual) hold for the bad bib, and move to good bib
    // TODO: determine why result are fewer from lddb__dependencies (and how much quicker it really is for larger selections)
    //selectBySqlWhere("id in (select id from lddb__dependencies where dependsonid = '${badBib.doc.shortId}' and relation = 'itemOf')", silent: false) { hold ->
    // TODO: match both itemOf and heldBy?
    selectBySqlWhere("""
            data#>>'{@graph,1,itemOf,@id}' = '${badBib.graph[1][ID]}'
    """ , silent: true) { hold ->
        // TODO: Skip conditional? They may be obsolete...
        if (hold.doc.sigel in conflict.misplacedSigels) {
            selectByIds([conflict.goodId]) { goodBib ->
                def fromId = hold.graph[1].itemOf[ID]
                def toId = goodBib.graph[1][ID]
                assert fromId.contains(conflict.badId)
                assert toId.contains(conflict.goodId)
                hold.graph[1].itemOf = [(ID): toId]
                hold.scheduleSave()
                scheduledForChange.println "CHANGE HOLD $hold.doc.shortId itemOf TO: $toId (FROM: $fromId)"
            }
        }
    }
    badBib.scheduleDelete(onError: { e ->
            failedBibIDs.println("Failed to delete ${badBib.doc.shortId} due to: $e")
        })
    scheduledForChange.println "DELETE BIB $badBib.doc.shortId"
}
