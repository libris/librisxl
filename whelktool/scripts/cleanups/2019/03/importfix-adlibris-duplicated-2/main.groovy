PrintWriter failedBibIDs = getReportWriter("failed-to-delete-bibIDs")
PrintWriter scheduledForChange = getReportWriter("scheduledForChange")

List recordsToKeep = []
Map conflictsByDeleteId = [:]

boolean verifyIsbnPresent(bib, isbn) {
    def finder = {
        def v = it.value.trim()
        it[TYPE] == 'ISBN' && (v.size() == 13
                ? v == isbn
                : isbn[0..-2].contains(v[0..-2]))
    }
    assert !bib.graph[1].containsKey('identifiedBy') ||
        bib.graph[1].identifiedBy.find(finder) ||
        !bib.graph[1].containsKey('indirectlyIdentifiedBy') ||
        bib.graph[1].indirectlyIdentifiedBy.find(finder)
}

new File(scriptDir, "Isbn-dubbletter_2-190211.tsv").each { row ->
    def (keepId, isbn, deleteId) = row.split(/\t/).collect { it.trim() }
    // Adjust implicit legacy bib id
    if (keepId.size() < 13) {
        keepId = "http://libris.kb.se/bib/$keepId" as String
    }
    assert deleteId.size() >= 13
    conflictsByDeleteId[deleteId] = [keepId: keepId, isbn: isbn, deleteId: deleteId]
}

// Select all bad bib records to be deleted.
selectByIds(conflictsByDeleteId.values().collect { it.deleteId }) { badBib ->
    def conflict = conflictsByDeleteId[badBib.doc.shortId]
    if (!conflict) {
        conflict = badBib.doc.recordIdentifiers.findResult {
            conflictsByDeleteId[it]
        }
    }
    verifyIsbnPresent(badBib, conflict.isbn)

    selectByIds([conflict.keepId]) { goodBib ->
        assert goodBib.doc.created < badBib.doc.created
        verifyIsbnPresent(goodBib, conflict.isbn)

        def toId = goodBib.graph[1][ID]
        assert toId.contains(goodBib.doc.shortId)

        def goodItemsHeldBy = [:]
        selectBySqlWhere("""
                data#>>'{@graph,1,itemOf,@id}' = '${toId}'
        """ , silent: true) { hold ->
            goodItemsHeldBy.get(hold.graph[1].heldBy[ID], []) << hold.graph[1][ID]
        }

        // Select all (eventual) hold for the bad bib, and move to good bib.
        selectBySqlWhere("""
                data#>>'{@graph,1,itemOf,@id}' = '${badBib.graph[1][ID]}'
        """ , silent: true) { hold ->
            def fromId = hold.graph[1].itemOf[ID]
            assert fromId.contains(badBib.doc.shortId)

            def itemId = "<${hold.graph[0][ID]}>"

            boolean brokenItem = false
            if (!hold.graph[1][ID]) {
                // Incomplete skeleton item!
                assert !hold.graph[1][TYPE]
                assert !hold.graph[1].heldBy
                brokenItem = true
                itemId += " [INCOMPLETE ITEM]"
            }

            def heldBy = hold.graph[1].heldBy?.get(ID)
            def goodItems = goodItemsHeldBy[heldBy]

            if (brokenItem || goodItems) {
                hold.scheduleDelete(loud: true)
                def goodItem = goodItems?.collect { "<$it>" }?.join(", ")
                scheduledForChange.println "DELETE HOLD ${itemId} (heldBy <$heldBy> on <$toId> as ${goodItem})"
            } else {
                hold.graph[1].itemOf = [(ID): toId]
                hold.scheduleSave(loud: true)
                scheduledForChange.println "CHANGE HOLD ${itemId} itemOf TO: <$toId> (FROM: <$fromId>)"
            }
        }

        badBib.scheduleDelete(onError: { e ->
                failedBibIDs.println("Failed to delete ${badBib.graph[0][ID]} due to: $e")
            }, loud: true)
        scheduledForChange.println "DELETE BIB <${badBib.graph[0][ID]}> (kept <${goodBib.graph[0][ID]}>, paired on isbn: $conflict.isbn)"
    }
}
