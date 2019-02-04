PrintWriter failedBibIDs = getReportWriter("failed-to-delete-bibIDs")
PrintWriter scheduledForChange = getReportWriter("scheduledForChange")

List recordsToKeep = []
Map conflictsByIsbn = [:]
Map conflictsByOtherId = [:]

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

new File(scriptDir, "isbn-dubletter.txt").each { row ->
    def (isbn, xlId, _sigelCodes) = row.split(/\t/).collect { it.trim() }
    // Adjust implicit legacy bib id
    if (xlId.size() < 13) {
        xlId = "http://libris.kb.se/bib/$xlId" as String
    }
    def conflict = conflictsByIsbn[isbn]
    if (conflict) {
        conflict.otherId = xlId
        conflictsByOtherId[xlId] = conflict
    } else {
        conflict = [someId: xlId, isbn: isbn]
        conflictsByIsbn[isbn] = conflict
    }
}

// Select all bad bib records to be deleted.
selectByIds(conflictsByOtherId.keySet() as List) { otherBib ->
    def conflict = conflictsByOtherId[otherBib.doc.shortId]
    if (!conflict) {
        conflict = otherBib.doc.recordIdentifiers.findResult {
            conflictsByOtherId[it]
        }
    }
    verifyIsbnPresent(otherBib, conflict.isbn)

    selectByIds([conflict.someId]) { someBib ->
        verifyIsbnPresent(someBib, conflict.isbn)

        assert someBib.doc.created != otherBib.doc.created
        def (goodBib, badBib) = [someBib, otherBib]
        if (goodBib.doc.created > badBib.doc.created) {
            (goodBib, badBib) = [badBib, goodBib]
        }
        assert goodBib.doc.created < badBib.doc.created

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
