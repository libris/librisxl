import whelk.Whelk
import whelk.datatool.DocumentItem

Whelk whelk = getWhelk()

String where = """
        data #>> '{@graph,1,@type}' = 'ContentType' 
        and deleted = false
        """

selectBySqlWhere(where) { DocumentItem doc ->
    whelk.storage.recalculateDependencies(doc.doc)
}

whelk.bulkIndex(whelk.elastic.getAffectedIds(["https://id.kb.se/term/rda/PerformedMusic", "https://id.kb.se/term/rda/NotatedMusic"]))


