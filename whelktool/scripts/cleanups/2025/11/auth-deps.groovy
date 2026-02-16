import whelk.Whelk
import whelk.datatool.DocumentItem

Whelk whelk = getWhelk()

selectByCollection('auth') { DocumentItem doc ->
    whelk.storage.recalculateDependencies(doc.doc)
}

String where = """
        data #>> '{@graph,1,@type}' = 'ContentType' 
        and deleted = false
        """

selectBySqlWhere(where) { DocumentItem doc ->
    whelk.storage.recalculateDependencies(doc.doc)
}