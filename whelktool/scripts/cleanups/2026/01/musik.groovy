import whelk.Whelk
import whelk.datatool.DocumentItem

Whelk whelk = getWhelk()

String where = """
        where data #>> '{@graph,1,@type}' = 'ContentType' 
        and deleted = false
        """

selectBySqlWhere(where) { DocumentItem doc ->
    whelk.storage.recalculateDependencies(doc.doc)
}

//whelk.reindexAllLinks('fbr4fb6qh08ngbz7')

def q = [
        "instanceOf._categoryByCollection.identify.@id": ["https://id.kb.se/term/rda/NotatedMusic"],
        '_sort': ["@id"]
]

selectByIds(queryIds(q).collect()) { DocumentItem doc ->
    whelk.elastic.index(doc.doc, whelk)
}

q = [
        "instanceOf._categoryByCollection.identify.@id": ["https://id.kb.se/term/rda/PerformedMusic"],
        '_sort': ["@id"]
]

selectByIds(queryIds(q).collect()) { DocumentItem doc ->
    whelk.elastic.index(doc.doc, whelk)
}


