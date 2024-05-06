String where = "id in (select id from lddb__identifiers where graphindex = 1 AND iri like '%#work' and mainid is false)"

selectBySqlWhere(where) { doc ->
    if (doc.graph[1]['sameAs'].removeIf{ it['@id'].endsWith('#work') }) {
        doc.scheduleSave()
    }
    if (doc.graph[1]['sameAs'].isEmpty()) {
        doc.graph[1].remove('sameAs')
    }
}