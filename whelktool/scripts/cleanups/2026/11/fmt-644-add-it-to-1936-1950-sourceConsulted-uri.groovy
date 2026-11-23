String where = """
    collection = 'bib'
    AND data#>>'{@graph,0,sourceConsulted,0,derivedFrom,0,@id}' = 'https://libris.kb.se/zjt8vfphwf0sztrb'
    """

selectBySqlWhere(where) { doc ->
    Map record = doc.graph[0]
    record['sourceConsulted'][0]['derivedFrom'][0]['@id'] = 'https://libris.kb.se/zjt8vfphwf0sztrb#it'

    doc.scheduleSave()
}