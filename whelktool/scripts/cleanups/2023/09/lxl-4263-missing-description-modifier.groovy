def where = """
    changedIn = 'batch import'
    and modified > '2023-09-07' and modified < '2023-09-09'
    and data#>'{@graph,0,descriptionLastModifier,@id}' = 'null'
"""

selectBySqlWhere(where) {
    def record = it.graph[0]
    record['descriptionCreator'] = ['@id':'https://libris.kb.se/library/SEK']
    record['descriptionLastModifier'] = ['@id':'https://libris.kb.se/library/SEK']
    it.scheduleSave()
}
