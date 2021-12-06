String where = """
    collection = 'bib'
    AND deleted = false
    AND data#>'{@graph,0,bibliography}' @> '[{"@id": "https://libris.kb.se/library/DST"}]'
"""

selectBySqlWhere(where) {
    it.scheduleSave(loud: true)
}
