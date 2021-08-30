String where = """
    collection = 'bib'
    AND deleted = 'false'
    AND data#>'{@graph,1,identifiedBy}' @> '[{\"typeNote\":\"uri\"}]'
"""

selectBySqlWhere(where) {
    // Resave and {@type: Identifier, typeNote: uri} will be normalized to {@type:URI}
    it.scheduleSave()
}
