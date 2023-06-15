def where = """
    collection = 'bib' 
    and deleted = false 
    and data#>'{@graph,1,instanceOf,genreForm}' @> '[{"@id":"https://id.kb.se/term/barngf/Pocketb%C3%B6cker"}]' 
"""

selectBySqlWhere(where) {
    def instance = it.graph[1]
    def work = it.graph[1].instanceOf
    work.genreForm.remove(["@id": "https://id.kb.se/term/barngf/Pocketb%C3%B6cker"])
    if (work.genreForm.isEmpty()) {
        work.remove('genreForm')
    }
    instance.genreForm = (asList(instance.genreForm) + ["@id": "https://id.kb.se/term/barngf/Pocketb%C3%B6cker"]).unique()
    it.scheduleSave()
}