// See https://jira.kb.se/browse/FMT-19 for more info

def where = """
    collection = 'bib' 
    and deleted = false 
    and data#>'{@graph,1,instanceOf,genreForm}' @> '[{"@id":"https://id.kb.se/term/barngf/Pocketb%C3%B6cker"}]' 
"""

unhandled = getReportWriter('unhandled.txt')

selectBySqlWhere(where) { bib ->
    def instance = bib.graph[1]
    def work = bib.graph[1].instanceOf
    work.genreForm.remove(["@id": "https://id.kb.se/term/barngf/Pocketb%C3%B6cker"])
    def isbns = instance['identifiedBy'].findAll { it['@type'] == 'ISBN' }
    if (isbns.size() == 1) {
        def isbn = isbns.find()
        if (!isbn.containsKey('qualifier')) {
            isbn['qualifier'] = 'pocket'
        }
        if (work.genreForm.isEmpty()) {
            work.remove('genreForm')
        }
        bib.scheduleSave()
    } else {
        unhandled.println(bib.doc.shortId)
    }
}