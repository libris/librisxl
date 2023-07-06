def where = """
    collection = 'bib' 
    and deleted = false
    and data#>>'{@graph,1,instanceOf,@type}' = 'Text'
    and data#>'{@graph,1,instanceOf,language}' @> '[{"@id":"https://id.kb.se/language/swe"}]'
    and data#>'{@graph,1,instanceOf,genreForm}' @> '[{"@id":"https://id.kb.se/marc/FictionNotFurtherSpecified"}]'
    and data#>'{@graph,1,instanceOf}' ? 'illustrativeContent' 
"""

selectBySqlWhere(where) {
    def instance = it.graph[1]
    def work = instance.instanceOf



/* 
    if (!instance.illustrativeContent) { 
        create instance.illustrativeContent []
        append when unique entities
    }
    move work.illustrativeContent -> instance.IllustrativeContent

    work.illustrativeContent.remove

*/