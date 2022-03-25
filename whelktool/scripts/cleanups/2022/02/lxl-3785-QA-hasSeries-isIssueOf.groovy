/* 
Clean up QA after old script
*/

def where = """
    collection = 'bib'
    AND deleted = 'false'
    AND data#>>'{@graph,1,@type}' = 'Electronic'
    AND data#>>'{@graph,0,generationProcess,@id}' = 'https://libris.kb.se/sys/globalchanges/cleanups/2022/02/lxl-3785-supplementTo-hasSeries.groovy'
"""

selectBySqlWhere(where) { bib ->
    def (record, thing) = bib.graph
    
    if (!thing.hasSeries || thing.isIssueOf) {
        return
    }

    thing.isIssueOf = thing.hasSeries
    thing.remove('hasSeries')
    bib.scheduleSave()
}