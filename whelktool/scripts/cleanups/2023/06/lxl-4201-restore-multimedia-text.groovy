/**
    Restore overeager removals in lxl-4201-remove-multimedia.groovy
    See LXL-4201
 */

def where = """
    collection = 'bib' 
    AND deleted = false
    AND data#>>'{@graph,0,generationProcess,@id}' = 'https://libris.kb.se/sys/globalchanges/cleanups/2023/06/lxl-4201-remove-multimedia-text.groovy'
    """

def whelk = getWhelk()

def IGNORE = ['@type', 'genreForm', 'carrierType', 'contentType', 'intendedAudience'] as Set

selectBySqlWhere(where) { bib ->
    def before = whelk.storage.load(bib.doc.shortId, "-2")
    def (_, thing) = before.data["@graph"]
    
    def shouldRestore = ((List<Map>) asList(thing.instanceOf.hasPart))
            .findAll { part -> part.'@type' == 'Multimedia' && part.genreForm == [["@id": "https://id.kb.se/marc/Document"]] }
            .findAll {(it.keySet() - IGNORE).size() > 0 }

    shouldRestore.each {
        incrementStats("Shape", it.keySet())
        incrementStats("Content", it)
    }
    
    if (shouldRestore) {
        bib.doc.data = before.data
        bib.scheduleSave()
    }
}

def getWhelk() {
    def whelk = null

    selectByIds(['https://id.kb.se/marc']) {
        whelk = it.whelk
    }

    return whelk
}