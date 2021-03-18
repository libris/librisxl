import whelk.util.DocumentUtil
import whelk.Whelk

uris = [
        getWhelk().baseUri.toString(),
        "https://id.kb.se",
]

selectBySqlWhere('deleted = false') { doc ->
    try {
        DocumentUtil.findKey(doc.graph, '@id') { String value, List path ->
            if (uris.any{value.startsWith(it)}) {
                def p = path.findAll {!(it instanceof Integer) && it != '@id'}
                if (p) {
                    incrementStats('0 p', p.join('.'))
                    incrementStats(p.last(), p.join('.'))
                }
            }
        }    
    } catch (Exception e) {
        println(e)
    }
}

def getWhelk() {
    def whelk = null
    selectByIds(['https://id.kb.se/marc']) { docItem ->
        whelk = docItem.whelk
    }
    if (!whelk) {
        throw new RuntimeException("Could not get Whelk")
    }
    return whelk
}