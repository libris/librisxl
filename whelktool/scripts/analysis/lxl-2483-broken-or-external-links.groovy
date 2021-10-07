import groovy.transform.Memoized
import whelk.util.DocumentUtil

whelk = getWhelk()

selectBySqlWhere('deleted is false', silent: true) { doc ->
     DocumentUtil.findKey(doc.graph, "@id") { value, path ->
         if (is404(value)) {
             incrementStats('404', value)
         }
     }
}

@Memoized
boolean is404(iri) {
    String systemId = whelk.storage.getSystemIdByIri(iri)
    return !systemId
}

def getWhelk() {
    // A little hack to get a handle to whelk...
    def whelk = null
    selectByIds(['https://id.kb.se/marc']) { docItem ->
        whelk = docItem.whelk
    }
    if (!whelk) {
        throw new RuntimeException("Could not get Whelk")
    }
    return whelk
}

