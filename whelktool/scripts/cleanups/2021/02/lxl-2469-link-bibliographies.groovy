/**
Link Bibliographies in meta.bibliography
 
 */

import whelk.util.Statistics
import whelk.filter.BlankNodeLinker

class Script {
    static PrintWriter errors
    static BlankNodeLinker linker
    static Statistics statistics = new Statistics(5).printOnShutdown()
}

Script.errors = getReportWriter("errors.txt")
Script.linker = buildLinker()

println("Mappings:\n${Script.linker.map.sort{ it.key }.collect{it.toString()}.join('\n') }\n")
println("Ambiguous:\n${Script.linker.ambiguousIdentifiers.sort{ it.key }.collect{it.toString()}.join('\n') }}\n")

selectByCollection('bib') { bib ->
    try {
        process(bib)
    }
    catch(Exception e) {
        Script.errors.println("${bib.doc.shortId} $e")
        e.printStackTrace(Script.errors)
    }
}

void process(bib) {
    def record = bib.graph[0]
    if(!record['bibliography']) {
        return
    }

    Script.statistics.withContext(bib.doc.shortId) {
        if(Script.linker.linkAll(record, 'bibliography')) {
            bib.scheduleSave()
        }
    }
}

def buildLinker() {
    def types = ['Library', 'Bibliography']
    def matchFields = ['sigel']
    def linker = new BlankNodeLinker(types, matchFields, Script.statistics)

    linker.loadDefinitions(getWhelk())
    linker.addSubstitutions(substitutions())

    return linker
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

def substitutions() {
    [:]
}
