/**
 Link blank nodes in 'inScheme'

 See LXL-3390
 */

import whelk.util.Statistics
import whelk.filter.BlankNodeLinker

class Script {
    static PrintWriter modified
    static PrintWriter errors
    static BlankNodeLinker linker
    static Statistics statistics = new Statistics(5).printOnShutdown()
}

Script.modified = getReportWriter("modified.txt")
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
    def thing = bib.graph[1]
    if(!(thing['carrierType'])) {
        return
    }

    Script.statistics.withContext(bib.doc.shortId) {
        if(Script.linker.linkAll(thing, 'carrierType')) {
            Script.modified.println("${bib.doc.shortId}")
            bib.scheduleSave()
        }
    }
}

def buildLinker() {
    def types = ['CarrierType']
    def matchFields = ['code', 'label']
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