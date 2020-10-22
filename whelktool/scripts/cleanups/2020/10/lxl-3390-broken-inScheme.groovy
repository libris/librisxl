/**
 Link blank nodes in 'inScheme'

 See LXL-3390
 */

import whelk.util.Statistics
import whelk.filter.BlankNodeLinker

substitutions = [
        'gmgpc/ / swe'                    : 'gmgpc/swe',
        'sap'                             : 'sao',
]

class Script {
    static PrintWriter modified
    static PrintWriter errors
    static BlankNodeLinker linker
    static Statistics statistics = new Statistics(5).printOnShutdown()
}

Script.modified = getReportWriter("modified.txt")
Script.errors = getReportWriter("errors.txt")
Script.linker = buildLinker()

println("Mappings: ${Script.linker.map}")
println("Ambiguous: ${Script.linker.ambiguousIdentifiers}")

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
    Script.statistics.withContext(bib.doc.shortId) {
        if(Script.linker.linkAll(bib.graph[1], 'inScheme')) {
            Script.modified.println("${bib.doc.shortId}")
            bib.scheduleSave()
        }
    }
}

def buildLinker() {
    def types = ['TopicScheme', 'ConceptScheme']
    def matchFields = ['code']
    def linker = new BlankNodeLinker(types, matchFields, Script.statistics)

    linker.loadDefinitions(getWhelk())
    linker.addSubstitutions(substitutions)

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