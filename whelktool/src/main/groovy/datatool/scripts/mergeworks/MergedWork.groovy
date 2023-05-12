package datatool.scripts.mergeworks

import whelk.Document
import whelk.JsonLd
import whelk.Whelk

abstract class MergedWork {
    Document document
    Doc doc
    Collection<Doc> derivedFrom
    List workPath
    File reportDir

    String changedIn = "xl"
    String changedBy = "SEK"
    String generationProcess = 'https://libris.kb.se/sys/merge-works'

    boolean loud = false

    abstract void store(Whelk whelk)

    void linkInstances(Whelk whelk) {
        derivedFrom?.each {
            Document d = it.doc
            d.data[JsonLd.GRAPH_KEY][1][JsonLd.WORK_KEY] = [(JsonLd.ID_KEY): d.getThingIdentifiers().first()]
            d.setGenerationDate(new Date())
            d.setGenerationProcess(generationProcess)
            whelk.storeAtomicUpdate(d, !loud, false, changedIn, changedBy, it.checksum)
        }
    }

    void setGenerationFields() {
        document.setGenerationDate(new Date())
        document.setGenerationProcess(generationProcess)
    }

    void addCloseMatch(List<String> workIds) {
        def work = Util.getPathSafe(document.data, workPath)
        def closeMatch = (Util.asList(work['closeMatch']) + (workIds - work['@id']).collect { ['@id': it] }).unique()
        if (closeMatch) {
            work['closeMatch'] = closeMatch
        }
    }
}