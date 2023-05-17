package datatool.scripts.mergeworks

import whelk.JsonLd
import whelk.Whelk

class LinkedWork extends Work {

    LinkedWork(Doc doc, Collection<Doc> derivedFrom, File reportDir) {
        this.doc = doc
        this.document = doc.document
        this.derivedFrom = derivedFrom
        this.reportDir = new File(reportDir, 'updated')
        this.workPath = ['@graph', 1]
    }

    @Override
    void store(Whelk whelk) {
        whelk.storeAtomicUpdate(document, !loud, false, changedIn, generationProcess, doc.checksum)
    }

    void update(Map workData) {
        document.data[JsonLd.GRAPH_KEY][1] = workData
    }
}
