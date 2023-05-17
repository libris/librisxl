package datatool.scripts.mergeworks

import whelk.Whelk

class LocalWork extends Work {

    LocalWork(Doc doc, File reportDir) {
        this.doc = doc
        this.document = doc.document
        this.reportDir = new File(reportDir, 'updated')
        this.workPath = ['@graph', 1, 'instanceOf']
    }

    @Override
    void store(Whelk whelk) {
        whelk.storeAtomicUpdate(document, !loud, false, changedIn, generationProcess, doc.checksum)
    }
}
