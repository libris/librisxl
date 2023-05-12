package datatool.scripts.mergeworks

import whelk.Whelk

class LocalWork extends MergedWork {

    LocalWork(Doc doc, File reportDir, String checksum) {
        this.doc = doc
        this.document = doc.doc
        this.oldChecksum = checksum
        this.reportDir = new File(reportDir, 'updated')
        this.workPath = ['@graph', 1, 'instanceOf']
    }

    @Override
    void store(Whelk whelk) {
        whelk.storeAtomicUpdate(document, !loud, false, changedIn, generationProcess, oldChecksum)
    }
}
