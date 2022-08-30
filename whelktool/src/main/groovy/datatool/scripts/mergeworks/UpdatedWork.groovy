package datatool.scripts.mergeworks

import whelk.Document

class UpdatedWork implements MergedWork {
    Document doc
    Collection<Doc> derivedFrom
    File reportDir
    String checksum

    UpdatedWork(Document doc, Collection<Doc> derivedFrom, File reportDir, String checksum) {
        this.doc = doc
        this.derivedFrom = derivedFrom
        this.reportDir = new File(reportDir, 'updated')
        this.checksum = checksum
    }
}
