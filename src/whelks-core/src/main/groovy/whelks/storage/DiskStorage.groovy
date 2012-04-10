package se.kb.libris.whelks.storage

import se.kb.libris.whelks.Document

class DiskStorage implements Storage {

    def storageDir = "./whelkstorage"

    DiskStorage(sPath = null) {
        if (sPath != null) { this.storageDir = sPath }
        dir = File(this.storageDir)
        dir.mkdir()
    }

    def void store(Document d) {
        println "storing document " + d
        def filename = d.getIdentifier().toString()
        File file = new File("$storageDir/$filename").withWriter {
            out -> 
                out.println d.getData()
        }
    }

    def Document retrieve(URL u) {
        return null
    }
}
