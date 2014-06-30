package se.kb.libris.whelks.component

import se.kb.libris.whelks.Document

import spock.lang.*


class PairtreeHybridDiskStorageSpec extends Specification {

    def settings = [
        storageDir: "/tmp/"
    ]

    def "should batch load"() {
        given:
        def storage = new DummyPairtreeHybridDiskStorage(settings)
        def doc1 = makeDoc("/path/1")
        def recently = System.currentTimeMillis()
        when:
        storage.batchLoad([doc1])
        then: "document has been written and indexed"
        storage.writeCounter == 1
        storage.indexCounter == 1
        and: "document timestamp has been updated"
        doc1.timestamp >= recently
    }

    def "should batch load a lot"() {
        given:
        def aLot = 10000
        def storage = new DummyPairtreeHybridDiskStorage(settings)
        when:
        storage.batchLoad((0..<aLot).collect { makeDoc("/path/$it") })
        then: "lots of documents have been written, then indexed once"
        storage.writeCounter == aLot
        storage.indexCounter == 1
    }

    def makeDoc(id) {
        def doc = new Document()
        doc.identifier = id
        return doc
    }

    class DummyPairtreeHybridDiskStorage extends PairtreeHybridDiskStorage {

        int writeCounter = 0
        int indexCounter = 0

        DummyPairtreeHybridDiskStorage(settings) {
            super(settings)
        }

        void connectClient() {
        }

        boolean handlesContent(String ctype) {
            return true
        }

        protected boolean writeDocumentToDisk(Document doc, String filePath, String fileName) {
            writeCounter++
            return true
        }

        void index(final List<Map<String,String>> data) {
            indexCounter++
        }

    }

}
