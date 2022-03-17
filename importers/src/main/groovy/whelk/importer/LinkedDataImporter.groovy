package whelk.importer

import groovy.transform.CompileStatic

import whelk.Document
import whelk.IdGenerator
import whelk.JsonLd
import whelk.TargetVocabMapper
import whelk.Whelk
import whelk.converter.TrigToJsonLdParser
import whelk.exception.CancelUpdateException

@CompileStatic
class LinkedDataImporter extends DatasetImporter {
    static String ID = JsonLd.ID_KEY
    static String TYPE = JsonLd.TYPE_KEY
    static String GRAPH = JsonLd.GRAPH_KEY

    Whelk whelk
    TargetVocabMapper tvm
    Map contextDocData

    LinkedDataImporter(Whelk whelk) {
        this.whelk = whelk
        contextDocData = getDocByMainEntityId(whelk.kbvContextUri).data
        tvm = new TargetVocabMapper(whelk.jsonld, contextDocData)
    }

    void importRdfAsMultipleRecords(String url) {
        def datasetUrl = url
        /* TODO: indirection via record in XL? Like:
        <https://id.kb.se/dataset/example> a :Dataset ;
            :dataSource <https://example.org/data.rdf> .
        */
        def srcData = new URL(url).withInputStream { TrigToJsonLdParser.parse(it) }
        def kbvData = tvm.applyTargetVocabularyMap(whelk.defaultTvmProfile, contextDocData, srcData)
        Set<String> idsInInput = []

        for (item in kbvData[GRAPH]) {
            idsInInput.add(storeAsRecord((Map) item, datasetUrl).getShortId())
        }

        List<String> needsRetry = []
        long deletedCount = removeDeleted(whelk, datasetUrl, idsInInput, needsRetry)
        println("Deleted: ${deletedCount}")
    }

    private Document storeAsRecord(Map mainEntity, String datasetUrl) {
        String mainUrl = mainEntity[ID]
        Document existingDoc = getDocByMainEntityId(mainUrl)
        String docId = getDocId(existingDoc)
        Document incomingDoc = recordify(docId, mainEntity, datasetUrl)
        if (existingDoc != null) { // Update
            println("Update")
            update(incomingDoc)
        } else { // Create
            println("Create")
            whelk.createDocument(incomingDoc, "xl", null, "definitions", false)
        }
        return incomingDoc
    }

    private Document recordify(String docId, Map mainEntity, String datasetUrl) {
        def record = [
            (ID): docId,
            (TYPE): 'CacheRecord', // TODO: depending on .. dataset?
            'inDataset': [(ID): datasetUrl],
            'mainEntity': [(ID): mainEntity[ID]],
            // TODO: created, modified ...
        ]
        def data = [(GRAPH): [record, mainEntity]]
        return new Document(data)
    }

    private Document getDocByMainEntityId(String id) {
        return whelk.storage.loadDocumentByMainId(id, null)
    }

    private String getDocId(Document doc) {
        return doc != null ? doc.getCompleteId() : Document.BASE_URI.toString() + IdGenerator.generate()
    }

    private Document update(Document incomingDoc) {
        whelk.storeAtomicUpdate(incomingDoc.getShortId(), true, "xl", null, { doc ->
            if (doc.getChecksum(whelk.jsonld) != incomingDoc.getChecksum(whelk.jsonld)) {
                doc.data = incomingDoc.data
            }
            else {
                throw new CancelUpdateException() // Not an error, merely cancels the update
            }
        })
    }

    public static void main(String[] args) {
        String url = args[0]
        new LinkedDataImporter(null).importRdfAsMultipleRecords(url)
    }
}
