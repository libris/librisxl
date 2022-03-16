import whelk.Document
import whelk.JsonLd
import whelk.TargetVocabMapper
import whelk.Whelk
import whelk.converter.TrigToJsonLdParser

class LinkedDataImporter {
    static String ID = JsonLd.ID_KEY
    static String TYPE = JsonLd.TYPE_KEY
    static String GRAPH = JsonLd.GRAPH_KEY

    Whelk whelk
    TargetVocabMapper tvm

    LinkedDataImporter(Whelk whelk) {
        this.whelk = whelk
        //tvm = new TargetVocabMapper(whelk.jsonld, dataContext)
    }

    void importRdfAsMultipleRecords(String url) {
        def datasetUrl = url /* TODO: indirection via record in XL? Like:
        <https://id.kb.se/dataset/example> a :Dataset ;
            :dataSource <https://example.org/data.rdf> .
        */
        def srcData = new URL(url).withInputStream { TrigToJsonLdParser.parse(it) }
        def kbvData = srcData // tvm.applyTargetVocabularyMap(kbvTargetId, targetMap, srcData)
        Set<String> remainingIds = whelk?.findBy(['inDataset.@id', datasetUrl]) ?: []
        for (item in kbvData[GRAPH]) {
            storeAsRecord(item, datasetUrl)
            remainingIds.remove(item[ID])
        }
        for (remId in remainingIds) {
            whelk?.delete(remId)
        }
    }

    void storeAsRecord(Map mainEntity, String datasetUrl) {
        def mainUrl = mainEntity[ID]
        def data = recordify(mainEntity, datasetUrl)
        def doc = whelk?.getByMainEntity(mainUrl) // Update if found
        if (doc != null) { // Create new
            doc.data = data
        } else {
            doc = whelk?.createNew(data)
        }
        doc?.save()
        println "Saving: ${data}"
    }

    Map recordify(Map mainEntity, String datasetUrl) {
        def record = [
            (ID): '#tempid',
            (TYPE): 'CacheRecord', // TODO: depending on .. dataset?
            'inDataset': [(ID): datasetUrl],
            'mainEntity': [(ID): mainEntity[ID]],
            // TODO: created, modified ...
        ]
        return [
            (GRAPH): [record, mainEntity]
        ]
    }


    public static void main(String[] args) {
        String url = args[0]
        new LinkedDataImporter(null).importRdfAsMultipleRecords(url)
    }
}
