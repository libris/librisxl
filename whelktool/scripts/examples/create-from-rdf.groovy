import whelk.Whelk
import whelk.datatool.DocumentItem
import static whelk.converter.RdfReader.readRdf

List<Map> loadDescriptions(Whelk whelk, String rdfSourcePath) {
    var context = whelk.storage.loadDocumentByMainId(whelk.systemContextUri, null).data
    Map data = new File(rdfSourcePath).withInputStream { readRdf(it, rdfSourcePath, context) }
    return data[GRAPH]
}

String rdfSourcePath = System.getProperty("rdfdata")
List<Map> newDocs = loadDescriptions(getWhelk(), rdfSourcePath).collect { Map mainEntity ->
    def mainId = mainEntity[ID]
    assert mainId.endsWith('#it')
    var record = [
        '@id': mainId.replace('#it', ''),
        'mainEntity' : ['@id': mainId]
    ]
    if (mainEntity.containsKey('meta')) {
        def meta = mainEntity.get('meta')
        assert meta instanceof Map
        assert '@id' !in meta
        record.putAll(meta)
        mainEntity.remove('meta')
    }
    var data = ['@graph': [record, mainEntity]]

    create(data)
}

selectFromIterable(newDocs, { newItem ->
    newItem.scheduleSave()
})
