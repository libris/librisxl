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
    var record = [
        '@id': 'TEMPID',
        'mainEntity' : ['@id': mainEntity[ID]]
    ]
    if (mainEntity.containsKey('meta')) {
      record.putAll(mainEntity.get('meta'))
      mainEntity.remove('meta')
    }
    var data = ['@graph': [record, mainEntity]]

    create(data)
}

selectFromIterable(newDocs, { newItem ->
    newItem.scheduleSave()
})
