import whelk.Whelk
import whelk.converter.TrigToJsonLdParser
import whelk.datatool.DocumentItem

List<Map> loadDescriptions(Whelk whelk, String rdfSourcePath) {
    Map data = new File(rdfPatchFile).withInputStream { TrigToJsonLdParser.parse(it) }
    contextDocData = whelk.storage.loadDocumentByMainId(whelk.systemContextUri, null).data
    return TrigToJsonLdParser.compact(data, contextDocData)[GRAPH]
}

String rdfDataFile = System.getProperty("rdfdata")
List<Map> newDocs = loadDescriptions(getWhelk(), rdfDataFile).collect {
    create( [ "@graph": [
        [
            "@id": "TEMPID",
            "mainEntity" : ["@id": it[ID]]
        ],
        it
    ]])
}

selectFromIterable(newDocs, { newItem ->
    newItem.scheduleSave()
})
