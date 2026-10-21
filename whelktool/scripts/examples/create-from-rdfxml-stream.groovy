import java.util.stream.StreamSupport

import whelk.converter.XmlIterParser
import whelk.converter.RdfXmlToJsonLdParser

import whelk.Whelk
import whelk.datatool.DocumentItem
import static whelk.converter.RdfReader.readRdf


DocumentItem saveRecord(Map data) {
    Map record = data[GRAPH].find { it -> it[TYPE] == 'Record' }
    assert record
    def mainId = record['mainEntity'][ID]
    assert mainId.endsWith('#it')
    Map mainEntity = data[GRAPH].find { it -> it[ID] == mainId }
    assert mainEntity

    return create(['@graph': [record, mainEntity]])
}

var rdfSourcePath = System.getProperty("rdfdata")
var file = new File(rdfSourcePath)

var whelk = getWhelk()
var context = whelk.storage.loadDocumentByMainId(whelk.systemContextUri).data

file.withInputStream {
    var iterParser = new XmlIterParser(it)

    for (var xml in iterParser) {
        var bis = new ByteArrayInputStream(xml.getBytes('UTF-8'))
        var data = RdfXmlToJsonLdParser.parse(bis, context)
        var docItem = saveRecord(data)
        selectFromIterable([docItem], { newItem ->
            newItem.scheduleSave()
        })
    }
}
