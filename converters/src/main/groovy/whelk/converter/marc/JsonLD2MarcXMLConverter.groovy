package whelk.converter.marc

import groovy.util.logging.Slf4j as Log
import org.codehaus.jackson.map.ObjectMapper
import se.kb.libris.util.marc.MarcRecord
import whelk.Document
import whelk.converter.FormatConverter
import whelk.converter.JSONMarcConverter
import whelk.converter.marc.JsonLD2MarcConverter

@Log
class JsonLD2MarcXMLConverter implements FormatConverter {

    JsonLD2MarcConverter jsonldConverter = null
    final static ObjectMapper mapper = new ObjectMapper()

    @Override
    Document convert(Document doc) {

        if (jsonldConverter == null)
            jsonldConverter =  plugins.find { it instanceof JsonLD2MarcConverter }

        assert jsonldConverter

        Document jsonDocument = jsonldConverter.doConvert(doc)

        MarcRecord record = JSONMarcConverter.fromJson(jsonDocument.getDataAsString())

        log.debug("Setting document identifier in field 887.")
        boolean has887Field = false
        for (field in record.getDatafields("887")) {
            if (!field.getSubfields("2").isEmpty() && field.getSubfields("2").first().data == "librisxl") {
                has887Field = true
                def subFieldA = field.getSubfields("a").first()
                subFieldA.setData(mapper.writeValueAsString(["@id":doc.identifier,"modified":doc.modified,"checksum":doc.checksum]))
            }
        }
        if (!has887Field) {
            def df = record.createDatafield("887")
            df.addSubfield("a".charAt(0), mapper.writeValueAsString(["@id":doc.identifier,"modified":doc.modified,"checksum":doc.checksum]))
            df.addSubfield("2".charAt(0), "librisxl")
            record.addField(df)
        }

        Document xmlDocument = whelk.createDocument(getResultContentType()).withEntry(doc.manifest).withContentType(getResultContentType()).withData(whelk.converter.JSONMarcConverter.marcRecordAsXMLString(record))

        log.debug("Document ${xmlDocument.identifier} created successfully with entry: ${xmlDocument.manifest}")
        return xmlDocument
    }

    @Override
    String getRequiredContentType() {
        return "application/ld+json"
    }

    @Override
    String getResultContentType() {
        return "application/marcxml+xml"
    }
}
