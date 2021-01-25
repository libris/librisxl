package whelk.converter.marc

import groovy.util.logging.Log4j2 as Log
import org.codehaus.jackson.map.ObjectMapper
import org.w3c.dom.DocumentFragment
import se.kb.libris.util.marc.MarcRecord
import whelk.Document
import whelk.JsonLd
import whelk.converter.FormatConverter
import whelk.converter.JSONMarcConverter

@Log
class JsonLD2MarcXMLConverter implements FormatConverter {

    // List of restricted chars defined at: https://www.w3.org/TR/xml11/#charsets
    static List<String> restrictedSequences = []
    static {
        for (int i = 0x0; i < 0x8+1; ++i)
            restrictedSequences.add("&#"+i+";")
        restrictedSequences.add("&#"+0xB+";")
        restrictedSequences.add("&#"+0xC+";")
        for (int i = 0xE; i < 0x1F+1; ++i)
            restrictedSequences.add("&#"+i+";")
        for (int i = 0x7F; i < 0x84+1; ++i)
            restrictedSequences.add("&#"+i+";")
        for (int i = 0x86; i < 0x9F+1; ++i)
            restrictedSequences.add("&#"+i+";")
    }

    JsonLD2MarcConverter jsonldConverter = null
    final static ObjectMapper mapper = new ObjectMapper()

    JsonLD2MarcXMLConverter(MarcFrameConverter marcFrameConverter) {
        jsonldConverter = new JsonLD2MarcConverter(marcFrameConverter)
    }

    @Override
    Map convert(Map data, String id) {
        Document originalDocument = new Document(data)

        Map marcJsonData = jsonldConverter.convert(data, id)

        MarcRecord record = JSONMarcConverter.fromJsonMap(marcJsonData)

        record = prepareRecord(record, id, originalDocument.getModified(), originalDocument.getChecksum())

        String xmlString = whelk.converter.JSONMarcConverter.marcRecordAsXMLString(record)
        xmlString = filterRestrictedXmlEscapeSequences(xmlString)
        Map xmlDocument = [(JsonLd.NON_JSON_CONTENT_KEY): xmlString]

        return xmlDocument
    }

    static MarcRecord prepareRecord(record, identifier, modified, checksum) {
        log.debug("Setting document identifier in field 887.")
        boolean has887Field = false
        for (field in record.getDatafields("887")) {
            if (!field.getSubfields("2").isEmpty() && field.getSubfields("2").first().data == "librisxl") {
                has887Field = true
                def subFieldA = field.getSubfields("a").first()
                subFieldA.setData(mapper.writeValueAsString(["@id":identifier,"modified":modified,"checksum":checksum]))
            }
        }
        if (!has887Field) {
            def df = record.createDatafield("887")
            df.addSubfield("a".charAt(0), mapper.writeValueAsString(["@id":identifier,"modified":modified,"checksum":checksum]))
            df.addSubfield("2".charAt(0), "librisxl")
            record.addField(df)
        }
        return record
    }

    DocumentFragment convertToFragment(final Document doc) {
        Document marcJsonDocument = jsonldConverter.convert(doc)

        MarcRecord record = JSONMarcConverter.fromJson(marcJsonDocument.dataAsString)
        record = prepareRecord(record, doc.id, doc.modified, doc.checksum)

        return whelk.converter.JSONMarcConverter.marcRecordAsXMLFragment(record)
    }

    @Override
    String getRequiredContentType() {
        return "application/ld+json"
    }

    @Override
    String getResultContentType() {
        return "application/marcxml+xml"
    }

    private static String filterRestrictedXmlEscapeSequences(String xmlString) {
        for (String s : restrictedSequences) {
            xmlString = xmlString.replaceAll(s, "")
        }
        return xmlString
    }
}
