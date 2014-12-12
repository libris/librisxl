package whelk.converter.libris

import groovy.util.logging.Slf4j as Log
import se.kb.libris.util.marc.MarcRecord
import whelk.Document
import whelk.plugin.BasicFormatConverter
import whelk.plugin.libris.JsonLD2MarcConverter
import whelk.converter.*

import org.codehaus.jackson.map.*

import static whelk.converter.JSONMarcConverter.marcRecordAsXMLString

@Log
class JsonLD2MarcXMLConverter extends BasicFormatConverter {

    JsonLD2MarcConverter jsonldConverter = null

    @Override
    Document doConvert(Document doc) {

        if (jsonldConverter == null)
            jsonldConverter =  plugins.find { it instanceof JsonLD2MarcConverter }

        assert jsonldConverter;

        Document jsonldDocument = jsonldConverter.doConvert(doc)

        MarcRecord record = JSONMarcConverter.fromJson(jsonldDocument.getDataAsString())

        log.debug("Creating new document ${doc.identifier} from doc with entry: ${doc.entry} and meta: ${doc.meta}")

        log.debug("Setting document identifier in field 877.")
        def df = record.createDatafield("877")
        df.addSubfield("a".charAt(0), mapper.writeValueAsString(["identifier":doc.identifier,"modified":doc.modified,"checksum":doc.checksum]))
        df.addSubfield("2".charAt(0), "librisxl")
        record.addField(df)

        Document document = whelk.createDocument(getResultContentType())
        document.setIdentifier(doc.getIdentifier())
        document.data = whelk.converter.JSONMarcConverter.marcRecordAsXMLString(record)

        log.debug("Document ${doc.identifier} created successfully with entry: ${doc.entry} and meta: ${doc.meta}")
        return document
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
