package whelk

import org.codehaus.jackson.map.ObjectMapper
import se.kb.libris.util.marc.MarcRecord
import se.kb.libris.util.marc.io.Iso2709Deserializer
import whelk.converter.MarcJSONConverter
import whelk.converter.marc.MarcFrameConverter
import whelk.importer.MySQLLoader
import whelk.util.LegacyIntegrationTools

import java.sql.Timestamp

class VCopyToWhelkConverter {
    private static ObjectMapper mapper = new ObjectMapper()

    static Map convert(List<VCopyDataRow> rows, MarcFrameConverter marcFrameConverter) {
        VCopyDataRow row = rows.last()

        Timestamp timestamp = row.updated >= row.created ? row.updated : row.created
        Map doc = getMarcDocMap(row.data)

        def controlNumber = getControlNumber(doc)
        Document whelkDocument = convertDocument(marcFrameConverter, doc, row.collection, getOaipmhSetSpecs(rows))
        whelkDocument.created = row.created

        return [collection: row.collection, document: whelkDocument, isDeleted: row.isDeleted, timestamp: timestamp, controlNumber: controlNumber, checksum: whelkDocument.getChecksum()]

    }

    private
    static Document convertDocument(MarcFrameConverter marcFrameConverter, Map doc, String collection, List authData) {
        if (doc) {
            String oldStyleIdentifier = "/" + collection + "/" + getControlNumber(doc)

            def id = LegacyIntegrationTools.generateId(oldStyleIdentifier)

            Map spec = [oaipmhSetSpecs: authData]

            Map convertedData = authData == null ? marcFrameConverter.convert(doc as Map, id as String) :
                    marcFrameConverter.convert(doc as Map, id as String, spec as Map)

            /*
             * Temporary, make converted data use the same structure (Lists, not LinkedHashmap$LinkedValues) as
             * everything else. Do this by serializing as json text and deserializing back. Obviously slow. Fix later.
             */
            String jsonString = mapper.writeValueAsString(convertedData)
            convertedData = mapper.readValue(jsonString, Map)

            Document document = new Document(convertedData)
            return document
        }
    }

    static Map getMarcDocMap(byte[] data) {
        byte[] dataBytes = MySQLLoader.normalizeString(
                new String(data as byte[], "UTF-8"))
                .getBytes("UTF-8")

        MarcRecord record = Iso2709Deserializer.deserialize(dataBytes)

        if (record) {
            return MarcJSONConverter.toJSONMap(record)
        } else {
            return null
        }
    }

    static getAuthDocsFromRows(List<VCopyDataRow> rows) {
        rows.collect { it ->
            if (it.auth_id > 0) {
                return [bibid: it.bib_id,
                        id   : it.auth_id,
                        data : getMarcDocMap(it.authdata as byte[])]
            } else return null
        }
    }

    private static List getOaipmhSetSpecs(result) {
        List specs = []
        result.each { resultSet ->
            if (resultSet.collection == "bib") {
                int authId = resultSet.auth_id ?: 0
                if (authId > 0) {
                    specs.add("authority:${authId}")
                }
            } else if (resultSet.collection == "hold") {
                if (resultSet.bib_id > 0)
                    specs.add("bibid:${resultSet.bib_id}")
                if (resultSet.sigel)
                    specs.add("location:${resultSet.sigel}")
            }
            else
                return null
        }
        return specs
    }

    private static String getControlNumber(Map doc) {
        def fields = doc.get("fields")
        for (def field : fields) {
            if (field.get("001") != null)
                return field.get("001")
        }
        return null
    }
}
