package whelk.util

import se.kb.libris.util.marc.MarcRecord
import se.kb.libris.util.marc.io.Iso2709Deserializer
import whelk.Document
import whelk.converter.MarcJSONConverter
import whelk.converter.marc.MarcFrameConverter

import java.sql.ResultSet
import java.sql.Timestamp
import java.text.Normalizer

public class VCopyToWhelkConverter {

    public static class VCopyDataRow {
        byte[] data
        boolean isDeleted
        Timestamp created
        Timestamp updated
        String collection
        int bib_id
        int auth_id
        int mfhd_id
        byte[] authdata
        String sigel


        public VCopyDataRow(ResultSet resultSet, String collection) {
            data = resultSet.getBytes('data')
            isDeleted = resultSet.getBoolean('deleted')
            created = resultSet.getTimestamp('create_date')
            updated = resultSet.getTimestamp('update_date')
            this.collection = collection
            bib_id = collection == 'bib' ? resultSet.getInt('bib_id') : 0
            auth_id = collection == 'bib' ? resultSet.getInt('auth_id') : 0
            mfhd_id = collection == 'hold' ? resultSet.getInt('mfhd_id') : 0
            authdata = collection == 'bib' ? resultSet.getBytes('auth_data') : null
            sigel = collection == "hold" ? resultSet.getString("shortname") : null
        }
    }


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

            Document document = new Document(convertedData)
            return document
        }
    }

    static Map getMarcDocMap(byte[] data) {
        byte[] dataBytes = normalizeString(
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

    static String normalizeString(String inString) {
        if (!Normalizer.isNormalized(inString, Normalizer.Form.NFC)) {
            return Normalizer.normalize(inString, Normalizer.Form.NFC)
        }
        return inString
    }
}
