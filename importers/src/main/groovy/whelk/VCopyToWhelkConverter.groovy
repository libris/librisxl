package whelk

import se.kb.libris.util.marc.MarcRecord
import se.kb.libris.util.marc.io.Iso2709Deserializer
import whelk.component.PostgreSQLComponent
import whelk.converter.MarcJSONConverter
import whelk.converter.marc.MarcFrameConverter
import whelk.filter.LinkFinder
import whelk.importer.MySQLLoader
import whelk.util.LegacyIntegrationTools

import java.sql.Timestamp

class VCopyToWhelkConverter
{
    //private static MarcFrameConverter marcFrameConverter = new MarcFrameConverter(new LinkFinder(new PostgreSQLComponent()))

    public static Map convert(List<VCopyDataRow> rows, MarcFrameConverter marcFrameConverter) {
        VCopyDataRow row = rows.last()

        Document whelkDocument = null
        Timestamp timestamp = row.updated >= row.created ? row.updated : row.created
        Map doc = getMarcDocMap(row.data)
        def controlNumber = getControlNumber(doc)
        if (!isSuppressed(doc)) {
            try {
                switch (row.collection) {
                    case 'auth':
                        whelkDocument = convertDocument(marcFrameConverter, doc, row.collection, row.created)
                        break
                    case 'hold':
                        whelkDocument = convertDocument(marcFrameConverter, doc, row.collection, row.created, getOaipmhSetSpecs(rows))
                        break
                    case 'bib':
                        whelkDocument = convertDocument(marcFrameConverter, doc, row.collection, row.created, getOaipmhSetSpecs(rows))
                        break
                }
                return [collection: row.collection, document: whelkDocument, isSuppressed: false, isDeleted: row.isDeleted, timestamp: timestamp, controlNumber: controlNumber, checksum: whelkDocument.getChecksum()]
            }
            catch (any) {
                println "ALLVARLIGT FEL! ${any.message}"
                println "Bibid: ${row.bib_id}"
                any.printStackTrace()
                return [collection: row.collection, document: null, isSuppressed: true, isDeleted: false, timestamp: timestamp, controlNumber: "0", checksum: "0"]
            }

        } else
            return [collection: row.collection, document: null, isSuppressed: true, isDeleted: row.isDeleted, timestamp: timestamp, controlNumber: controlNumber, checksum: "0"]
    }

    private static Document convertDocument(MarcFrameConverter marcFrameConverter, Map doc, String collection, Date created, List authData = null) {
        if (doc && !isSuppressed(doc)) {
            String oldStyleIdentifier = "/" + collection + "/" + getControlNumber(doc)

            def id = LegacyIntegrationTools.generateId(oldStyleIdentifier)

            Map spec = [oaipmhSetSpecs: authData]

            Map convertedData = authData == null ? marcFrameConverter.convert(doc as Map, id as String) :
                    marcFrameConverter.convert(doc as Map, id as String, spec as Map)

            Document document = new Document(convertedData)
            document.created = created
            return document
        } else {
            println "is suppresse: ${isSuppressed(doc)}"
            return null
        }
    }

    private static Map getMarcDocMap(byte[] data) {
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

    private static getAuthDocsFromRows(List<VCopyDataRow> rows) {
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
        }
        return specs
    }

    private static isSuppressed(Map doc) {
        def fields = doc.get("fields")
        for (def field : fields) {
            if (field.get("599") != null) {
                def field599 = field.get("599")
                if (field599.get("subfields") != null) {
                    def subfields = field599.get("subfields")
                    for (def subfield : subfields) {
                        if (subfield.get("a").equals("SUPPRESSRECORD"))
                            return true
                    }
                }
            }
        }
        return false
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
