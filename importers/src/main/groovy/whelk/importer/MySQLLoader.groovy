package whelk.importer

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j as Log

import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.ResultSet

import java.text.Normalizer

import se.kb.libris.util.marc.MarcRecord
import se.kb.libris.util.marc.io.Iso2709Deserializer
import whelk.converter.MarcJSONConverter

@Log
@CompileStatic
class MySQLLoader {

    static final String JDBC_DRIVER = "com.mysql.jdbc.Driver"
    final String collection
    final String connectionUrl

    static Map<String, String> selectByMarcType = [

        auth: """
            SELECT auth_id, data, create_date FROM auth_record WHERE auth_id > ? AND deleted = 0 ORDER BY auth_id
            """,

        bib: """
            SELECT bib.bib_id, bib.data, bib.create_date, auth.auth_id FROM bib_record bib
            LEFT JOIN auth_bib auth ON bib.bib_id = auth.bib_id
            WHERE bib.bib_id > ? AND bib.deleted = 0 ORDER BY bib.bib_id
            """,

        hold: """
            SELECT mfhd_id, data, bib_id, shortname, create_date FROM mfhd_record
            WHERE mfhd_id > ? AND deleted = 0 ORDER BY mfhd_id
            """
    ]

    MySQLLoader(String connectionUrl, String collection) {
        Class.forName(JDBC_DRIVER)
        this.connectionUrl = connectionUrl
        this.collection = collection
    }

    void run(LoadHandler handler) {
        Connection connection = DriverManager.getConnection(connectionUrl)
        connection.setAutoCommit(false)

        PreparedStatement statement = connection.prepareStatement(
                selectByMarcType[collection],
                java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY)
        statement.setFetchSize(Integer.MIN_VALUE)
        statement.setInt(1, 0) // start from id 0

        ResultSet resultSet = statement.executeQuery()

        try {
            while (resultSet.next()) {
                processNext(resultSet, handler)
            }
        }
        finally {
            resultSet.close()
            statement.close()
            connection.close()
        }
    }

    void processNext(ResultSet resultSet, LoadHandler handler) {
        int currentRecordId = -1
        Map doc = null
        List specs = null
        int recordId = resultSet.getInt(1)
        MarcRecord record = Iso2709Deserializer.deserialize(
                normalizeString(
                    new String(resultSet.getBytes("data"), "UTF-8")).getBytes("UTF-8"))
        if (record) {
            doc = MarcJSONConverter.toJSONMap(record)
            if (!recordId.equals(currentRecordId)) {
                specs = getOaipmhSetSpecs(resultSet)
                if (doc) {
                    handler.handle(doc, specs, resultSet.getTimestamp("create_date"))
                }
                currentRecordId = recordId
                doc = [:]
                specs = []
            }
        }
        specs = getOaipmhSetSpecs(resultSet)
        if (doc) {
            handler.handle(doc, specs, resultSet.getTimestamp("create_date"))
        }
    }

    List getOaipmhSetSpecs(ResultSet resultSet) {
        List specs = []
        if (collection == "bib") {
            int authId = resultSet.getInt("auth_id")
            if (authId > 0)
                specs.add("authority:" + authId)
        } else if (collection == "hold") {
            int bibId = resultSet.getInt("bib_id")
            String sigel = resultSet.getString("shortname")
            if (bibId > 0)
                specs.add("bibid:" + bibId)
            if (sigel)
                specs.add("location:" + sigel)
        }
        return specs
    }

    static String normalizeString(String inString) {
        if (!Normalizer.isNormalized(inString, Normalizer.Form.NFC)) {
            log.trace("Normalizing ...")
            return Normalizer.normalize(inString, Normalizer.Form.NFC)
        }
        return inString
    }

    static interface LoadHandler {
        void handle(Map doc, List specs, Date createDate)
    }

}
