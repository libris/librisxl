package whelk.importer

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j as Log
import se.kb.libris.util.marc.MarcRecord
import se.kb.libris.util.marc.io.Iso2709Deserializer
import whelk.converter.MarcJSONConverter

import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.text.Normalizer

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

            bib : """
            SELECT bib.bib_id, bib.data, bib.create_date, auth.auth_id, auth_data.data as auth_data FROM bib_record bib
            LEFT JOIN auth_bib auth ON bib.bib_id = auth.bib_id
            LEFT JOIN auth_record auth_data on auth.auth_id = auth_data.auth_id
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
        catch(any){
            log.error("",any)
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
        int recordId = resultSet.getInt(1)
        MarcRecord record = Iso2709Deserializer.deserialize(
                normalizeString(
                        new String(resultSet.getBytes("data"), "UTF-8")).getBytes("UTF-8"))
        if (record) {

            doc = MarcJSONConverter.toJSONMap(record)
            if (!recordId.equals(currentRecordId)) { //TODO: What is this construct for?
                if (doc) {
                    handler.handle(doc, resultSet.getTimestamp("create_date"))
                }
                currentRecordId = recordId
                doc = [:]

            }
        }

        if (doc) {
            handler.handle(doc, resultSet.getTimestamp("create_date"))
        }
    }

    static String normalizeString(String inString) {
        if (!Normalizer.isNormalized(inString, Normalizer.Form.NFC)) {
            log.trace("Normalizing ...")
            return Normalizer.normalize(inString, Normalizer.Form.NFC)
        }
        return inString
    }

    static interface LoadHandler {
        void handle(Map doc, Date createDate)
    }

}
