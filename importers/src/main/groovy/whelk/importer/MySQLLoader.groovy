package whelk.importer

import groovy.sql.Sql
import groovy.util.logging.Log4j2 as Log
import whelk.util.VCopyToWhelkConverter
import java.sql.ResultSet
import java.sql.Statement

@Log
class MySQLLoader {

    static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver"
    static private final int BATCH_SIZE = 200

    static  Map<String,String> selectExampleDataByMarcType = [
            auth: """
            SELECT auth_id, data, create_date, deleted, update_date FROM auth_record WHERE auth_id IN (?) ORDER BY auth_id
            """,
            bib : """
            SELECT bib.bib_id, bib.data, bib.create_date, auth.auth_id, auth_data.data as auth_data, bib.deleted as deleted, bib.update_date as update_date FROM bib_record bib
            LEFT JOIN auth_bib auth ON bib.bib_id = auth.bib_id
            LEFT JOIN auth_record auth_data on auth.auth_id = auth_data.auth_id
            WHERE bib.bib_id IN (?) ORDER BY bib.bib_id
             """,
            hold: """
            SELECT mfhd_id, data, bib_id, shortname, create_date, deleted, update_date FROM mfhd_record
            WHERE mfhd_id IN (?) ORDER BY mfhd_id
            """]

    static Map<String, String> selectByMarcType = [

            auth: """
            SELECT auth_id, data, create_date, deleted, update_date FROM auth_record WHERE auth_id > ? AND deleted = 0 ORDER BY auth_id
            """,

            bib : """
            SELECT bib.bib_id, bib.data, bib.create_date, auth.auth_id, auth_data.data as auth_data, bib.deleted as deleted, bib.update_date as update_date FROM bib_record bib
            LEFT JOIN auth_bib auth ON bib.bib_id = auth.bib_id
            LEFT JOIN auth_record auth_data on auth.auth_id = auth_data.auth_id
            WHERE bib.bib_id > ? AND bib.deleted = 0 ORDER BY bib.bib_id
             """,

            hold: """
            SELECT mfhd_id, data, bib_id, shortname, create_date, deleted, update_date FROM mfhd_record
            WHERE mfhd_id > ? AND deleted = 0 ORDER BY mfhd_id
            """

    ]

    static Map<String, String> selectHarvestByMarcType = [

            auth: """
            SELECT auth_id, data, create_date, deleted, update_date FROM auth_record WHERE auth_id > ? AND (update_date > ? OR create_date > ?) ORDER BY update_date,create_date
            """,

            bib : """
            SELECT bib.bib_id, bib.data, bib.create_date, auth.auth_id, auth_data.data AS auth_data, bib.deleted AS deleted, bib.update_date AS update_date FROM bib_record bib
            LEFT JOIN auth_bib auth ON bib.bib_id = auth.bib_id
            LEFT JOIN auth_record auth_data ON auth.auth_id = auth_data.auth_id
            WHERE bib.bib_id > ? AND (bib.update_date > ? OR bib.create_date > ? ) ORDER BY bib.update_date,bib.create_date
             """,

            hold: """
            SELECT mfhd_id, data, bib_id, shortname, create_date, deleted, update_date FROM mfhd_record
            WHERE mfhd_id > ? AND (update_date > ? OR create_date > ?)  ORDER BY update_date,create_date
            """

    ]

    static{
        Class.forName(JDBC_DRIVER)
    }

    static void run(LoadHandler handler, String sqlQuery, List<Object> queryParameters, String collection, String connectionUrl) {

        final Sql sql = prepareSql(connectionUrl)

        int rowCount = 0
        int recordCount = 0
        long startTime = System.currentTimeMillis()

        List<VCopyToWhelkConverter.VCopyDataRow> previousRowsInGroup = []
        VCopyToWhelkConverter.VCopyDataRow previousRow = null
        VCopyToWhelkConverter.VCopyDataRow currentRow = null

        List<List<VCopyToWhelkConverter.VCopyDataRow>> currentBatch = []

        sql.eachRow(sqlQuery, queryParameters) { ResultSet resultSet ->
            try {
                ++rowCount
                currentRow = new VCopyToWhelkConverter.VCopyDataRow(resultSet, collection)
                switch (previousRow) {
                    case null:                              //first run
                        previousRow = currentRow
                        previousRowsInGroup.add(currentRow)
                        break
                    case { collection == 'bib' && it.bib_id == currentRow.bib_id }://Same bib record
                        previousRowsInGroup.add(currentRow)
                        break
                    default:                                //new record
                        currentBatch = batchForConversion(previousRowsInGroup, currentBatch, handler)
                        ++recordCount
                        previousRow = currentRow
                        previousRowsInGroup = []
                        previousRowsInGroup.add(currentRow)
                        break
                }
            }
            catch (any) {
                log.error("Batch failed" , any)
                // we might want to throw the exception 
                // here eventually in order to not hide failed records
            }

            if (recordCount % 1000 == 1) {
                def elapsedSecs = (System.currentTimeMillis() - startTime) / 1000
                if (elapsedSecs > 0) {
                    Double docsPerSec = recordCount / elapsedSecs
                    def message = "Working. Currently ${rowCount} rows recieved and ${recordCount} records sent. Crunching ${docsPerSec.round(1)} records / s."
                    log.info message
                }
            }
        }

        if (!previousRowsInGroup.isEmpty()) {
            currentBatch = batchForConversion(previousRowsInGroup, currentBatch, handler)
        }
        currentBatch = flushConversionBatch(currentBatch, handler)
        log.info "Done reading from DB."

    }

    static interface LoadHandler {
        void handle(List<List<VCopyToWhelkConverter.VCopyDataRow>> currentBatch)
    }

    /**
     * Send the row list to be dispatched for conversion. Essentially this means, add it to the global 'currentBatch',
     * and if the batch is large enough flush it to the conversion process and return a new empty batch instead.
     */
    private static List<List<VCopyToWhelkConverter.VCopyDataRow>> batchForConversion(List<VCopyToWhelkConverter.VCopyDataRow> rowList,
                                                               List<List<VCopyToWhelkConverter.VCopyDataRow>> currentBatch,
                                                               LoadHandler handler) {
        currentBatch.add(rowList)

        if (currentBatch.size() > BATCH_SIZE) {
            return flushConversionBatch(currentBatch, handler)
        }
        return currentBatch
    }

    private static List<List<VCopyToWhelkConverter.VCopyDataRow>> flushConversionBatch(List<List<VCopyToWhelkConverter.VCopyDataRow>> currentBatch,
                                                                 LoadHandler handler) {
        handler.handle(currentBatch)
        return []
    }

    private static Sql prepareSql(String connectionUrl) {
        def sql = Sql.newInstance(connectionUrl, JDBC_DRIVER)
        sql.withStatement { Statement stmt -> stmt.fetchSize = Integer.MIN_VALUE }
        sql.connection.autoCommit = false
        sql.resultSetType = ResultSet.TYPE_FORWARD_ONLY
        sql.resultSetConcurrency = ResultSet.CONCUR_READ_ONLY
        sql
    }
}
