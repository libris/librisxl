package whelk

import groovy.sql.Sql
import groovy.util.logging.Slf4j as Log
import se.kb.libris.util.marc.MarcRecord
import se.kb.libris.util.marc.io.Iso2709Deserializer
import whelk.actors.FileDumper
import whelk.converter.MarcJSONConverter
import whelk.importer.MySQLLoader
import whelk.util.LegacyIntegrationTools

import java.sql.ResultSet
import java.sql.Statement
import java.sql.Timestamp


/**
 * Writes documents into a PostgreSQL load-file, which can be efficiently imported into lddb
 */
@Log
class PostgresLoadfileWriter {

    /*
    static void dumpAuthStats(String folderName, String connectionUrl) {
        def collection = 'bib'
        StatsMaker statsMaker = new StatsMaker()
        statsMaker.start()
        String sqlQuery = MySQLLoader.selectByMarcType[collection]
        List<Object> queryParameters = [0]
        dump(statsMaker, collection, connectionUrl, sqlQuery, queryParameters)
        statsMaker.stop()
    }

    static void "import"(DefaultActor actor, String collection, String connectionUrl, Date date) {
        String sqlQuery = MySQLLoader.selectHarvestByMarcType[collection]
        String dateString = date.toTimestamp().toString()
        List<Object> queryParameters = [0, dateString, dateString]
        dump(actor, collection, connectionUrl, sqlQuery, queryParameters)
    }


    static void "import"(DefaultActor actor, String collection, String connectionUrl, String[] vcopyIdsToImport) {
        String sqlQuery = MySQLLoader.selectExampleDataByMarcType[collection].replace('?', vcopyIdsToImport.collect { it -> '?' }.join(','))
        dump(actor, collection, connectionUrl, sqlQuery, vcopyIdsToImport.toList())
    }

    static void dumpGpars(String exportFileName, String collection, String connectionUrl) {
        String sqlQuery = MySQLLoader.selectByMarcType[collection]
        List<Object> queryParameters = [0]
        def fileDumper = new FileDumper(exportFileName)
        fileDumper.start()
        dump(fileDumper, collection, connectionUrl, sqlQuery, queryParameters)
        fileDumper.stop()
    }
*/

    private final static int BATCH_SIZE = 200

    public static void dumpToFile(String exportFileName, String collection, String connectionUrl) {
        String sqlQuery = MySQLLoader.selectByMarcType[collection]
        List<Object> queryParameters = [0]
        dump(collection, exportFileName, connectionUrl, sqlQuery, queryParameters)
    }

    private static void dump(String collection, String exportFileName, String connectionUrl, String sqlQuery, List<Object> queryParameters) {

        final Sql sql = prepareSql(connectionUrl)
        FileDumper fileDumper = new FileDumper(exportFileName)

        int rowCount = 0
        int recordCount = 0
        long startTime = System.currentTimeMillis()

        List<VCopyDataRow> previousRowsInGroup = []
        VCopyDataRow previousRow = null
        VCopyDataRow currentRow = null

        List<List<VCopyDataRow>> currentBatch = []

        sql.eachRow(sqlQuery, queryParameters) { ResultSet resultSet ->
            try {
                ++rowCount
                currentRow = new VCopyDataRow(resultSet, collection)
                switch (previousRow) {
                    case null:                              //first run
                        previousRow = currentRow
                        previousRowsInGroup.add(currentRow)
                        break
                    case { collection == 'bib' && it.bib_id == currentRow.bib_id }://Same bib record
                        previousRowsInGroup.add(currentRow)
                        break
                    default:                                //new record
                        currentBatch = batchForConversion(previousRowsInGroup, currentBatch, fileDumper)
                        ++recordCount
                        previousRow = currentRow
                        previousRowsInGroup = []
                        previousRowsInGroup.add(currentRow)
                        break
                }
            }
            catch (any) {
                println any.message
                println any.stackTrace
                //throw any // dont want to miss any records
            }

            if (recordCount % 1000 == 0) {
                def elapsedSecs = (System.currentTimeMillis() - startTime) / 1000
                if (elapsedSecs > 0) {
                    def docsPerSec = recordCount / elapsedSecs
                    def message = "Working. Currently ${rowCount} rows recieved and ${recordCount} records sent. Crunching ${docsPerSec} records / s."
                    println message
                    log.info message
                }
            }
        }

        if (!previousRowsInGroup.isEmpty()) {
            currentBatch = batchForConversion(previousRowsInGroup, currentBatch, fileDumper)
            ++recordCount
        }
        currentBatch = flushConversionBatch(currentBatch, fileDumper)

        println "Done reading from DB."

        fileDumper.close()
        println "Done."
    }

    /**
     * Send the row list to be dispatched for conversion. Essentially this means, add it to the global 'currentBatch',
     * and if the batch is large enough flush it to the conversion process and return a new empty batch instead.
     */
    private static List<List<VCopyDataRow>> batchForConversion(List<VCopyDataRow> rowList,
                                                               List<List<VCopyDataRow>> currentBatch,
                                                               FileDumper fileDumper) {
        currentBatch.add(rowList)

        if (currentBatch.size() > BATCH_SIZE) {
            return flushConversionBatch(currentBatch, fileDumper)
        }
        return currentBatch
    }

    private static List<List<VCopyDataRow>> flushConversionBatch(List<List<VCopyDataRow>> currentBatch,
                                                                 FileDumper fileDumper) {
        fileDumper.convertAndWrite(currentBatch)
        return []
    }

    private static Sql prepareSql(String connectionUrl) {
        def sql = Sql.newInstance(connectionUrl, "com.mysql.jdbc.Driver")
        sql.withStatement { Statement stmt -> stmt.fetchSize = Integer.MIN_VALUE }
        sql.connection.autoCommit = false
        sql.resultSetType = ResultSet.TYPE_FORWARD_ONLY
        sql.resultSetConcurrency = ResultSet.CONCUR_READ_ONLY
        sql
    }

}





