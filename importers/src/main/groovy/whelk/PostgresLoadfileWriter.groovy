package whelk

import groovy.sql.Sql
import groovy.util.logging.Slf4j as Log
import se.kb.libris.util.marc.MarcRecord
import se.kb.libris.util.marc.io.Iso2709Deserializer
import whelk.actors.FileDumper
import whelk.component.PostgreSQLComponent
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

    public static void dumpToFile(String exportFileName, String collection, String connectionUrl,
                                  PostgreSQLComponent postgreSQLComponent) {
        String sqlQuery = MySQLLoader.selectByMarcType[collection]
        List<Object> queryParameters = [0]
        dump(collection, exportFileName, connectionUrl, sqlQuery, queryParameters, postgreSQLComponent)
    }

    private static void dump(String collection, String exportFileName, String connectionUrl, String sqlQuery,
                             List<Object> queryParameters, PostgreSQLComponent postgreSQLComponent) {

        FileDumper fileDumper = new FileDumper(exportFileName, postgreSQLComponent)

        MySQLLoader.run(fileDumper, sqlQuery, queryParameters, collection, connectionUrl)

        fileDumper.close()
        println "Done."
    }

}





