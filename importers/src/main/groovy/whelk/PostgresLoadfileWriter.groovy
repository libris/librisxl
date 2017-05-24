package whelk

import groovy.util.logging.Slf4j as Log
import whelk.actors.FileDumper
import whelk.component.PostgreSQLComponent
import whelk.importer.MySQLLoader

/**
 * Writes documents into a PostgreSQL load-file, which can be efficiently imported into lddb
 */
@Log
class PostgresLoadfileWriter {
    static void dumpToFile(String exportFileName, String collection, String connectionUrl,
                                  PostgreSQLComponent postgreSQLComponent) {
        String sqlQuery = MySQLLoader.selectByMarcType[collection]
        List<Object> queryParameters = [0]
        dump(collection, exportFileName, connectionUrl, sqlQuery, queryParameters, postgreSQLComponent)
    }

    static void dumpToFile(String exportFileName, String collection,
                           String connectionUrl, List<String> vcopyIdsToImport,
                           PostgreSQLComponent postgreSQLComponent,
                           boolean overwriteExistingFiles = true) {
        String sqlQuery = MySQLLoader.selectExampleDataByMarcType[collection]
                                     .replace('?', vcopyIdsToImport.collect { it -> '?' }.join(','))
        dump(collection, exportFileName, connectionUrl, sqlQuery,
                vcopyIdsToImport, postgreSQLComponent, overwriteExistingFiles)
    }

    private static void dump(String collection, String exportFileName, String connectionUrl, String sqlQuery,
                             List<Object> queryParameters, PostgreSQLComponent postgreSQLComponent, boolean overwriteExistingFiles = true) {

        FileDumper fileDumper = new FileDumper(exportFileName, postgreSQLComponent, overwriteExistingFiles)

        MySQLLoader.run(fileDumper, sqlQuery, queryParameters, collection, connectionUrl)

        fileDumper.close()
        println "Done."
    }

    static List<String> collectIDsFromExampleFile(String exampleDataFileName, String collection) {
        List<String> ids = []
        File exampleFile = new File(exampleDataFileName)
        final String preamble = collection+"/"
        exampleFile.eachLine { line ->
            if (line.startsWith(preamble))
            ids.add(line.substring(preamble.length(), line.indexOf('\t')))
        }
        return ids
    }
}
