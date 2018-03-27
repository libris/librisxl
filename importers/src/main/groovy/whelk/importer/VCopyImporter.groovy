package whelk.importer

import groovy.json.JsonBuilder
import groovy.util.logging.Log4j2 as Log
import whelk.Whelk
import whelk.actors.WhelkSaver

@Log
class VCopyImporter {

    Whelk whelk


    VCopyImporter(Whelk whelk) {
        this.whelk = whelk
    }

    ImportResult doImport(String collection, String sourceSystem, String connectionUrl, Date from) {

        def whelkSaver = new WhelkSaver(whelk, sourceSystem)
        whelkSaver.importResult.sourceSystem = sourceSystem
        whelkSaver.importResult.fromDate = from

        String sqlQuery = MySQLLoader.selectHarvestByMarcType[collection]
        String dateString = from.toTimestamp().toString()
        List<Object> queryParameters = [0, dateString, dateString]
        MySQLLoader.run(whelkSaver, sqlQuery, queryParameters, collection, connectionUrl)

        ImportResult result = whelkSaver.importResult
        log.debug new JsonBuilder(result).toPrettyString()
        return result
    }

    ImportResult doImport(String collection, String sourceSystem, String connectionUrl, String[] vcopyIdsToImport) {

        def whelkSaver = new WhelkSaver(whelk, sourceSystem)
        whelkSaver.importResult.sourceSystem = sourceSystem

        String sqlQuery = MySQLLoader.selectExampleDataByMarcType[collection].replace('?', vcopyIdsToImport.collect { it -> '?' }.join(','))

        MySQLLoader.run(whelkSaver, sqlQuery, vcopyIdsToImport.toList(), collection, connectionUrl)

        ImportResult result = whelkSaver.importResult
        log.debug new JsonBuilder(result).toPrettyString()

        return result
    }

}

