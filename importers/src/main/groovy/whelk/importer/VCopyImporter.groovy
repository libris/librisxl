package whelk.importer

import groovy.json.JsonBuilder
import groovy.util.logging.Slf4j as Log
import whelk.Whelk
import whelk.actors.WhelkSaver
import whelk.PostgresLoadfileWriter
import whelk.component.PostgreSQLComponent
import whelk.converter.marc.MarcFrameConverter

/**
 * Created by Theodor on 2017-01-05.
 */
@Log
class VCopyImporter {

    Whelk whelk
    MarcFrameConverter converter


    VCopyImporter(Whelk whelk, MarcFrameConverter converter) {
        this.whelk = whelk
        this.converter = converter
    }

    ImportResult doImport(String collection, String sourceSystem, String connectionUrl, Date from) {

        def whelkSaver = new WhelkSaver(whelk, converter, sourceSystem, whelk.storage)
        whelkSaver.importResult.sourceSystem = sourceSystem
        whelkSaver.importResult.fromDate = from

        String sqlQuery = MySQLLoader.selectHarvestByMarcType[collection]
        String dateString = date.toTimestamp().toString()
        List<Object> queryParameters = [0, dateString, dateString]
        MySQLLoader.run(whelkSaver, sqlQuery, queryParameters, collection, connectionUrl)


        ImportResult result = whelkSaver.importResult
        log.debug new JsonBuilder(result).toPrettyString()
        return result
    }

    ImportResult doImport(String collection, String sourceSystem, String connectionUrl, String[] vcopyIdsToImport) {

        def whelkSaver = new WhelkSaver(whelk, converter, sourceSystem, whelk.storage )
        whelkSaver.importResult.sourceSystem = sourceSystem

        String sqlQuery = MySQLLoader.selectExampleDataByMarcType[collection].replace('?', vcopyIdsToImport.collect { it -> '?' }.join(','))

        MySQLLoader.run(whelkSaver, sqlQuery, vcopyIdsToImport.toList(), collection, connectionUrl)

        ImportResult result = whelkSaver.importResult
        log.debug new JsonBuilder(result).toPrettyString()

        return result
    }

}

