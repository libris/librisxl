package whelk.importer

import groovy.json.JsonBuilder
import groovy.util.logging.Slf4j as Log
import whelk.Whelk
import whelk.actors.WhelkSaver
import whelk.PostgresLoadfileWriter

/**
 * Created by Theodor on 2017-01-05.
 */
@Log
class VCopyImporter {

    static ImportResult doImport(Whelk whelk, String collection, String sourceSystem, String connectionUrl, Date from) {

        def whelkSaver = new WhelkSaver(whelk, sourceSystem)
        whelkSaver.importResult.sourceSystem = sourceSystem
        whelkSaver.importResult.fromDate = from
        whelkSaver.start()

        PostgresLoadfileWriter.import(whelkSaver, collection, connectionUrl, from)
        ImportResult result = whelkSaver.importResult
        log.debug new JsonBuilder(result).toPrettyString()
        whelkSaver.stop()
        return result
    }

    static ImportResult doImport(Whelk whelk, String collection, String sourceSystem, String connectionUrl, String[] vcopyIdsToImport) {

        def whelkSaver = new WhelkSaver(whelk, sourceSystem)
        whelkSaver.importResult.sourceSystem = sourceSystem

        whelkSaver.start()

        PostgresLoadfileWriter.import(whelkSaver, collection, connectionUrl, vcopyIdsToImport)
        ImportResult result = whelkSaver.importResult
        log.debug new JsonBuilder(result).toPrettyString()
        whelkSaver.stop()
        return result
    }

}

