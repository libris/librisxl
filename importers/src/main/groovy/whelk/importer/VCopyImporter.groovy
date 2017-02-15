package whelk.importer

import groovy.json.JsonBuilder
import groovy.util.logging.Slf4j as Log
import whelk.Whelk
import whelk.actors.WhelkSaver
import whelk.PostgresLoadfileWriter
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

        def whelkSaver = new WhelkSaver(whelk, converter, sourceSystem)
        whelkSaver.importResult.sourceSystem = sourceSystem
        whelkSaver.importResult.fromDate = from
        whelkSaver.start()

        PostgresLoadfileWriter.import(whelkSaver, collection, connectionUrl, from)
        ImportResult result = whelkSaver.importResult
        log.debug new JsonBuilder(result).toPrettyString()
        whelkSaver.stop()
        return result
    }

    ImportResult doImport(String collection, String sourceSystem, String connectionUrl, String[] vcopyIdsToImport) {

        def whelkSaver = new WhelkSaver(whelk, converter, sourceSystem)
        whelkSaver.importResult.sourceSystem = sourceSystem

        whelkSaver.start()

        PostgresLoadfileWriter.import(whelkSaver, collection, connectionUrl, vcopyIdsToImport)
        ImportResult result = whelkSaver.importResult
        log.debug new JsonBuilder(result).toPrettyString()
        whelkSaver.stop()
        return result
    }

}

