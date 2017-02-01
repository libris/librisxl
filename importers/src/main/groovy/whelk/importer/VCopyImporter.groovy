package whelk.importer

import groovy.json.JsonBuilder
import whelk.Whelk
import whelk.actors.WhelkSaver
import whelk.PostgresLoadfileWriter

/**
 * Created by Theodor on 2017-01-05.
 */
class VCopyImporter {

    static ImportResult doImport(Whelk whelk, String collection, String sourceSystem, String connectionUrl, Date from) {

        def whelkSaver = new WhelkSaver(whelk, sourceSystem)
        whelkSaver.importResult.sourceSystem = sourceSystem
        whelkSaver.importResult.fromDate = from
        whelkSaver.start()

        PostgresLoadfileWriter.import(whelkSaver, collection, connectionUrl, from)
        println new JsonBuilder(whelkSaver.importResult).toPrettyString()
        return whelkSaver.importResult
    }

    static ImportResult doImport(Whelk whelk, String collection, String sourceSystem, String connectionUrl, String[] vcopyIdsToImport) {

        def whelkSaver = new WhelkSaver(whelk, sourceSystem)
        whelkSaver.importResult.sourceSystem = sourceSystem

        whelkSaver.start()

        PostgresLoadfileWriter.import(whelkSaver, collection, connectionUrl, vcopyIdsToImport)
        whelkSaver.stop()
        return whelkSaver.importResult
    }

}

