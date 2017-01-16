package whelk.importer

import groovy.json.JsonBuilder
import groovyx.gpars.actor.Actors
import groovyx.gpars.actor.DefaultActor
import sun.reflect.generics.reflectiveObjects.NotImplementedException
import whelk.Document
import whelk.Whelk
import whelk.tools.PostgresLoadfileWriter

import java.sql.Timestamp

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
        return whelkSaver.importResult
    }

}

class WhelkSaver extends DefaultActor {
    ImportResult importResult
    String sourceSystem
    Whelk whelk
    int numrecs = 0

    WhelkSaver(Whelk w, String sourceSystem) {
        this.importResult = new ImportResult()
        this.whelk = w
        this.sourceSystem = sourceSystem

    }

    void setLastRecordTimeStamp(Timestamp timestamp) {
        if (timestamp > importResult.lastRecordDatestamp)
            importResult.lastRecordDatestamp = timestamp
    }

    @Override
    protected void act() {
        loop {
            react { argument ->
                try {
                    setLastRecordTimeStamp(argument.timestamp as Timestamp)
                    Document doc = argument.document
                    if (argument.isDeleted) {
                        String systemId = whelk.storage.locate(argument.id as String, false)?.id
                        if (systemId) {
                            whelk.remove(systemId, sourceSystem, null, argument.collection as String)
                            importResult.numberOfDocumentsDeleted++
                        }
                    } else if (argument.isSuppressed) {
                        importResult.numberOfDocumentsSkipped++
                    } else {
                        whelk.store(doc, sourceSystem, null, argument.collection as String, false)
                    }
                    importResult.numberOfDocuments++
                    reply true
                }
                catch (any) {
                    println any.message
                    println argument.inspect()
                    throw any
                }
            }
        }
    }
}
