package whelk.actors

import groovy.json.JsonOutput
import groovy.util.logging.Log4j2 as Log
import groovyx.gpars.actor.DefaultActor
import whelk.Document
import whelk.Location
import whelk.util.VCopyToWhelkConverter
import whelk.Whelk
import whelk.component.PostgreSQLComponent
import whelk.filter.LinkFinder
import whelk.importer.ImportResult
import whelk.PostgresLoadfileWriter
import whelk.converter.marc.MarcFrameConverter
import whelk.importer.MySQLLoader
import whelk.util.LegacyIntegrationTools

import java.sql.Timestamp

/**
 * Created by theodortolstoy on 2017-01-27.
 */

@Log
class WhelkSaver implements MySQLLoader.LoadHandler {
    ImportResult importResult
    int exceptionsThrown
    String sourceSystem
    Whelk whelk
    PostgreSQLComponent postgreSQLComponent

    WhelkSaver(Whelk w, MarcFrameConverter converter, String sourceSystem, PostgreSQLComponent postgreSQLComponent) {
        this.postgreSQLComponent = postgreSQLComponent
        exceptionsThrown = 0
        this.importResult = new ImportResult()
        this.whelk = w
        this.sourceSystem = sourceSystem
    }

    void setLastRecordTimeStamp(Timestamp timestamp) {
        if (timestamp > importResult.lastRecordDatestamp)
            importResult.lastRecordDatestamp = timestamp
    }

    void handle(List<List<VCopyToWhelkConverter.VCopyDataRow>> batch) {

        LinkFinder lf = new LinkFinder(postgreSQLComponent)

        MarcFrameConverter marcFrameConverter = new MarcFrameConverter(lf)

            for (List<VCopyToWhelkConverter.VCopyDataRow> argument : batch) {
                try {
                    log.trace "Got message (${argument.size()} rows). Reacting."
                    Map record = VCopyToWhelkConverter.convert(argument, marcFrameConverter)
                    if (record) {
                        setLastRecordTimeStamp(record.timestamp as Timestamp)
                        Document doc = record.document
                        if (record.isDeleted) {
                            log.trace "Record is deleted"
                            Location l = whelk.storage.locate(record.controlNumber as String, false)
                            String systemId = l?.id
                            if (systemId) {
                                log.trace "Removing record with systemID: ${systemId}"
                                whelk.remove(systemId, sourceSystem, null, record.collection as String)
                                importResult.numberOfDocumentsDeleted++
                            }
                        } else {
                            Document conflictingDocument = whelk.storage.load(doc.getShortId())
                            if (conflictingDocument == null)
                                whelk.store(doc, sourceSystem, null, record.collection as String, false)
                            else
                                whelk.storeAtomicUpdate(doc.getShortId(), false, sourceSystem, null, record.collection as String, false, {
                                    Document _doc ->
                                        _doc.data = doc.data
                                })
                        }
                        importResult.numberOfDocuments++
                    }
                }
                catch (any) {
                    exceptionsThrown++
                    log.error "Error saving to Whelk", any
                }
            }
    }
}
