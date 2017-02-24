package whelk.actors

import groovy.json.JsonOutput
import groovy.util.logging.Slf4j as Log
import groovyx.gpars.actor.DefaultActor
import whelk.Document
import whelk.Location
import whelk.VCopyToWhelkConverter
import whelk.Whelk
import whelk.importer.ImportResult
import whelk.VCopyDataRow
import whelk.PostgresLoadfileWriter
import whelk.converter.marc.MarcFrameConverter
import whelk.importer.MySQLLoader

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
    MarcFrameConvertingActor convertActor

    WhelkSaver(Whelk w, MarcFrameConverter converter, String sourceSystem) {
        exceptionsThrown = 0
        this.importResult = new ImportResult()
        this.whelk = w
        this.sourceSystem = sourceSystem
        convertActor = new MarcFrameConvertingActor(converter)
        convertActor.start()
    }

    void setLastRecordTimeStamp(Timestamp timestamp) {
        if (timestamp > importResult.lastRecordDatestamp)
            importResult.lastRecordDatestamp = timestamp
    }

    void handle(List<List<VCopyDataRow>> batch) {

        MarcFrameConverter marcFrameConverter = new MarcFrameConverter()

        try {
            for (List<VCopyDataRow> argument : batch) {
                log.trace "Got message (${argument.size()} rows). Reacting."
                Map record = VCopyToWhelkConverter.convert(argument, marcFrameConverter)
                if (record && !record.isSuppressed) {
                    log.trace "record is not suppressed. Record: ${record.inspect()}"
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
                    } else if (record.isSuppressed) {
                        log.trace "Record is suppressed"
                        importResult.numberOfDocumentsSkipped++
                    } else {
                        log.trace "Storing record "
                        whelk.store(doc, sourceSystem, null, record.collection as String, false)
                    }
                    importResult.numberOfDocuments++

                }
            }
        }
        catch (any) {
            exceptionsThrown++
            log.error "Error saving to Whelk", any
            println any.message
        }
    }
}
