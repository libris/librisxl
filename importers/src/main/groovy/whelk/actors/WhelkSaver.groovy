package whelk.actors

import groovy.json.JsonOutput
import groovy.util.logging.Slf4j as Log
import groovyx.gpars.actor.DefaultActor
import whelk.Document
import whelk.Location
import whelk.VCopyToWhelkConverter
import whelk.Whelk
import whelk.component.PostgreSQLComponent
import whelk.filter.LinkFinder
import whelk.importer.ImportResult
import whelk.VCopyDataRow
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
    MarcFrameConvertingActor convertActor
    PostgreSQLComponent postgreSQLComponent

    WhelkSaver(Whelk w, MarcFrameConverter converter, String sourceSystem, PostgreSQLComponent postgreSQLComponent) {
        this.postgreSQLComponent = postgreSQLComponent
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

        LinkFinder lf = new LinkFinder(postgreSQLComponent)

        MarcFrameConverter marcFrameConverter = new MarcFrameConverter(lf)

            for (List<VCopyDataRow> argument : batch) {
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

                            // At this point doc.id will have been generated based on the incoming Voyager ID. This
                            // ensures that it correctly matches the ID of the stored XL post that is now to be updated
                            // _IF_ the post was first created in Voyager.
                            //
                            // If the post was however first created in XL, then the stored XL post that is now to be
                            // updated will have a timestamp-based ID, not related to the Voyager ID (which would only
                            // have been added to the post later).
                            // This means that doc.id and the XL id that we now want to update do _not_ match.
                            // Therefore we must search for existing XL posts based on the voyager-id based thing sameAs
                            // and, if any are found, replace doc.id.

                            String voyagerBasedThingSameAs = "http://libris.kb.se/resource/" + record.collection + "/" + record.controlNumber
                            String duplicateWithId = whelk.storage.getSystemIdByThingId(voyagerBasedThingSameAs)
                            if (duplicateWithId != null) {
                                log.debug("Incoming document is actually a duplicate of " + duplicateWithId + ". Replacing ID before saving.")
                                doc.setId(duplicateWithId)
                            }

                            log.trace "Storing record "
                            if (duplicateWithId == null)
                                whelk.store(doc, sourceSystem, null, record.collection as String, false, false)
                            else
                                whelk.storeAtomicUpdate(duplicateWithId, false, sourceSystem, null,
                                        record.collection, false, {
                                    Document _doc ->
                                        // Replace stored data with the incoming data.
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
