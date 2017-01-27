package whelk.actors


import groovyx.gpars.actor.DefaultActor
import whelk.Document
import whelk.Whelk
import whelk.importer.ImportResult
import whelk.VCopyDataRow

import java.sql.Timestamp

/**
 * Created by theodortolstoy on 2017-01-27.
 */
class WhelkSaver extends DefaultActor {
    ImportResult importResult
    String sourceSystem
    Whelk whelk
    MarcFrameConvertingActor converter

    WhelkSaver(Whelk w, String sourceSystem) {
        this.importResult = new ImportResult()
        this.whelk = w
        this.sourceSystem = sourceSystem
        converter = new MarcFrameConvertingActor()
        converter.start()
    }

    void setLastRecordTimeStamp(Timestamp timestamp) {
        if (timestamp > importResult.lastRecordDatestamp)
            importResult.lastRecordDatestamp = timestamp
    }

    @Override
    protected void act() {
        loop {
            react { List<VCopyDataRow> argument ->
                try {
                    Map record =whelk.PostgresLoadfileWriter.handleRowGroup(argument, converter)
                    if (record && !record.isSuppressed) {
                        setLastRecordTimeStamp(record.timestamp as Timestamp)
                        Document doc = record.document
                        if (argument.isDeleted) {
                            String systemId = whelk.storage.locate(record.controlNumber as String, false)?.id
                            if (systemId) {
                                whelk.remove(systemId, sourceSystem, null, argument.collection as String)
                                importResult.numberOfDocumentsDeleted++
                            }
                        } else if (record.isSuppressed) {
                            importResult.numberOfDocumentsSkipped++
                        } else {
                            whelk.store(doc, sourceSystem, null, argument.collection as String, false)
                        }
                        importResult.numberOfDocuments++

                    }
                }
                catch (any) {
                    println any.message
                    println argument.inspect()

                }
                reply true
            }
        }
    }
}
