package whelk.harvester

import groovy.util.logging.Slf4j as Log

import whelk.converter.marc.MarcFrameConverter

import whelk.*
import se.kb.libris.util.marc.*

import se.kb.libris.util.marc.io.*

@Log
class LibrisOaiPmhHarvester extends OaiPmhHarvester {

    static String SUPPRESSED_RECORD = "SUPPRESSRECORD"

    LibrisOaiPmhHarvester(){}

    LibrisOaiPmhHarvester(Whelk w, MarcFrameConverter mfc) {
        super(w, mfc)
    }

    @Override
    boolean okToSave(OaiPmhRecord oaiPmhRecord) {
        MarcRecord marcRecord = MarcXmlRecordReader.fromXml(oaiPmhRecord.record)
        def aList = marcRecord.getDatafields("599").collect { it.getSubfields("a").data }.flatten()
        if (aList.contains(SUPPRESSED_RECORD)) {
            log.debug("Record ${oaiPmhRecord.identifier} is suppressed.")
            return false
        }
        return true
    }
}
