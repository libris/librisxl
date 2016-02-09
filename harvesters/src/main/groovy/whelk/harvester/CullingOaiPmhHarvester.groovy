package whelk.harvester

import se.kb.libris.util.marc.MarcRecord
import se.kb.libris.util.marc.io.MarcXmlRecordReader
import whelk.Document
import whelk.Whelk
import whelk.converter.marc.MarcFrameConverter

/**
 * Created by markus on 2016-02-09.
 */
class CullingOaiPmhHarvester extends OaiPmhHarvester {


    CullingOaiPmhHarvester(Whelk w, MarcFrameConverter mfc) {
        super(w, mfc)
    }

    @Override
    boolean okToSave(OaiPmhRecord oaiPmhRecord) {
        if (oaiPmhRecord.deleted) {
            // Only accept delete records
            return true
        }
        return false
    }
}
