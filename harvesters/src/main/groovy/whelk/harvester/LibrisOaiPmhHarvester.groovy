package whelk.harvester

import groovy.util.logging.Log4j2 as Log

import whelk.converter.marc.MarcFrameConverter

import whelk.*
import se.kb.libris.util.marc.*

import se.kb.libris.util.marc.io.*

@Log
class LibrisOaiPmhHarvester extends OaiPmhHarvester {

    LibrisOaiPmhHarvester(){}

    LibrisOaiPmhHarvester(Whelk w, MarcFrameConverter mfc) {
        super(w, mfc)
    }
}
